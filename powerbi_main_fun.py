"""

Copyright 2018-2020 VMware, Inc.
SPDX-License-Identifier: BSD-2-Clause

"""

import calendar
import csv
import json
import logging
import random
import re
import sys
import time
import urllib
from datetime import datetime, timedelta
from time import sleep
from typing import List, Optional

import certifi
import geoip2.webservice
import mysql.connector
import requests
from geopy.geocoders import Nominatim
from mysql.connector import cursor, MySQLConnection
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from slack_webhook import Slack

import Functions.data_sanitization as data_sanitization
import Functions.sql_upserts as sql_upserts
import Functions.vco_calls as vco_calls
import fun_mysql_inserts as sql_inserts
import fun_mysql_query as sql_queries
from Objects.Config import Config
from VCOClient import VcoRequestManager, ApiException
from Functions.helpers import log_critical_error

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


def determine_if_any_edge_in_customer_needs_update(mycursor, cnx, customer, client, VCO_CUSTOMER_EDGE):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.INFO)
    params = {"enterpriseId": customer["id"], "with": []}
    kwargs = {"timeout": 10}
    get_edges = client.call_api('/enterprise/getEnterpriseEdges', params, **kwargs)
    sleep(0.1)
    for edge in get_edges:
        if edge["logicalId"]:
            if sql_queries.determine_if_edge_needs_update(mycursor, cnx, edge["logicalId"], VCO_CUSTOMER_EDGE):
                return True
    return False


def determine_full_permissions_to_this_customer(client, customer, VCO_CUSTOMER_EDGE):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.INFO)
    params = {"enterpriseId": customer["id"]}
    kwargs = {"timeout": 10}
    privileges = client.call_api('/role/getEnterpriseDelegatedPrivileges', params, **kwargs)
    sleep(0.1)

    if len(privileges) == 0:
        logger.info("no privileges")
        return False

    for privilege in privileges:
        if privilege['isDeny'] == 1:
            logger.info("We are missing permissions on this customer")
            logger.info(privilege)
            return False

    logger.info("We have full permissions on this customer")
    return True


def process_vco(vco: str, cfg: Config, vco_list, slack_notifications: bool = False, arg_customer: Optional[int] = None,
                debug: bool = False, ):
    slack_client = Slack(url=cfg.slack.url)
    vco_info = vco_list.get(vco)
    print(vco_info)
    vco_info['name'] = vco

    VCO_CUSTOMER_EDGE = vco_info.get('link')
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.INFO)
    logger.info('PROCESSING')

    vco_client, conn_err_msg = vco_calls.connect_to_vco(vco=vco_info)
    if vco_client:
        logger.info('Connected')
    else:
        logger.critical(f'Not Connected - {conn_err_msg}')
        if slack_notifications:
            slack_client.post(text=f'VCO: {vco_info.get("name")} - Unable to connect {conn_err_msg}')
        return False

    mysql_handle = mysql.connector.connect(host=cfg.mysql_prod.host, database=cfg.mysql_prod.db,
                                           user=cfg.mysql_prod.user, password=cfg.mysql_prod.password)

    mysql_cursor = mysql_handle.cursor()

    logger.info('Getting version and upserting VCO')

    vco_version, vers_err_msg = vco_calls.get_vco_version(vco_client=vco_client)
    if vers_err_msg:
        logger.error(vers_err_msg)
    vco_info['version'] = vco_version
    sql_upserts.upsert_vco(curs=mysql_cursor, vco_info=vco_info, sql_cnx=mysql_handle)
    sql_upserts.upsert_vco_attribute(curs=mysql_cursor, sql_cnx=mysql_handle, vco_link=vco_info.get('link'),
                                     name='software_version', text=vco_version, log_name=VCO_CUSTOMER_EDGE)
    logger.info('done upserting vco')

    # Get Customer List
    # Name and partner get sanitized in this function
    raw_customer_list, cust_err_msg = vco_calls.get_vco_customers(vco_client=vco_client)

    # Catch errors from customer call or an empty customer list

    if raw_customer_list is None:
        logger.critical(f'Unable to get customers for this VCO - {cust_err_msg}')
        if slack_notifications:
            slack_client.post(text=f'VCO: {vco_info.get("name")} - Unable to get customers from VCO - {cust_err_msg}')
        return False
    elif len(raw_customer_list) == 0:
        logger.error(f'No customers received for this VCO')
        return False

    # Clean the customer list
    # if arg_customer exists it will only return arg_customer
    customer_list = data_sanitization.clean_customers(customer_list=raw_customer_list, vco_name=vco_info.get('name'),
                                                      arg_customer=arg_customer)

    # Process each customer
    for customer in customer_list:
        try:
            logger.info('Processing customer')
            process_customer(mysql_cursor, mysql_handle, customer, vco_list, vco, vco_client, cfg=cfg)
        except Exception as e:
            logger.critical(f'Unable to process customer - Name: {customer.get("name")} - ID: {customer.get("id")} - '
                            f'UUID: {customer.get("logicalId")}')
            log_critical_error(ex=e, log_name=VCO_CUSTOMER_EDGE)
            if debug:
                raise e.with_traceback(sys.exc_info()[2])

    return True


def process_customer(mysql_cursor, mysql_handle, customer, vco_list, vco, client, cfg: Config, force_run=True):
    vco_info = vco_list.get(vco, {})
    customer_name = customer.get('name')
    customer_uuid = customer.get('logicalId')
    vco_link = vco_info.get('link')

    VCO_CUSTOMER_EDGE = f'{vco_link}:{customer_name}'
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.DEBUG)

    logger.info("STARTED")

    # Determine if we should bypass customer
    # STEP1: Determine if Customer exist in Database create if it doesn't
    sql_queries.determine_if_customer_exists_in_mysql_creates_if_not(mysql_cursor=mysql_cursor,
                                                                     mysql_handle=mysql_handle,
                                                                     customerid=customer_uuid,
                                                                     Customer_Name=customer_name, VCO=vco_link,
                                                                     VCO_CUSTOMER_EDGE=VCO_CUSTOMER_EDGE)
    # STEP2: Update Customer with information we have
    sql_inserts.update_customer_with_vco_name_partner(mysql_cursor=mysql_cursor, mysql_handle=mysql_handle,
                                                      customer=customer, vco_link=vco_link,
                                                      vco_partner=vco_info.get('partner'), log_name=VCO_CUSTOMER_EDGE)
    process_marketing_name(mysql_cursor, mysql_handle, customer, VCO_CUSTOMER_EDGE)

    logger.info("Pull getEnterpriseEdges")
    try:
        sleep(0.5)
        # NEEDS ENHANCEMENTE FOR VCO PROPERTIES
        # We will try to get this info with licenses, if that fails we will get without license
        try:
            params = {'enterpriseId': customer['id'],
                      'with': ['site', 'configuration', 'recentLinks', 'vnfs', 'licenses', 'cloudServices']}
            logger.info(params)
            kwargs = {'timeout': 300}
            get_edges = client.call_api('/enterprise/getEnterpriseEdges', params, **kwargs)
            logger.info('Pull getEnterpriseEdges:DONE')
        except ApiException:
            logger.error('Unable to getEnterpriseEdges with license, getting without license')
            params = {'enterpriseId': customer['id'],
                      'with': ['site', 'configuration', 'recentLinks', 'vnfs', 'cloudServices']}
            logger.info(params)
            kwargs = {'timeout': 300}
            get_edges = client.call_api('/enterprise/getEnterpriseEdges', params, **kwargs)
            logger.info('Pull getEnterpriseEdges:DONE')
        if len(get_edges) == 0:
            logger.info('This customer has no Edges, nothing to do here')
            return
    except Exception as e:
        logger.critical('getEnterpriseEdges:ERROR')
        log_critical_error(ex=e, log_name=VCO_CUSTOMER_EDGE)
        return

    for edge in get_edges:
        try:
            process_basic_edge(mysql_cursor, mysql_handle, customer, customer_name, vco_list, vco, edge, cfg=cfg,
                               force_run=force_run)
        except Exception as e:
            log_critical_error(ex=e, log_name=VCO_CUSTOMER_EDGE)
    permissions = determine_full_permissions_to_this_customer(client, customer, VCO_CUSTOMER_EDGE)

    if permissions:
        get_services = []
        identifiable_applications = []
        configuration = []

        logger.info("Pull getEnterpriseServices")
        try:
            params = {"enterpriseId": customer["id"], "with": ["configuration", "profileCount", "edgeUsage"]}
            kwargs = {"timeout": 300}
            get_services = client.call_api('/enterprise/getEnterpriseServices', params, **kwargs)
            logger.info("Pull getEnterpriseServices DONE")
        except ApiException:
            logger.error('Unable to getEnterpriseServices')
        except Exception as e:
            logger.critical("getEnterpriseServices:ERROR")
            log_critical_error(ex=e, log_name=VCO_CUSTOMER_EDGE)

        sleep(0.5)

        logger.info("Get Enterprice Configuration")
        try:
            kwargs = {"timeout": 300}
            params = {"enterpriseId": customer["id"], "with": ["edgeCount", "modules", "refs"]}
            configuration = client.call_api('/enterprise/getEnterpriseConfigurations', params, **kwargs)
            # logger.info( json.dumps(alert, indent=4, sort_keys=True)
            logger.info("Get Enterprice Configuration Done")
        except ApiException:
            logger.error('Unable to getEnterpriseConfigurations')
        except Exception as e:
            logger.critical("getEnterpriseConfigurations:ERROR")
            log_critical_error(ex=e, log_name=VCO_CUSTOMER_EDGE)
            return

        try:
            logger.info("Pull getIdentifiableApplications")
            sleep(0.5)
            date = datetime.utcnow()
            date_before = date - timedelta(days=15)
            start = int(calendar.timegm(date_before.timetuple())) * 1000
            params = {"enterpriseId": customer["id"]}
            logger.info(params)
            kwargs = {"timeout": 200}
            identifiable_applications = client.call_api('/configuration/getIdentifiableApplications', params, **kwargs)
            logger.info("Pull getIdentifiableApplications:DONE")
        except ApiException:
            logger.error('Unable to getIdentifiableApplications')
        except Exception as e:
            logger.critical("getIdentifiableApplications:ERROR - q8QG4fR59dEV4f7e6gv")
            logger.error("getIdentifiableApplications:ERROR")
            log_critical_error(ex=e, log_name=VCO_CUSTOMER_EDGE)

            return

        for edge in get_edges:
            try:
                process_full_edge(mysql_cursor, mysql_handle, customer, customer_name, vco_list, vco, client, edge,
                                  get_services, configuration, force_run, identifiable_applications)
            except Exception as e:
                log_critical_error(ex=e, log_name=VCO_CUSTOMER_EDGE)

        if force_run or random.random() < 0.1:
            # Customer attributes don't change often we can update once in 10 days
            try:
                process_attributes_full_customer(mysql_cursor, mysql_handle, customer, customer_name, vco_list, vco, client,
                                                 get_edges, get_services, configuration, force_run, VCO_CUSTOMER_EDGE)
            except Exception as e:
                log_critical_error(ex=e, log_name=VCO_CUSTOMER_EDGE)
    return True


def process_attributes_full_customer(mysql_cursor, mysql_handle, customer, Customer_NAME, vco_list, vco, client,
                                     get_edges, get_services, configuration, force_run, VCO_CUSTOMER_EDGE):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.DEBUG)

    if sql_queries.determine_if_any_edge_has_attribute_in_customer(mysql_cursor, mysql_handle, customer["logicalId"],
                                                                   VCO_CUSTOMER_EDGE, "HA", "NONE"):
        sql_inserts.mysql_PowerBI_CUSTOMER_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer["logicalId"],
                                                                    VCO_CUSTOMER_EDGE, "HA_bool", False)
    else:
        sql_inserts.mysql_PowerBI_CUSTOMER_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer["logicalId"],
                                                                    VCO_CUSTOMER_EDGE, "HA_bool", True)

    if sql_queries.determine_if_any_edge_has_attribute_in_customer(mysql_cursor, mysql_handle, customer["logicalId"],
                                                                   VCO_CUSTOMER_EDGE, "bgp_bool", "True"):
        sql_inserts.mysql_PowerBI_CUSTOMER_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer["logicalId"],
                                                                    VCO_CUSTOMER_EDGE, "BGP_BOOL", True)
    else:
        sql_inserts.mysql_PowerBI_CUSTOMER_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer["logicalId"],
                                                                    VCO_CUSTOMER_EDGE, "BGP_BOOL", False)

    if sql_queries.determine_if_any_edge_has_attribute_in_customer(mysql_cursor, mysql_handle, customer["logicalId"],
                                                                   VCO_CUSTOMER_EDGE, "ospf_bool", "True"):
        sql_inserts.mysql_PowerBI_CUSTOMER_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer["logicalId"],
                                                                    VCO_CUSTOMER_EDGE, "OSPF_BOOL", True)
    else:
        sql_inserts.mysql_PowerBI_CUSTOMER_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer["logicalId"],
                                                                    VCO_CUSTOMER_EDGE, "OSPF_BOOL", False)

    if sql_queries.determine_if_any_edge_has_attribute_in_customer(mysql_cursor, mysql_handle, customer["logicalId"],
                                                                   VCO_CUSTOMER_EDGE, "HA", "VRRP"):
        sql_inserts.mysql_PowerBI_CUSTOMER_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer["logicalId"],
                                                                    VCO_CUSTOMER_EDGE, "VRRP_bool", True)
    else:
        sql_inserts.mysql_PowerBI_CUSTOMER_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer["logicalId"],
                                                                    VCO_CUSTOMER_EDGE, "VRRP_bool", False)

    if sql_queries.determine_if_any_edge_has_attribute_in_customer(mysql_cursor, mysql_handle, customer["logicalId"],
                                                                   VCO_CUSTOMER_EDGE, "HA", "CLUSTER"):
        sql_inserts.mysql_PowerBI_CUSTOMER_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer["logicalId"],
                                                                    VCO_CUSTOMER_EDGE, "Cluster_bool", True)
    else:
        sql_inserts.mysql_PowerBI_CUSTOMER_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer["logicalId"],
                                                                    VCO_CUSTOMER_EDGE, "Cluster_bool", False)

    if sql_queries.determine_if_any_edge_has_attribute_in_customer(mysql_cursor, mysql_handle, customer["logicalId"],
                                                                   VCO_CUSTOMER_EDGE, "Private_LINKS_bool", "True"):
        sql_inserts.mysql_PowerBI_CUSTOMER_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer["logicalId"],
                                                                    VCO_CUSTOMER_EDGE, "MPLS_BOOL", True)
    else:
        sql_inserts.mysql_PowerBI_CUSTOMER_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer["logicalId"],
                                                                    VCO_CUSTOMER_EDGE, "MPLS_BOOL", False)

    if sql_queries.determine_if_any_edge_has_attribute_in_customer(mysql_cursor, mysql_handle, customer["logicalId"],
                                                                   VCO_CUSTOMER_EDGE, "Public_LINKS_BACKUP", "1"):

        sql_inserts.mysql_PowerBI_CUSTOMER_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer["logicalId"],
                                                                    VCO_CUSTOMER_EDGE, "BACKUP_LINK", True)
    else:
        sql_inserts.mysql_PowerBI_CUSTOMER_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer["logicalId"],
                                                                    VCO_CUSTOMER_EDGE, "BACKUP_LINK", False)

    if (sql_queries.determine_if_any_edge_has_attribute_in_customer(mysql_cursor, mysql_handle, customer["logicalId"],
                                                                    VCO_CUSTOMER_EDGE, "PUBLIC_LINKS_WIRELESS",
                                                                    "1") or sql_queries.determine_if_any_edge_has_attribute_in_customer(
        mysql_cursor, mysql_handle, customer["logicalId"], VCO_CUSTOMER_EDGE, "PUBLIC_LINKS_WIRELESS", "2")):

        sql_inserts.mysql_PowerBI_CUSTOMER_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer["logicalId"],
                                                                    VCO_CUSTOMER_EDGE, "WIRELESS_LINK", True)
    else:
        sql_inserts.mysql_PowerBI_CUSTOMER_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer["logicalId"],
                                                                    VCO_CUSTOMER_EDGE, "WIRELESS_LINK", False)

    # Detect Segments
    Segments_num = 0
    Segments_bool = False
    Version = "Not Set"
    for edge in get_edges:
        local_segments = 0
        if edge["edgeState"] == "CONNECTED":
            Version = edge["buildNumber"]
        for config in edge["configuration"]["enterprise"]["modules"]:
            if config["name"] == "deviceSettings" and config["isEdgeSpecific"]:
                config_device_settings = config

                # logger.info(json.dumps(config, indent=4, sort_keys=True))
                if 'edgeSpecificData' in config_device_settings.keys():
                    if 'segments' in config_device_settings["edgeSpecificData"].keys():
                        for segment in config_device_settings["edgeSpecificData"]["segments"]:
                            Segments_bool = True
                            local_segments = local_segments + 1
                            if local_segments > Segments_num:
                                Segments_num = local_segments

    sql_inserts.mysql_PowerBI_CUSTOMER_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer["logicalId"],
                                                                VCO_CUSTOMER_EDGE, "Segments_num", Segments_num)
    sql_inserts.mysql_PowerBI_CUSTOMER_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer["logicalId"],
                                                                VCO_CUSTOMER_EDGE, "Segments_bool", Segments_bool)
    sql_inserts.mysql_PowerBI_CUSTOMER_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer["logicalId"],
                                                                VCO_CUSTOMER_EDGE, "Version", Version)

    # Updating routes is expensive and not super important, doing this just from time to time
    date_now = datetime.utcnow()
    ROUTE_NUM = 0
    number_of_routes_changes = 0
    try:
        sleep(0.5)
        params = {"enterpriseId": customer["id"]}
        logger.info(params)
        kwargs = {"timeout": 300}
        get_routes = client.call_api('/enterprise/getEnterpriseRouteTable', params, **kwargs)
        logger.info("Pull getEnterpriseRouteTable:DONE")
        if len(get_routes) == 0:
            logger.info("This customer has no routes")
    except ApiException:
        logger.error('Unable to getEnterpriseRouteTable')
    except Exception as e:
        logger.critical("getEnterpriseRouteTable:ERROR")
        log_critical_error(ex=e, log_name=VCO_CUSTOMER_EDGE)

    try:
        logger.info("Processing routes")
        for route in get_routes["subnets"]:
            for exit_ in route["eligableExits"]:
                if exit_["type"] != "DIRECT":
                    pass
            for exit_ in route["preferredExits"]:
                if exit_["type"] != "DIRECT":
                    ROUTE_NUM = ROUTE_NUM + 1
            if "learnedRoute" in route:

                date_route_got_changed = datetime.strptime(route["learnedRoute"]["modified"], '%Y-%m-%dT%H:%M:%S.%fZ')
                seconds = int(date_now.strftime('%s')) - int(date_route_got_changed.strftime('%s'))
                minutes = seconds / 60

                if (minutes < 1440):
                    # print ("This Route got changed last 24H")
                    number_of_routes_changes = number_of_routes_changes + 1
    except KeyError:
        logger.error("Processing routes:ERROR")
    except Exception as e:
        logger.critical("Processing routes:ERROR")
        log_critical_error(ex=e, log_name=VCO_CUSTOMER_EDGE)
    sql_inserts.mysql_PowerBI_CUSTOMER_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer["logicalId"],
                                                                VCO_CUSTOMER_EDGE, "ROUTE_NUM", ROUTE_NUM)
    sql_inserts.mysql_PowerBI_CUSTOMER_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer["logicalId"],
                                                                VCO_CUSTOMER_EDGE, "ROUTE_CHANGE",
                                                                number_of_routes_changes)


def process_full_edge(mysql_cursor, mysql_handle, customer, Customer_NAME, vco_list, vco, client, edge, get_services,
                      configuration, force_run=False, identifiable_applications=[]):
    VCO_CUSTOMER_EDGE = vco_list[vco]['link'] + ":" + Customer_NAME + ":" + edge["name"]
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.INFO)
    if not force_run:
        if sql_queries.determine_if_edge_needs_update(mysql_cursor, mysql_handle, edge["logicalId"], VCO_CUSTOMER_EDGE):
            logger.info("UPDATING EDGE SINCE ITS NOT UPDATED LAST 8 DAYS")
        elif (datetime.now() - datetime.strptime(re.split('T| ', edge["created"])[0], '%Y-%m-%d')).days % 6 == 0:
            logger.info("UPDATING BASED ON CREATION DATE")
        else:
            logger.info("NO UPDATE NEEDED")
            return
    else:
        logger.info("Force RUN")

    if not edge["logicalId"]:
        logger.info("This edge is empty in VCO nothing to do here")
        return

    # Process HA
    update_ha_and_cluster(mysql_cursor, mysql_handle, customer["logicalId"], edge, vco, VCO_CUSTOMER_EDGE, get_services)

    ### CHECK IF EDGE is CONNECTED

    if edge["edgeState"] == "CONNECTED":
        logger.info("EDGE IS CONNECTED we will process events config stack and so on")
    else:
        logger.info("edge is not connected we will be done here")
        return

    ##########
    # Process events - This should be light
    ##########
    logger.info("Pull getEnterpriseEvents")
    events = {'data': []}
    try:
        sleep(0.5)
        date = datetime.utcnow()
        date_before = date - timedelta(days=15)
        params = {"enterpriseId": customer["id"], "edgeId": edge["id"],
                  "interval": {"start": date_before.strftime('%Y-%m-%dT%H:%M:%S.%fZ')[:-3]}}
        logger.info(params)
        kwargs = {"timeout": 200}
        events = client.call_api('/event/getEnterpriseEvents', params, **kwargs)
        logger.info("Pull getEnterpriseEdges:DONE")
    except ApiException:
        logger.error('Unable to getEnterpriseEvents')
    except Exception as e:
        logger.critical("getEnterpriseEvents:ERROR")
        log_critical_error(ex=e, log_name=VCO_CUSTOMER_EDGE)

    update_edge_events(mysql_cursor=mysql_cursor, mysql_handle=mysql_handle, edge=edge,
                       VCO_CUSTOMER_EDGE=VCO_CUSTOMER_EDGE, events=events)

    update_edge_alerts_based_on_events(mysql_cursor, mysql_handle, customer, edge, vco, VCO_CUSTOMER_EDGE, client,
                                       events, configuration)

    # update_edge_other_alerts

    ##########
    ## Process Links - This is light
    ##########

    logger.info("Pull getEdgeConfigurationStack")
    try:
        sleep(0.5)
        params = {"enterpriseId": customer["id"], "edgeId": edge["id"], "with": ["modules"]}
        logger.info(params)
        kwargs = {"timeout": 200}
        edge_config_stack = client.call_api('/edge/getEdgeConfigurationStack', params, **kwargs)
        logger.info("Pull getEnterpriseEdges:DONE")
    except ApiException:
        logger.error('Unable to getEdgeConfigurationStack')
    except Exception as e:
        logger.critical("getEdgeConfigurationStack:ERROR")
        log_critical_error(ex=e, log_name=VCO_CUSTOMER_EDGE)
        return

        # Alerts based on config
    update_edge_alerts_based_on_configuration(mysql_cursor, mysql_handle, customer, edge, vco, VCO_CUSTOMER_EDGE,
                                              client, edge_config_stack, configuration)
    # dump_appd+id was a request from engineering
    # dump_appid_specific_qos_rules(customer_name=customer['name'], edge_uuid=edge['logicalId'], vco_name=vco,
    #                              log_prefix=VCO_CUSTOMER_EDGE, edge_config_stack=edge_config_stac

    logger.info("Pull getEdgeLinkMetrics")
    try:
        sleep(0.5)
        date = datetime.utcnow()
        date_before = date - timedelta(days=5)
        start = int(calendar.timegm(date_before.timetuple())) * 1000
        params = {"edgeId": edge["id"], "enterpriseId": customer["id"], "interval": {"start": start},
                  "with": ["bpsOfBestPathRx", "bpsOfBestPathTx", "scoreTx", "scoreRx", "bytesRx", "bytesTx"]}
        logger.info(params)
        kwargs = {"timeout": 200}
        link_metrics = client.call_api('/metrics/getEdgeLinkMetrics', params, **kwargs)
        logger.info("Pull getEdgeLinkMetrics:DONE")
    except ApiException:
        logger.error('Unable to getEdgeLinkMetrics')
    except Exception as e:
        logger.critical("getEdgeLinkMetrics:ERROR")
        log_critical_error(ex=e, log_name=VCO_CUSTOMER_EDGE)
        return

    logger.info("Pull getEdgeLinkSeries")
    try:
        sleep(0.5)
        date = datetime.utcnow()
        date_before = date - timedelta(days=30)
        start = int(calendar.timegm(date_before.timetuple())) * 1000
        # params = {"edgeId": edge["id"], "enterpriseId": customer["id"], "interval": {"start": start},"with": ["bytesRx", "bytesTx"]}
        ####Adding temporary start and end interval from 1 December 2019 till 31 December 2019 for pre-covid19
        params = {"edgeId": edge["id"], "enterpriseId": customer["id"],
                  "interval": {"start": 1575118800000, "end": 1577795400000}, "with": ["bytesRx", "bytesTx"]}
        logger.info(params)
        kwargs = {"timeout": 200}
        link_series = client.call_api('/metrics/getEdgeLinkSeries', params, **kwargs)
        logger.info("Pull getEdgeLinkSeries:DONE")
    except ApiException:
        logger.error('Unable to getEdgeLinkSeries')
    except Exception as e:
        logger.critical("getEdgeLinkSeries:ERROR")
        log_critical_error(ex=e, log_name=VCO_CUSTOMER_EDGE)
        return

    ###########
    ## Process QoE
    ###########

    update_edge_links(mysql_cursor, mysql_handle, customer, edge, vco, VCO_CUSTOMER_EDGE, client, link_metrics,
                      configuration, edge_config_stack)

    update_edge_overlay_link(mysql_cursor, mysql_handle, customer, edge, vco, VCO_CUSTOMER_EDGE, client)

    update_license_and_link_usage(mysql_cursor, mysql_handle, customer, edge, vco, VCO_CUSTOMER_EDGE, client,
                                  link_metrics, link_series, configuration, edge_config_stack)

    update_edge_qoe(mysql_cursor, mysql_handle, customer, edge, vco, VCO_CUSTOMER_EDGE, client)
    snmpv3_status(mysql_cursor, mysql_handle, customer, edge, vco, VCO_CUSTOMER_EDGE, edge_config_stack)

    update_segment_firewall(mysql_cursor, mysql_handle, edge, VCO_CUSTOMER_EDGE, edge_config_stack)

    return


def process_basic_edge(mysql_cursor, mysql_handle, customer, Customer_NAME, vco_list, vco, edge, cfg: Config,
                       force_run=False):
    VCO_CUSTOMER_EDGE = vco_list[vco]['link'] + ":" + Customer_NAME + ":" + edge["name"]
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.INFO)

    if not edge["logicalId"]:
        logger.info("This edge is empty in VCO nothing to do here")
        return

    sql_queries.determine_if_edge_exists_in_mysql_creates_if_not(mysql_cursor, mysql_handle, customer["logicalId"],
                                                                 edge, vco, VCO_CUSTOMER_EDGE)

    update_location_information(mysql_cursor, mysql_handle, customer["logicalId"], edge, vco, VCO_CUSTOMER_EDGE,
                                cfg=cfg)
    # Process Location
    update_attributes(mysql_cursor, mysql_handle, customer["logicalId"], edge, vco, VCO_CUSTOMER_EDGE)

    # Process routing features (OSPF,BGP,Multicast,Static/Netflox"
    update_routing(mysql_cursor, mysql_handle, customer["logicalId"], edge, vco, VCO_CUSTOMER_EDGE)
    # Process firewall
    update_non_segment_firewall(mysql_cursor, mysql_handle, customer["logicalId"], edge, vco, VCO_CUSTOMER_EDGE)
    # Process Edge VNF
    update_edge_vnf(mysql_cursor, mysql_handle, edge, VCO_CUSTOMER_EDGE)
    # Process Cloud Security service
    update_edge_css(mysql_cursor, mysql_handle, edge, VCO_CUSTOMER_EDGE, cfg=cfg)
    # Process QOS
    update_qos(mysql_cursor, mysql_handle, customer["logicalId"], edge, vco, VCO_CUSTOMER_EDGE)
    # Process config specific
    update_config_specific(mysql_cursor, mysql_handle, customer["logicalId"], edge, vco, VCO_CUSTOMER_EDGE)

    # Process link information
    update_recent_link_list(mysql_cursor, mysql_handle, customer["logicalId"], edge, vco, VCO_CUSTOMER_EDGE)

    # Process link information
    update_vco_license(mysql_cursor=mysql_cursor, mysql_handle=mysql_handle, edge=edge,
                       vco_customer_edge=VCO_CUSTOMER_EDGE)


def uo(args, **kwargs):
    return urllib.request.urlopen(args, cafile=certifi.where(), **kwargs)


def update_location_information(mysql_cursor, mysql_handle, customer_ID, edge, vco, VCO_CUSTOMER_EDGE, cfg: Config):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.INFO)

    geolocator = Nominatim(user_agent="get link details")
    geolocator.urlopen = uo

    if not sql_queries.determine_if_edge_needs_location_update(mysql_cursor, mysql_handle, edge['logicalId'],
                                                               VCO_CUSTOMER_EDGE):
        if random.random() > 0.01:
            return
        logger.info("Updating address 1 in 100 times")

    # GEOLOCATION IS DEFINED BY
    City = "Not set"
    State = "Not set"
    Country = "Not set"
    PostalCode = "Not set"
    lat = 0
    lon = 0
    Geospecific = "Not set"
    print(edge["site"])
    # Try to get location using geolocation
    geolocation_worked = False
    try:
        geos = json.load(open('DataFiles/country.json', 'r'))
        try:
            ### NOTE THIS CODE NEEDS TO BE IMPROVED, WE SHOULD TAKE IN ACCOUNT WHEN WE DONT HAVE LAT AND LONG BUT WE HAVE AN ADDRESS THAT WE CAN USE TO DETERMINE LAT AND LON
            if edge["site"]["lat"] != None and edge["site"]["lon"] != None:
                lat = edge["site"]["lat"]
                lon = edge["site"]["lon"]
                geoval = '%s,%s' % (edge["site"]["lat"], edge["site"]["lon"])
                location = geolocator.reverse(geoval, language="en-US,en")
                sleep(10)  # sleeping since there is a limit of quota usage
                data = location.raw
                data = data['address']
                logger.info(data)

                # if data is available via geo let's use that since it's standardized, otherwise use VCO

                if 'state' in data:
                    State = str(data['state'])
                elif edge["site"]["state"] != None:
                    State = edge["site"]["state"]

                if 'city' in data:
                    City = str(data['city'])
                elif 'county' in data:
                    City = str(data['county'])
                elif edge["site"]["city"] != None:
                    City = edge["site"]["city"]

                if 'country' in data:
                    Country = str(data["country"])
                elif edge["site"]["country"] != None:
                    Country = edge["site"]["country"]

                if 'postcode' in data:
                    str(data['postcode'])
                    PostalCode = str(data['postcode'])
                    if re.findall('[^A-Za-z0-9_  .-]', PostalCode):
                        PostalCode = edge["site"]["postalCode"]
                    else:
                        logger.info("regular string")
                        PostalCode = PostalCode
                else:
                    PostalCode = edge["site"]["postalCode"]

                for geo in geos:
                    if geo["ISO"].lower() == data['country_code'].lower():
                        Geospecific = geo["REG"]

                geolocation_worked = True
            else:
                logger.info("try to build location with links")
                for link in edge["recentLinks"]:
                    # Checking that this is not a Private link
                    if link['lat'] != 37.402866 and link['lon'] != -122.117332:
                        logger.info("got here")
                        lat = link['lat']
                        lon = link['lon']
                        geoval = '%s,%s' % (lat, lon)
                        location = geolocator.reverse(geoval, language="en-US,en")
                        sleep(10)
                        data = location.raw
                        data = data['address']
                        logger.info(data)

                        if 'state' in data:
                            State = str(data['state'])
                        elif edge["site"]["state"] != None:
                            State = edge["site"]["state"]

                        if 'city' in data:
                            City = str(data['city'])
                        elif 'county' in data:
                            City = str(data['county'])
                        elif edge["site"]["city"] != None:
                            City = edge["site"]["city"]

                        if 'country' in data:
                            Country = str(data["country"])
                        elif edge["site"]["country"] != None:
                            Country = edge["site"]["country"]

                        if 'postcode' in data:
                            str(data['postcode'])
                            PostalCode = str(data['postcode'])
                            if not re.findall('[^A-Za-z0-9_  .-]', str(data['postcode'])):
                                PostalCode = PostalCode

                        for geo in geos:
                            if geo["ISO"].lower() == data['country_code'].lower():
                                Geospecific = geo["REG"]
                        geolocation_worked = True
                    else:
                        logger.info("using maxmind")
                        try:
                            client = geoip2.webservice.Client(cfg.maxmind.account_id, cfg.maxmind.license_key)
                        except Exception as e:
                            log_critical_error(ex=e, log_name=VCO_CUSTOMER_EDGE)
                            pass
                        try:
                            response = client.insights(link['ipAddress'])
                            lat = response.location.latitude
                            lon = response.location.longitude
                            geoval = '%s,%s' % (lat, lon)
                            location = geolocator.reverse(geoval, language="en-US,en")
                            sleep(10)
                            data = location.raw
                            data = data['address']
                            logger.info(data)

                            if 'state' in data:
                                State = str(data['state'])
                            elif edge["site"]["state"] != None:
                                State = edge["site"]["state"]

                            if 'city' in data:
                                City = str(data['city'])
                            elif 'county' in data:
                                City = str(data['county'])
                            elif edge["site"]["city"] != None:
                                City = edge["site"]["city"]

                            if 'country' in data:
                                Country = str(data["country"])
                            elif edge["site"]["country"] != None:
                                Country = edge["site"]["country"]

                            if 'postcode' in data:
                                str(data['postcode'])
                                PostalCode = str(data['postcode'])
                                if not re.findall('[^A-Za-z0-9_  .-]', str(data['postcode'])):
                                    PostalCode = PostalCode

                            for geo in geos:
                                if geo["ISO"].lower() == data['country_code'].lower():
                                    Geospecific = geo["REG"]
                            geolocation_worked = True
                        except Exception as e:
                            log_critical_error(ex=e, log_name=VCO_CUSTOMER_EDGE)

            logger.info(Country + City + PostalCode + State + Geospecific)
            for geo in geos:
                if geo["Country"] == Country or geo["ISO"] == Country:
                    Geospecific = geo["REG"]

        except Exception as e:
            log_critical_error(ex=e, log_name=VCO_CUSTOMER_EDGE)
            GEOLOCATION = False

        if not geolocation_worked:
            if edge["site"]["city"] != None:
                City = edge["site"]["city"]

            if edge["site"]["country"] != None:
                Country = edge["site"]["country"]
                if Country == "United States" or Country == "US" or Country == "USA":
                    Country = "United States of America"
                elif Country == "TH":
                    Country = "Thailand"
                elif Country == "DE":
                    Country = "Germany"
                elif Country == "ES":
                    Country = "Spain"
                elif Country == "UK":
                    Country = "United Kingdom"
                elif Country == "NL":
                    Country = "Netherlands"
                elif Country == "CA":
                    Country = "Canada"
                elif Country == "FR":
                    Country = "France"
                elif Country == "IT":
                    Country = "Italy"
                elif Country == "AU":
                    Country = "Australia"
                else:
                    Country = edge["site"]["country"]

            if edge["site"]["postalCode"] != None:
                PostalCode = edge["site"]["postalCode"]

            if edge["site"]["state"] != None:
                State = edge["site"]["state"]

            logger.info(Country + City + PostalCode + State + Geospecific)
            for geo in geos:
                if geo["Country"] == Country or geo["ISO"] == Country:
                    Geospecific = geo["REG"]

        if Country != "Not set":
            sql_inserts.mysql_PowerBI_EDGE_UPDATE_LOCATION(mysql_handle, mysql_cursor, customer_ID, edge, vco,
                                                           VCO_CUSTOMER_EDGE, City, State, Country, PostalCode, lat,
                                                           lon, Geospecific)

    except Exception as e:
        logger.critical("UNABLE TO BUILD LOCATION")
        log_critical_error(ex=e, log_name=VCO_CUSTOMER_EDGE)


def update_edge_events(mysql_cursor, mysql_handle, edge, VCO_CUSTOMER_EDGE, events):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.INFO)
    events_to_skip = ['EDGE_INTERFACE_UP', 'EDGE_INTERFACE_DOWN', 'EDGE_NEW_DEVICE', 'LINK_DEAD', 'LINK_ALIVE',
                      'MGD_CONF_APPLIED', 'EDGE_DOWN', 'EDGE_UP']
    for event in events['data']:
        date = datetime.strptime(event['eventTime'], '%Y-%m-%dT%H:%M:%S.%fZ')
        name = event['event']
        type_ = 'Event'
        if name not in events_to_skip:
            sql_inserts.mysql_PowerBI_EDGE_INSERT_EVENT(mysql_handle=mysql_handle, mysql_cursor=mysql_cursor,
                                                        Customer_ID=None, edge=edge, VCO=None,
                                                        VCO_CUSTOMER_EDGE=VCO_CUSTOMER_EDGE, Date=date, Name=name,
                                                        Type=type_)
    return


def determine_if_edge_is_hub(configuration, edge, VCO_CUSTOMER_EDGE):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.INFO)
    for config in configuration:
        # print config["id"]
        for module in config["modules"]:
            if module["name"] == "deviceSettings" and "refs" in module.keys():
                if "deviceSettings:vpn:edgeHub" in module["refs"].keys():
                    try:
                        if "data" in module["refs"]["deviceSettings:vpn:edgeHub"]:
                            if str(module["refs"]["deviceSettings:vpn:edgeHub"]["data"]["logicalId"]) == str(
                                    edge["logicalId"]):
                                logger.info("edge is hub")
                                return True
                    except KeyError:
                        logger.info("failed to detect if edge is hub")
                    except Exception as e:
                        logger.critical("failed to detect if edge is hub")
                        log_critical_error(ex=e, log_name=VCO_CUSTOMER_EDGE)

    return False


def update_edge_alerts_based_on_events(mysql_cursor, mysql_handle, customer, edge, vco, VCO_CUSTOMER_EDGE, client,
                                       events, configuration):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.INFO)
    date_now = datetime.utcnow()
    Date = date_now.strftime('%Y-%m-01T00:00:00.000Z')[:-3]
    for event in events["data"]:
        Name = event["event"]
        if edge["edgeState"] == "CONNECTED":
            if Name == "EDGE_HEALTH_ALERT" or Name == "EDGE_MEMORY_USAGE_ERROR":
                Type = "BADCONFIG"
                sql_inserts.mysql_PowerBI_EDGE_INSERT_EVENT(mysql_handle, mysql_cursor, customer["logicalId"], edge,
                                                            vco, VCO_CUSTOMER_EDGE, Date, Name, Type)
            if Name == "EDGE_KERNEL_PANIC":
                Type = "BADCONFIG"
                sql_inserts.mysql_PowerBI_EDGE_INSERT_EVENT(mysql_handle, mysql_cursor, customer["logicalId"], edge,
                                                            vco, VCO_CUSTOMER_EDGE, Date, Name, Type)
            if determine_if_edge_is_hub(configuration, edge, VCO_CUSTOMER_EDGE) and Name == "EDGE_TUNNEL_CAP_WARNING":
                Type = "BADCONFIG"
                sql_inserts.mysql_PowerBI_EDGE_INSERT_EVENT(mysql_handle, mysql_cursor, customer["logicalId"], edge,
                                                            vco, VCO_CUSTOMER_EDGE, Date, "HUB_TUNNEL_CAP_WARNING",
                                                            Type)


def dump_appid_specific_qos_rules(customer_name: str, edge_uuid: str, vco_name: str, log_prefix: str,
                                  edge_config_stack: List[dict]) -> None:
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': log_prefix})
    today = datetime.today()
    filename = f'/tmp/appid_specific_rules_{vco_name.replace(".", "-")}_{str(today)}.csv'
    with open(filename, 'a+') as csv_file:
        try:
            for config in edge_config_stack:
                if 'modules' in config:
                    for module in config['modules']:
                        if module['name'] == 'QOS':
                            data = module['data']
                            if 'segments' in data.keys():
                                for segment in data['segments']:
                                    if 'rules' in segment.keys():
                                        for rule in segment['rules']:
                                            match = rule['match']
                                            if match['appid'] != -1:
                                                app_dict = {'customer': customer_name, 'edge': edge_uuid,
                                                            'vco': vco_name, 'rule_name': rule['name'],
                                                            'appid': match['appid']}
                                                w = csv.DictWriter(csv_file, app_dict.keys())
                                                w.writerow(app_dict)
        except KeyError as e:
            logger.critical(f'an unplanned keyerror occured')
        except Exception as e:
            log_critical_error(ex=e, log_name=log_prefix)
            logger.critical('dump_appid_specific_rules error')
    return


def update_edge_alerts_based_on_configuration(mysql_cursor, mysql_handle, customer, edge, vco, VCO_CUSTOMER_EDGE,
                                              client, edge_config_stack, configuration):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.INFO)
    date_now = datetime.utcnow()
    Date = date_now.strftime('%Y-%m-01T00:00:00.000Z')[:-3]

    outdatedversion = ["R30-20170828-GA", "R31-20171207-GA", "R31-20180125-GA-22337", "R31-20180223-GA-22230",
                       "R311-20180315-GA", "R311-20180317-GA", "R312-20180607-GA", "R312-20180607-GA-24109",
                       "R312-20180716-GA", "R312-20180716-GA-24162", "R312-20180716-GA-24652", "R312-20180716-GA-25117",
                       "R312-20180716-GA-25713", "R312-20180716-GA-POC", "R320-20180409-GA", "R320-20180409-GA-23248",
                       "R320-20180409-GA-23248-24818", "R320-20180409-GA-23706", "R320-20180409-GA-23706-SKYUS",
                       "R320-20180427-GA", "R320-20180508-GA-MFG", "R320-20180911-GA-ADIDAS", "R330-20190619-GA",
                       "R330-20190630-GA", "R330-20190711-GA", "R330-20190723-GA", "R330-20190723-GA-35295",
                       "R330-20190723-GA-35836-31747", "R330-20190807-GA-610", "R330-MAESTRO-20190404-MFG",
                       "R330-MTHD-20190328-GA", "R331-20190815-GA", "R331-20190925-GA", "R331-20190925-GA-35295",
                       "R331-20190925-GA-35295-36582-34794-35562-34370", "R331-20190925-GA-35295-36582-35562",
                       "R331-20190925-GA-36719", "R331-20191021-GA", "R331-20191021-GA-28378-34801-37003",
                       "R331-20191212-GA-ATT", "R331-20200120-GA-ATT", "R331-20200120-GA-ATT-DES-ONLY",
                       "R340-20200128-BETA-f59c1d0c7d", "R340-20200131-GA-d5aabea079", "R340-20200218-GA-c57f8316dd"]

    if edge["buildNumber"] in outdatedversion:
        Type = "BADCONFIG"
        logger.debug("this is a badconfig")
        Name = "NOT_RECOMMENDED_VERSION"
        sql_inserts.mysql_PowerBI_EDGE_INSERT_EVENT(mysql_handle, mysql_cursor, customer["logicalId"], edge, vco,
                                                    VCO_CUSTOMER_EDGE, Date, Name, Type)
    else:
        logger.info("Valid version")

    eoslversion = ["R15-20141211-GA", "R15-AFTER-R11-MERGE-v1-676-g8e2f74a", "R171-20150420-P1", "R18-20150526-GA",
                   "R182-20150702-GA", "R183-20150820-VCG", "R183-20150903-P1", "R183-20150911-METTEL",
                   "R183-20151119-METTEL-B", "R184-20151125-P1", "R184-20151222-P1-VONAGE", "R185-20160229-GA",
                   "R185-20160316-GA-VONAGE", "R185-20160411-GA-PPTP-HOTFIX", "R185-20160413-P1-GA",
                   "R185-20160530-P2-GA", "R185-20160530-P2-GA-VONAGE", "R185-20160616-P3-GA",
                   "R185-20160625-P3-GA-VONAGE", "R186-20160718-GA-APN", "R186-20160822-P1-GA-VONAGE",
                   "R20-20160401-GA", "R20-20160402-GA", "R20-20160415-P1-GA", "R20-20160424-P2-GA",
                   "R20-20160427-P2-GA", "R20-20160531-P3-GA", "R20-20160708-MFG", "R21-20160704-GA", "R21-20160708-GA",
                   "R21-20160715-GA", "R21-20160715-GA-13429-13424", "R211-20160809-GA", "R211-20160814-GA",
                   "R212-20160904-GA", "R212-20160909-GA", "R213-20160926-GA", "R213-20160929-GA", "R213-20161006-GA",
                   "R213-20161006-GA-HA", "R213-20161008-GA-CLOUD-EDGE-PPTP", "R214-20161111-GA", "R215-20161227-GA",
                   "R215-20161227-GA-16281", "R215-20161227-GA-MITEL", "R215-20170113-GA-14155", "R221-20161109-GA",
                   "R23-20161227-GA", "R23-20161227-GA-16187", "R23-20161227-GA-MITEL", "R23-20170118-GA",
                   "R231-20170217-GA", "R232-20170318-GA", "R232-20170318-GA-16997", "R232-20170403-GA-METTEL",
                   "R232-20170414-GA-TPAC", "R233-20170416-QA", "R233-20170426-GA", "R233-20170426-GA-MITEL",
                   "R233-20170515-GA-TPAC", "R233-20170516-GA-WINDSTREAM", "R233-20170517-GA-CDK",
                   "R233-20170522-GA-MDM89", "R234-20170606-GA", "R234-20170606-GA-16721", "R234-20170606-GA-18670",
                   "R234-20170606-GA-18781", "R234-20170606-GA-MITEL", "R234-20170606-GA-VONAGE",
                   "R234-20170815-GA-VONAGE", "R234-20170825-GA-TPAC", "R24-20170418-BETA", "R24-20170425-BETA",
                   "R24-20170425-QA", "R24-20170428-GA", "R241-20170503-QA-23-g68926a4", "R241-20170531-QA-37-g5b9d1cc",
                   "R241-20170531-QA-39-gddbac50", "R241-20170612-RC1", "R241-20170615-GA", "R241-20170621-GA",
                   "R241-20170629-GA", "R241-20170720-P1-GA", "R241-20170720-P1-GA-18997", "R241-20170722-MFG",
                   "R241-20170809-MFG", "R241-20170809-MFG-2", "R242-20170714-QA", "R242-20170827-QA",
                   "R242-20170911-GA", "R242-20171004-GA-20424", "R243-20171031-GA", "R243-20171031-GA-19026",
                   "R243-20171031-GA-21313", "R243-20171031-GA-23380", "R243-20171031-GA-24968",
                   "R243-20171031-GA-MITEL", "R243-20171031-GA-MITEL-USB", "R243-20171031-GA-VONAGE",
                   "R243-20171120-GA-21288", "R243-20180123-GA-22204", "R244-20180220-GA", "R244-20180220-GA-21288",
                   "R244-20180220-GA-23706", "R244-20180220-GA-23706-24597", "R244-20180220-GA-23706-USB",
                   "R244-20180220-GA-24267", "R244-20180220-GA-24267-21988", "R244-20180220-GA-24652",
                   "R244-20180220-GA-27682", "R244-20180220-GA-USB", "R244-20180220-GA-USB-23871",
                   "R244-20180327-GA-23079", "R244-20190521-GA-31333", "R244-20190530-GA-31333", "R25-20171003-GA",
                   "R25-20171010-GA", "R25-20171103-GA", "R25-20171107-GA", "R251-20180109-GA", "R251-20180131-GA",
                   "R251-20180131-GA-21552", "R251-20180131-GA-22591", "R251-20180131-GA-23871",
                   "R251-20180317-GA-23060", "R252-20180430-GA", "R252-20180430-GA-23854", "R252-20180430-GA-24267",
                   "R252-20180430-GA-24335", "R252-20180430-GA-25167", "R252-20180430-GA-25497", "R252-20180430-GA-CDK",
                   "R252-20180430-GA-MITEL", "R252-20180430-GA-ZTEMODEM-29480", "R252-20181004-GA-CDK",
                   "R252-20181116-GA-CDK", "R252-20181116-GA-CDK-23372", "R252-20190131-GA-COCC", "R253-20180727-GA-PS",
                   "R253-20190430-GA-DG"]

    if edge["buildNumber"] in eoslversion:
        print("EOSL Version")
        Type = "BADCONFIG"
        Name = "END_OF_SUPPORT_LIFE"
        print(Date, Name, Type)
        sql_inserts.mysql_PowerBI_EDGE_INSERT_EVENT(mysql_handle, mysql_cursor, customer["logicalId"], edge, vco,
                                                    VCO_CUSTOMER_EDGE, Date, Name, Type)
    else:
        logger.info("Not a 2.x version")

    if edge["modelNumber"] == "edge1000qat" or edge["modelNumber"] == "edge3400" or edge["modelNumber"] == "edge3X00" or \
            edge["modelNumber"] == "edge840" or edge["modelNumber"] == "edge3800" and edge["edgeState"] == "CONNECTED":
        for configs in edge_config_stack:
            sw_int = []
            if configs["name"] == "Edge Specific Profile":
                for modules in configs["modules"]:
                    if modules["name"] == "deviceSettings":
                        for net in modules["data"]["lan"]["networks"]:
                            try:
                                for interface in net["interfaces"]:
                                    if interface == "GE1":
                                        if modules["data"]["ha"]["enabled"] == True:
                                            logger.info("GE1 is HA enabled")
                                        else:
                                            sw_int.append(interface)
                                    else:
                                        sw_int.append(interface)
                            except KeyError:
                                continue
                            except Exception as e:
                                logger.critical('Failure in update_edge_alerts_based_on_configuration ACEzJ9e9nDhu7WdW')
                                log_critical_error(ex=e, log_name=VCO_CUSTOMER_EDGE)
                                continue
                        if sw_int:
                            if edge["modelNumber"] == "edge1000qat":
                                Type = "BADCONFIG"
                                Date = date_now.strftime('%Y-%m-01T00:00:00.000Z')[:-3]
                                Name = "EDGE2000_SWITCHED_INT"
                                val = (Date, edge["logicalId"], Name, Type)
                                sql_inserts.mysql_PowerBI_EDGE_INSERT_EVENT(mysql_handle, mysql_cursor,
                                                                            customer["logicalId"], edge, vco,
                                                                            VCO_CUSTOMER_EDGE, Date, Name, Type)
                            elif edge["modelNumber"] == "edge3400" or edge["modelNumber"] == "edge3X00" or edge[
                                "modelNumber"] == "edge840" or edge["modelNumber"] == "edge3800":
                                Type = "BADCONFIG"
                                Date = date_now.strftime('%Y-%m-01T00:00:00.000Z')[:-3]
                                Name = edge["modelNumber"] + "_SWITCHED_INT"
                                val = (Date, edge["logicalId"], Name, Type)
                                sql_inserts.mysql_PowerBI_EDGE_INSERT_EVENT(mysql_handle, mysql_cursor,
                                                                            customer["logicalId"], edge, vco,
                                                                            VCO_CUSTOMER_EDGE, Date, Name, Type)
                            else:
                                logger.info("Catured nothing")
                        else:
                            logger.info("Good Config with edge 2000")

    for configs in edge_config_stack:
        # print json.dumps(configs, indent=4, sort_keys=True)
        if configs["name"] == "Edge Specific Profile":
            for modules in configs["modules"]:
                if modules["name"] == "WAN":
                    if "links" in modules["data"].keys():
                        for link in modules["data"]["links"]:
                            if link["bwMeasurement"] != "USER_DEFINED" and edge[
                                "edgeState"] == "CONNECTED" and determine_if_edge_is_hub(configuration, edge,
                                                                                         VCO_CUSTOMER_EDGE):
                                Type = "BADCONFIG"
                                Date = date_now.strftime('%Y-%m-01T00:00:00.000Z')[:-3]
                                Name = "HUB_WITH_DYNAMIC_BANDWIDTH"
                                sql_inserts.mysql_PowerBI_EDGE_INSERT_EVENT(mysql_handle, mysql_cursor,
                                                                            customer["logicalId"], edge, vco,
                                                                            VCO_CUSTOMER_EDGE, Date, Name, Type)
                            if link["dynamicBwAdjustmentEnabled"] and edge["edgeState"] == "CONNECTED":
                                if re.match('R2', edge["buildNumber"]) is not None:
                                    Type = "BADCONFIG"
                                    Date = date_now.strftime('%Y-%m-01T00:00:00.000Z')[:-3]
                                    Name = "R2_EDGE_DBA"
                                    sql_inserts.mysql_PowerBI_EDGE_INSERT_EVENT(mysql_handle, mysql_cursor,
                                                                                customer["logicalId"], edge, vco,
                                                                                VCO_CUSTOMER_EDGE, Date, Name, Type)


def update_attributes(mysql_cursor, mysql_handle, customer_ID, edge, vco, VCO_CUSTOMER_EDGE):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.INFO)

    Profile_ID = edge['configuration']['enterprise']['id']
    Activation_Status = edge['activationState']
    Certificate = edge['endpointPkiMode']
    Version = edge['buildNumber']
    Edge_Status = edge['edgeState']
    Model = edge['modelNumber']
    street_address = ""
    if edge['serialNumber'] != None:
        serial = edge['serialNumber']
    else:
        serial = None
    if edge['haSerialNumber'] != None:
        ha_serial = edge['haSerialNumber']
    else:
        ha_serial = None
    if edge['site']['streetAddress'] != None:
        street_address = edge['site']['streetAddress']
    if edge['site']['streetAddress2'] != None:
        street_address = street_address + ' ' + edge['site']['streetAddress2']
    # ".encode('ascii',"ignore")"
    name = re.match('[A-Za-z0-9_ -]{1,60}', edge['name'])
    if name:
        EdgeName = name.group(0)
    else:
        EdgeName = 'Invalid'
    if (edge['activationState'] == 'ACTIVATED'):
        last_contact = datetime.strptime(re.split('T| ', edge["lastContact"])[0], '%Y-%m-%d')
        activated_time = datetime.strptime(re.split('T| ', edge["activationTime"])[0], '%Y-%m-%d')
        activated_days = (last_contact - activated_time).days
    else:
        activated_days = 0
        activated_time = None

    sql_inserts.mysql_PowerBI_EDGE_UPDATE_BASIC_ATTRIBUTES(mysql_handle, mysql_cursor, customer_ID, edge, vco,
                                                           VCO_CUSTOMER_EDGE, Profile_ID, Activation_Status,
                                                           Certificate, Version, EdgeName, Edge_Status, Model,
                                                           activated_time, activated_days, serial, ha_serial,
                                                           street_address)


def update_non_segment_firewall(mysql_cursor, mysql_handle, customer_ID, edge, vco, VCO_CUSTOMER_EDGE):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.INFO)

    Firewall_Edge_Specific = False
    Firewall_rules_in_bool = False
    Firewall_rules_out_bool = False
    Firewall_rules_num = 0

    for config in edge["configuration"]["enterprise"]["modules"]:
        if config["name"] == "firewall":
            Firewall_Edge_Specific = config["isEdgeSpecific"]
            config_firewall = config

    if Firewall_Edge_Specific:
        try:
            for rule in config_firewall["edgeSpecificData"]["inbound"]:
                try:
                    rule["name"]
                    Firewall_rules_in_bool = True
                    Firewall_rules_num = Firewall_rules_num + 1
                except KeyError:
                    pass
                except Exception as e:
                    logger.critical(f'error in update_firewall njRLXiGeRu4HGzfI4Z1U')
                    log_critical_error(ex=e, log_name=VCO_CUSTOMER_EDGE)
            for rule in config_firewall["edgeSpecificData"]["outbound"]:
                try:
                    rule["name"]
                    Firewall_rules_out_bool = True
                    Firewall_rules_num = Firewall_rules_num + 1
                except KeyError:
                    pass
                except Exception as e:
                    logger.critical(f'error in update_firewall 03KFgKe1WXKsP4wtMG0G')
                    log_critical_error(ex=e, log_name=VCO_CUSTOMER_EDGE)
        except KeyError:
            pass
        except Exception as e:
            logger.critical(f'error in update_firewall SlRpWA291Hdku1nbS4dG')
            log_critical_error(ex=e, log_name=VCO_CUSTOMER_EDGE)
            pass

    if 'R2' in edge["buildNumber"]:
        sql_inserts.mysql_PowerBI_EDGE_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer_ID, edge, vco,
                                                                VCO_CUSTOMER_EDGE, "Firewall_Edge_Specific",
                                                                Firewall_Edge_Specific)
        sql_inserts.mysql_PowerBI_EDGE_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer_ID, edge, vco,
                                                                VCO_CUSTOMER_EDGE, "Firewall_rules_num",
                                                                Firewall_rules_num)
        sql_inserts.mysql_PowerBI_EDGE_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer_ID, edge, vco,
                                                                VCO_CUSTOMER_EDGE, "Firewall_rules_out_bool",
                                                                Firewall_rules_out_bool)
        sql_inserts.mysql_PowerBI_EDGE_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer_ID, edge, vco,
                                                                VCO_CUSTOMER_EDGE, "Firewall_rules_in_bool",
                                                                Firewall_rules_in_bool)
        sql_upserts.upsert_edge_attribute(curs=mysql_cursor, sql_cnx=mysql_handle, edge_id=edge['logicalId'],
                                          name='firewall_edge_specific', used=Firewall_Edge_Specific,
                                          log_name=VCO_CUSTOMER_EDGE)
        sql_upserts.upsert_edge_attribute(curs=mysql_cursor, sql_cnx=mysql_handle, edge_id=edge['logicalId'],
                                          name='firewall_rules', num=Firewall_rules_num, log_name=VCO_CUSTOMER_EDGE)
        sql_upserts.upsert_edge_attribute(curs=mysql_cursor, sql_cnx=mysql_handle, edge_id=edge['logicalId'],
                                          name='firewall_rules_out', used=Firewall_rules_out_bool,
                                          log_name=VCO_CUSTOMER_EDGE)
        sql_upserts.upsert_edge_attribute(curs=mysql_cursor, sql_cnx=mysql_handle, edge_id=edge['logicalId'],
                                          name='firewall_rules_in', used=Firewall_rules_in_bool,
                                          log_name=VCO_CUSTOMER_EDGE)


def update_routing(mysql_cursor, mysql_handle, customer_ID, edge, vco, VCO_CUSTOMER_EDGE):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.INFO)

    static_routes_bool = False
    static_routes_num = 0
    bgp_bool = False
    ospf_bool = False
    Multicast_bool = False
    netflow_bool = False

    # logger.info(json.dumps(edge["configuration"], indent=4, sort_keys=True))
    Device_Settings_Edge_Specific = False
    config_device_settings = {}
    for config in edge["configuration"]["enterprise"]["modules"]:
        if config["name"] == "deviceSettings":
            Device_Settings_Edge_Specific = config["isEdgeSpecific"]
            config_device_settings = config

    if Device_Settings_Edge_Specific:
        try:
            for route in config_device_settings["edgeSpecificData"]["routes"]["static"]:
                static_routes_bool = True
                static_routes_num = static_routes_num + 1
        except KeyError:
            logger.info("no static routes")
        except Exception as e:
            logger.critical(f'error in update_routing YeShYykH8BEY6okNA1Cb')
            log_critical_error(ex=e, log_name=VCO_CUSTOMER_EDGE)
        try:
            if config_device_settings["edgeSpecificData"]["bgp"]["enabled"] == True:
                bgp_bool = True
        except KeyError:
            logger.info("no bgp ")
        except Exception as e:
            logger.critical(f'error in update_routing BKwqrqu7ySQ810PFIEfa')
            log_critical_error(ex=e, log_name=VCO_CUSTOMER_EDGE)
        try:
            if config_device_settings["edgeSpecificData"]["netflow"]["enabled"] == True:
                netflow_bool = True
        except KeyError:
            logger.info("no netflow ")
        except Exception as e:
            logger.critical(f'error in update_routing Sz72YtIA1ojRFSZ2mqyt')
            log_critical_error(ex=e, log_name=VCO_CUSTOMER_EDGE)
        try:
            for interface in config_device_settings["edgeSpecificData"]["routedInterfaces"]:
                try:
                    if interface["ospf"]["enabled"] == True:
                        ospf_bool = True
                        OSPF_BOOL = True
                except KeyError:
                    pass
                except Exception as e:
                    logger.critical("error in update_routing N1KzIXZbSOvGOXFksccV")
                    log_critical_error(ex=e, log_name=VCO_CUSTOMER_EDGE)
                try:
                    if interface["multicast"]["igmp"]["enabled"] == True:
                        Multicast_bool = True
                    if interface["multicast"]["pim"]["enabled"] == True:
                        Multicast_bool = True
                except KeyError:
                    pass
                except Exception as e:
                    logger.critical(f'error in update_routing MjSckx4ni0dZFKgRKU4E')
                    log_critical_error(ex=e, log_name=VCO_CUSTOMER_EDGE)
        except KeyError:
            logger.info('no edgeSpecificData or no routedInterfaces')
        except Exception as e:
            logger.critical(f'error in update_routing TD0FJ77TrVEdroIPXIMd')
            log_critical_error(ex=e, log_name=VCO_CUSTOMER_EDGE)
            logger.info("#SHOUD NOT GET HERE")

        try:
            # logger.info(json.dumps(config_device_settings, indent=4, sort_keys=True))
            for segment in config_device_settings["edgeSpecificData"]["segments"]:
                try:
                    if segment["bgp"]["enabled"] == True:
                        bgp_bool = True
                except KeyError:
                    logger.info('no bgp')
                    pass
                except Exception as e:
                    logger.critical(f'error in update_routing rdwPLCOV9CNO7jP4yNtG')
                    log_critical_error(ex=e, log_name=VCO_CUSTOMER_EDGE)
                try:
                    if segment["netflow"]["enabled"] == True:
                        netflow_bool = True
                except KeyError:
                    logger.info("no netflow")
                except Exception as e:
                    logger.critical(f'error in update_routing J6RBgM1Zt7yh15C3edZP')
                    log_critical_error(ex=e, log_name=VCO_CUSTOMER_EDGE)
                try:
                    for route in segment["routes"]["static"]:
                        static_routes_bool = True
                        static_routes_num = static_routes_num + 1
                except KeyError:
                    logger.info("no static routes")
                except Exception as e:
                    logger.critical(f'error in update_routing AydEjQ4P2cEkwdcsPJ4h')
                    log_critical_error(ex=e, log_name=VCO_CUSTOMER_EDGE)
        except KeyError:
            logger.info("no segmenets")
        except Exception as e:
            logger.critical(f'error in update_routing jZMufq04mZ7kdKAQenOb')
            log_critical_error(ex=e, log_name=VCO_CUSTOMER_EDGE)

    sql_inserts.mysql_PowerBI_EDGE_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer_ID, edge, vco,
                                                            VCO_CUSTOMER_EDGE, "netflow_bool", netflow_bool)
    sql_inserts.mysql_PowerBI_EDGE_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer_ID, edge, vco,
                                                            VCO_CUSTOMER_EDGE, "static_routes_bool", static_routes_bool)
    sql_inserts.mysql_PowerBI_EDGE_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer_ID, edge, vco,
                                                            VCO_CUSTOMER_EDGE, "static_routes_num", static_routes_num)
    sql_inserts.mysql_PowerBI_EDGE_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer_ID, edge, vco,
                                                            VCO_CUSTOMER_EDGE, "Multicast_bool", Multicast_bool)
    sql_inserts.mysql_PowerBI_EDGE_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer_ID, edge, vco,
                                                            VCO_CUSTOMER_EDGE, "ospf_bool", ospf_bool)
    sql_inserts.mysql_PowerBI_EDGE_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer_ID, edge, vco,
                                                            VCO_CUSTOMER_EDGE, "bgp_bool", bgp_bool)
    sql_upserts.upsert_edge_attribute(curs=mysql_cursor, sql_cnx=mysql_handle, edge_id=edge['logicalId'],
                                      name='netflow', used=netflow_bool, log_name=VCO_CUSTOMER_EDGE)
    sql_upserts.upsert_edge_attribute(curs=mysql_cursor, sql_cnx=mysql_handle, edge_id=edge['logicalId'],
                                      name='static_routes', used=static_routes_bool, num=static_routes_num,
                                      log_name=VCO_CUSTOMER_EDGE)
    sql_upserts.upsert_edge_attribute(curs=mysql_cursor, sql_cnx=mysql_handle, edge_id=edge['logicalId'],
                                      name='multicast', used=Multicast_bool, log_name=VCO_CUSTOMER_EDGE)
    sql_upserts.upsert_edge_attribute(curs=mysql_cursor, sql_cnx=mysql_handle, edge_id=edge['logicalId'], name='ospf',
                                      used=ospf_bool, log_name=VCO_CUSTOMER_EDGE)
    sql_upserts.upsert_edge_attribute(curs=mysql_cursor, sql_cnx=mysql_handle, edge_id=edge['logicalId'], name='bgp',
                                      used=bgp_bool, log_name=VCO_CUSTOMER_EDGE)
    return


def update_qos(mysql_cursor, mysql_handle, customer_ID, edge, vco, VCO_CUSTOMER_EDGE):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.INFO)

    QOS_Edge_Specific = False
    Business_policy_num = 0

    for config in edge["configuration"]["enterprise"]["modules"]:
        if config["name"] == "QOS":
            QOS_Edge_Specific = config["isEdgeSpecific"]
            config_QOS = config

    if QOS_Edge_Specific:
        try:
            for rule in config_QOS["edgeSpecificData"]["rules"]:
                Business_policy_num = Business_policy_num + 1
        except KeyError:
            logger.info("No 2.X Business Policy")
        try:
            for segment in config_QOS["edgeSpecificData"]["segments"]:
                # logger.info(json.dumps(segment, indent=4, sort_keys=True))
                try:
                    Business_policy_num += len(segment["rules"])
                except KeyError:
                    logger.info("No 3.X Business Policy")
                except Exception as e:
                    logger.critical(f'error in update_qos ZpUzztldu2gsPZOo37JY')
                    log_critical_error(ex=e, log_name=VCO_CUSTOMER_EDGE)
        except KeyError:
            logger.info("no segments")
        except Exception as e:
            logger.critical(f'error in update_qos agO7YdppIKy6zACnbbpi')
            log_critical_error(ex=e, log_name=VCO_CUSTOMER_EDGE)
    sql_inserts.mysql_PowerBI_EDGE_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer_ID, edge, vco,
                                                            VCO_CUSTOMER_EDGE, "QOS_Edge_Specific", QOS_Edge_Specific)
    sql_inserts.mysql_PowerBI_EDGE_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer_ID, edge, vco,
                                                            VCO_CUSTOMER_EDGE, "Business_policy_num",
                                                            Business_policy_num)

    if 0 <= Business_policy_num <= 9:
        bp_range = '0-9'
    elif 10 <= Business_policy_num <= 19:
        bp_range = '10-19'
    elif 20 <= Business_policy_num <= 50:
        bp_range = '20-50'
    else:
        bp_range = '<50'

    sql_upserts.upsert_edge_attribute(curs=mysql_cursor, sql_cnx=mysql_handle, edge_id=edge['logicalId'],
                                      name='qos_edge_specific', used=QOS_Edge_Specific, log_name=VCO_CUSTOMER_EDGE)
    sql_upserts.upsert_edge_attribute(curs=mysql_cursor, sql_cnx=mysql_handle, edge_id=edge['logicalId'],
                                      name='business_policies', num=Business_policy_num,
                                      filter_val=f'business_policies-{bp_range}', log_name=VCO_CUSTOMER_EDGE)
    return


def update_ha_and_cluster(mysql_cursor, mysql_handle, customer_ID, edge, vco, VCO_CUSTOMER_EDGE, services=[]):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.INFO)

    for config in edge["configuration"]["enterprise"]["modules"]:
        if config["name"] == "deviceSettings":
            Device_Settings_Edge_Specific = config["isEdgeSpecific"]
            config_device_settings = config

    HA = "NONE"
    Cluster_bool = False

    if edge["haState"] == "UNCONFIGURED":
        HA = "NONE"
    elif edge["haState"] == "PENDING_INIT" or edge["haState"] == "FAILED" or edge["haState"] == "PENDING_DISSOCIATION":
        HA = "ACTIVE_STANDBY_DOWN"
    elif config_device_settings["edgeSpecificData"]["ha"]["enabled"]:
        HA = "ACTIVE_STANDBY_UP"

    try:
        if "segments" in config_device_settings["edgeSpecificData"]:
            for segment in config_device_settings["edgeSpecificData"]["segments"]:
                if segment["vrrp"]["enabled"]:
                    HA = "VRRP"
    except KeyError:
        logger.info("no VRRP")
    except Exception as e:
        logger.critical(f'error in update_ha_and_cluster o1eW56qBIrQ6ll94PrDC')
        log_critical_error(ex=e, log_name=VCO_CUSTOMER_EDGE)

    for service in services:
        if service["type"] == "edgeHubClusterMember":
            # print json.dumps(service, indent=4, sort_keys=True)
            if service["edgeId"] == edge["id"]:
                HA = "CLUSTER"
                Cluster_bool = True

    sql_inserts.mysql_PowerBI_EDGE_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer_ID, edge, vco,
                                                            VCO_CUSTOMER_EDGE, "HA", HA)

    if 'ACTIVE' in HA:
        ha_used = True
    else:
        ha_used = False

    sql_upserts.upsert_edge_attribute(curs=mysql_cursor, sql_cnx=mysql_handle, edge_id=edge['logicalId'], name='ha',
                                      used=ha_used, log_name=VCO_CUSTOMER_EDGE)
    if ha_used:
        if 'DOWN' in HA:
            standby_up = False
        elif 'UP' in HA:
            standby_up = True
        else:
            standby_up = None
        sql_upserts.upsert_edge_attribute(curs=mysql_cursor, sql_cnx=mysql_handle, edge_id=edge['logicalId'],
                                          name='standby_up', used=standby_up, log_name=VCO_CUSTOMER_EDGE)
    if HA == 'VRRP':
        vrrp_used = True
    else:
        vrrp_used = False
    sql_upserts.upsert_edge_attribute(curs=mysql_cursor, sql_cnx=mysql_handle, edge_id=edge['logicalId'], name='vrrp',
                                      used=vrrp_used, log_name=VCO_CUSTOMER_EDGE)
    if HA == 'CLUSTER':
        cluster_used = True
    else:
        cluster_used = False
    sql_upserts.upsert_edge_attribute(curs=mysql_cursor, sql_cnx=mysql_handle, edge_id=edge['logicalId'],
                                      name='cluster', used=cluster_used, log_name=VCO_CUSTOMER_EDGE)
    return


def update_config_specific(mysql_cursor, mysql_handle, customer_ID, edge, vco, VCO_CUSTOMER_EDGE):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.INFO)

    for config in edge["configuration"]["enterprise"]["modules"]:
        if config["name"] == "deviceSettings":
            Device_Settings_Edge_Specific = config["isEdgeSpecific"]
            config_device_settings = config
        if config["name"] == "firewall":
            Firewall_Edge_Specific = config["isEdgeSpecific"]
            config_firewall = config
        if config["name"] == "QOS":
            QOS_Edge_Specific = config["isEdgeSpecific"]
            config_QOS = config
        if config["name"] == "WAN":
            WAN_Edge_Specific = config["isEdgeSpecific"]

    sql_inserts.mysql_PowerBI_EDGE_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer_ID, edge, vco,
                                                            VCO_CUSTOMER_EDGE, "Device_Settings_Edge_Specific",
                                                            Device_Settings_Edge_Specific)
    sql_inserts.mysql_PowerBI_EDGE_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer_ID, edge, vco,
                                                            VCO_CUSTOMER_EDGE, "Firewall_Edge_Specific",
                                                            Firewall_Edge_Specific)
    sql_inserts.mysql_PowerBI_EDGE_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer_ID, edge, vco,
                                                            VCO_CUSTOMER_EDGE, "QOS_Edge_Specific", QOS_Edge_Specific)
    sql_inserts.mysql_PowerBI_EDGE_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer_ID, edge, vco,
                                                            VCO_CUSTOMER_EDGE, "WAN_Edge_Specific", WAN_Edge_Specific)
    sql_upserts.upsert_edge_attribute(curs=mysql_cursor, sql_cnx=mysql_handle, edge_id=edge['logicalId'],
                                      name='device_settings_edge_specific', used=Device_Settings_Edge_Specific,
                                      log_name=VCO_CUSTOMER_EDGE)
    sql_upserts.upsert_edge_attribute(curs=mysql_cursor, sql_cnx=mysql_handle, edge_id=edge['logicalId'],
                                      name='qos_edge_specific', used=QOS_Edge_Specific, log_name=VCO_CUSTOMER_EDGE)
    sql_upserts.upsert_edge_attribute(curs=mysql_cursor, sql_cnx=mysql_handle, edge_id=edge['logicalId'],
                                      name='wan_edge_specific', used=WAN_Edge_Specific, log_name=VCO_CUSTOMER_EDGE)


def update_vco_license(mysql_cursor, mysql_handle, edge, vco_customer_edge):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': vco_customer_edge})
    logger.setLevel(logging.INFO)
    logger.info('##PROCESS LICENSE##')
    # Change to .get in next version
    try:
        if edge['licenses']:
            # logger.info(json.dumps(edge, indent=4, sort_keys=True))
            for license in edge['licenses']:
                logger.info('Found a license')
                license_sku = license['sku']
                license_start = license['start']
                license_end = license['end']
                license_term_months = license['termMonths']
                license_edition = license['edition']
                license_active = license['active']
                license_bandwidth_tier = license['bandwidthTier']
                license_add_ons = ""

                sql_inserts.mysql_PowerBI_LICENSE_VC(mysql_handle=mysql_handle, mysql_cursor=mysql_cursor,
                                                     edge_uuid=edge['logicalId'], vco_customer_edge=vco_customer_edge,
                                                     license_sku=license_sku, license_start=license_start,
                                                     license_end=license_end, license_active=license_active,
                                                     license_term_months=license_term_months, edition=license_edition,
                                                     bandwidth_tier=license_bandwidth_tier, add_ons=license_add_ons)

            logger.info('FOUND LICENSE')
    except KeyError:
        logger.info('no license')
    except Exception as e:
        logger.critical(f'error in license function {sys.exc_info()[0]}')
        log_critical_error(ex=e, log_name=vco_customer_edge)


def update_recent_link_list(mysql_cursor, mysql_handle, customer_ID, edge, vco, VCO_CUSTOMER_EDGE):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.INFO)

    Private_LINKS_num = 0
    MPLS_BOOL = False
    Private_LINKS_bool = False
    Private_LINKS_num = 0
    Public_LINKS_bol = False
    Public_LINKS_BACKUP = 0
    PUBLIC_LINKS_WIRELESS = 0
    Public_LINKS_num = 0

    for link in edge["recentLinks"]:
        # print "#recentLink"
        # print json.dumps(link, indent=4, sort_keys=True)
        if link["lat"] == 37.402866 or link["lat"] == "37.402866":
            Private_LINKS_num = Private_LINKS_num + 1
            Private_LINKS_bool = True
            MPLS_BOOL = 1
        else:

            Public_LINKS_num = Public_LINKS_num + 1
            Public_LINKS_bol = True
            if link["backupState"] != "UNCONFIGURED":
                Public_LINKS_BACKUP = Public_LINKS_BACKUP + 1
                BACKUP_LINK = 1
            if link["networkType"] == "WIRELESS":
                PUBLIC_LINKS_WIRELESS = PUBLIC_LINKS_WIRELESS + 1
                WIRELESS_LINK = 1

    sql_inserts.mysql_PowerBI_EDGE_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer_ID, edge, vco,
                                                            VCO_CUSTOMER_EDGE, "Private_LINKS_num", Private_LINKS_num)
    sql_inserts.mysql_PowerBI_EDGE_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer_ID, edge, vco,
                                                            VCO_CUSTOMER_EDGE, "Private_LINKS_bool", Private_LINKS_bool)
    sql_inserts.mysql_PowerBI_EDGE_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer_ID, edge, vco,
                                                            VCO_CUSTOMER_EDGE, "Public_LINKS_bol", Public_LINKS_bol)
    sql_inserts.mysql_PowerBI_EDGE_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer_ID, edge, vco,
                                                            VCO_CUSTOMER_EDGE, "Private_LINKS_num", Private_LINKS_num)
    sql_inserts.mysql_PowerBI_EDGE_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer_ID, edge, vco,
                                                            VCO_CUSTOMER_EDGE, "Public_LINKS_BACKUP",
                                                            Public_LINKS_BACKUP)
    sql_inserts.mysql_PowerBI_EDGE_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer_ID, edge, vco,
                                                            VCO_CUSTOMER_EDGE, "PUBLIC_LINKS_WIRELESS",
                                                            PUBLIC_LINKS_WIRELESS)
    sql_inserts.mysql_PowerBI_EDGE_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer_ID, edge, vco,
                                                            VCO_CUSTOMER_EDGE, "Public_LINKS_num", Public_LINKS_num)

    sql_upserts.upsert_edge_attribute(curs=mysql_cursor, sql_cnx=mysql_handle, edge_id=edge['logicalId'],
                                      name='private_links', used=Private_LINKS_bool, num=Private_LINKS_num,
                                      log_name=VCO_CUSTOMER_EDGE)
    sql_upserts.upsert_edge_attribute(curs=mysql_cursor, sql_cnx=mysql_handle, edge_id=edge['logicalId'],
                                      name='public_links', used=Public_LINKS_bol, num=Public_LINKS_num,
                                      log_name=VCO_CUSTOMER_EDGE)
    if Public_LINKS_BACKUP > 0:
        backup_links_used = True
    else:
        backup_links_used = False
    sql_upserts.upsert_edge_attribute(curs=mysql_cursor, sql_cnx=mysql_handle, edge_id=edge['logicalId'],
                                      name='backup_links', used=backup_links_used, num=Public_LINKS_BACKUP,
                                      log_name=VCO_CUSTOMER_EDGE)

    if PUBLIC_LINKS_WIRELESS > 0:
        wireless_links_used = True
    else:
        wireless_links_used = False
    sql_upserts.upsert_edge_attribute(curs=mysql_cursor, sql_cnx=mysql_handle, edge_id=edge['logicalId'],
                                      name='public_wireless_links', used=wireless_links_used, num=PUBLIC_LINKS_WIRELESS,
                                      log_name=VCO_CUSTOMER_EDGE)


def update_edge_info_with_basic_information(mysql_cursor, mysql_handle, customer_ID, edge, vco, VCO_CUSTOMER_EDGE):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.INFO)

    update_location_information(mysql_cursor, mysql_handle, customer_ID, edge, vco, VCO_CUSTOMER_EDGE)

    update_attributes(mysql_cursor, mysql_handle, customer_ID, edge, vco, VCO_CUSTOMER_EDGE)


def update_edge_links(mysql_cursor, mysql_handle, customer, edge, vco, VCO_CUSTOMER_EDGE, client, link_metrics,
                      configuration, edge_config_stack):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.INFO)

    for linkd in link_metrics:
        EdgeID = edge["logicalId"]
        LinkName = "Not set"
        try:
            LinkName = linkd["link"]["displayName"]
        except KeyError:
            logger.info('Link Name Not Set')
        except Exception as e:
            logger.critical('error in update_edge_links Etm7GRkGrDt7f2EzrQaE')
            log_critical_error(ex=e, log_name=VCO_CUSTOMER_EDGE)

        LinkUUID = linkd["link"]["internalId"]
        LinkID = EdgeID + '-' + LinkUUID
        # print (LinkID, EdgeID, LinkUUID)
        Interface = linkd["link"]["interface"]
        Latitude = linkd["link"]["lat"]
        Longitude = linkd["link"]["lon"]
        NetworkSide = linkd["link"]["networkSide"]
        Networktype = linkd["link"]["networkType"]
        LinkIpAddress = False
        try:
            LinkIpAddress = linkd["link"]["ipAddress"]
        except KeyError:
            logger.info('Link IP Not Set')
        except Exception as e:
            logger.critical('error in update_edge_links 46G6WoIaEG2DxyyvNxwG')
            log_critical_error(ex=e, log_name=VCO_CUSTOMER_EDGE)

        MTU = "Not set"
        ISP = "Not set"
        OverlayType = "Not set"
        Linktype = "Not set"
        LinkMode = "Not set"
        VLANID = "Not set"
        for configs in edge_config_stack:
            if configs["name"] == "Edge Specific Profile":
                for modules in configs["modules"]:
                    if modules["name"] == "WAN":
                        if "links" in modules["data"].keys():
                            for link in modules["data"]["links"]:
                                if link["internalId"] == linkd["link"]["internalId"]:
                                    MTU = link["MTU"]
                                    ISP = link["isp"]
                                    OverlayType = link["discovery"]
                                    Linktype = link["type"]
                                    LinkMode = link["mode"]
                                    if LinkMode == 'Private':
                                        ISP = 'MPLS'
                                    VLANID = link["vlanId"]
        # print EdgeID, LinkUUID, LinkName, ISP, Interface, Latitude, Longitude, NetworkSide, Networktype, MTU, OverlayType, Linktype, LinkMode, VLANID
        sql_inserts.mysql_PowerBI_EDGE_INSERT_LINK(mysql_handle, mysql_cursor, customer["logicalId"], edge, vco,
                                                   VCO_CUSTOMER_EDGE, LinkUUID, LinkName, ISP, Interface, Latitude,
                                                   Longitude, NetworkSide, Networktype, LinkIpAddress, MTU, OverlayType,
                                                   Linktype, LinkMode, VLANID)


def update_edge_overlay_link(mysql_cursor, mysql_handle, customer, edge, vco, VCO_CUSTOMER_EDGE, client):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.INFO)
    EdgeID = edge["logicalId"]
    LinkUUID = edge["logicalId"] + '-' + "OVERLAY"
    Networktype = 'OVERLAY'
    Linktype = 'OVERLAY'
    LinkName = 'OVERLAY'
    ISP = 'VeloCloud'
    Interface = 'null0'
    NetworkSide = 'OVERLAY'
    OverlayType = 'OVERLAY_DEFINED'
    Latitude = 0
    Longitude = 0
    MTU = 0
    VLANID = 0
    LinkMode = 'OVERLAY'
    LinkIpAddress = '0.0.0.0'

    # Some values are not string this info generates issues
    # logger.info(EdgeID, LinkUUID, LinkName, ISP, Interface, Latitude, Longitude, NetworkSide, Networktype, LinkIpAddress, MTU, OverlayType, Linktype, LinkMode, VLANID)
    sql_inserts.mysql_PowerBI_EDGE_INSERT_LINK(mysql_handle, mysql_cursor, customer["logicalId"], edge, vco,
                                               VCO_CUSTOMER_EDGE, LinkUUID, LinkName, ISP, Interface, Latitude,
                                               Longitude, NetworkSide, Networktype, LinkIpAddress, MTU, OverlayType,
                                               Linktype, LinkMode, VLANID)


def datetime_to_epoch_ms(dtm):
    return int(dtm.timestamp()) * 1000


def converttohuman(tdm):
    s = tdm / 1000
    return time.strftime("%Y-%m-%d  %H:%M:%S", time.localtime(s))


def CalculateBrownouts(index_qoe_state, qoe_list, VCO_CUSTOMER_EDGE):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.INFO)
    # iterate over the alist and  checkif if the link changes from 4 to 2
    # Increase the brownout  and timer by 1 if the  qlist change from 4 to 2
    # If the the link stays 2 consecutively then the timer keeps  increasing
    Brownout = 0
    Timer = 0
    length = len(qoe_list) - 1
    logger.info("Calculating brownouts and duration of the links")
    for qoe_index in index_qoe_state:
        # Check if n+1 item exists
        if qoe_index + 1 <= length:
            if qoe_list[qoe_index + 1] == 2:
                Brownout = Brownout + 1
                Timer = Timer + 1
                try:
                    # Check if n+2 item exists
                    if qoe_index + 2 <= length:
                        if qoe_list[qoe_index + 2] == 2:
                            i = qoe_index + 2
                            ran = range(i, len(qoe_list))
                            for t in ran:
                                if qoe_list[t] == 2:
                                    Timer = Timer + 1
                                else:
                                    break
                except IndexError:
                    logger.error("Index out of range")
                except Exception as e:
                    logger.critical(f'error in CalculateBrownouts f4ooVnH7Vgs7CmSSOBot {e}')
                    log_critical_error(ex=e, log_name=VCO_CUSTOMER_EDGE)
    return Brownout, Timer


def CalculateBlackouts(index_qoe_state, qoe_list, VCO_CUSTOMER_EDGE):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.INFO)
    # iterate over the alist and  checkif if the link changes from 4 to 0, 3 to 0 and 2 to  0
    # Increase the blackout  and timer by 1 if the  qlist change from the current state to 0
    # If the the link stays 0 consecutively then the timer keeps  increasing
    Blackout = 0
    Timer = 0
    length = len(qoe_list) - 1
    logger.info("Calculating blackouts and duration of the links")
    for qoe_index in index_qoe_state:
        # Check if n+1 item exists
        if qoe_index + 1 <= length:
            if qoe_list[qoe_index + 1] == 0:
                Blackout = Blackout + 1
                Timer = Timer + 1
                try:
                    # Check if n+2 item exists
                    if qoe_index + 2 <= length:
                        if qoe_list[qoe_index + 2] == 0:
                            i = qoe_index + 2
                            ran = range(i, len(qoe_list))
                            for t in ran:
                                if qoe_list[t] == 0:
                                    Timer = Timer + 1
                                else:
                                    break
                except Exception as e:
                    logger.critical('error in CalculateBlackouts sGhqWo6mvfmu1mEboT7p')
                    log_critical_error(ex=e, log_name=VCO_CUSTOMER_EDGE)
                    logger.error("Index out of range")
    return (Blackout, Timer)


def lowest_qoe(VCO_CUSTOMER_EDGE: str, arr: List, size: int):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.INFO)
    arrs = []
    # Split the array into sub sequence of sample 8 is considered as an hour data
    while len(arr) > size:
        pice = arr[:size]
        arrs.append(pice)
        arr = arr[size:]
    arrs.append(arr)
    lowest_qoe = []
    noof4s = noof3s = 0
    for lowest in arrs:
        # Count the the number of times green and yellow quality occurs
        noof4s = [i for i, x in enumerate(lowest) if x == 4]
        noof3s = [i for i, x in enumerate(lowest) if x == 3]
        # print (len(noof4s),len(noof3s))
        score = (len(noof4s) * 10 + len(noof3s) * 5) / float(8)
        # print (score)
        lowest_qoe.append(score)
    logger.info("Lowest QOE is calculated")
    logger.info(lowest_qoe)
    if lowest_qoe:
        min_qoe = min(lowest_qoe)
    else:
        min_qoe = None
    return min_qoe


def calculate_edge_link_qoe(mysql_cursor, mysql_handle, customer, edge, vco, VCO_CUSTOMER_EDGE, client, qoe_metrics,
                            STOP, START):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.INFO)

    LinkBrownouts = LinkBrownoutDurations = LinkBlackoutDurations = LinkBlackoutDuration = LinkBlackouts = LinkBrownoutDuration = 0
    for links in qoe_metrics:
        id_ = links
        EdgeID = edge["logicalId"]
        if links == "overallLinkQuality":
            LinkUUID = EdgeID + '-' + "OVERLAY"
            state = "after"
        else:
            LinkUUID = id_
            state = "before"
        # LinkID = EdgeID + '-' + LinkUUID
        Date = START.strftime('%Y-%m-%d 00:00:00')
        Score = None
        try:
            Score = qoe_metrics[id_]["totalScore"]
        except KeyError:
            logger.error("Score Value not found")
        except Exception as e:
            logger.critical(f'error in calculate_edge_link_qoe nxcGNQE547fncnoInuXJ {e}')
            log_critical_error(ex=e, log_name=VCO_CUSTOMER_EDGE)
        voice_qal = []
        logger.info("Calculating QOE Link Quality and duration of the links")
        # logger.info("Link Quality Value")
        # Calculate the Before data from the API call
        print(EdgeID, LinkUUID, state)
        for x in range(0, 200):
            try:
                before_data = qoe_metrics[links]["timeseries"][x][state]["0"]
                voice_qal.append(before_data)
            except (KeyError, IndexError):
                logger.info("Link Quality Value Not Available")  # print ("Link Quality Value Not available")
            except Exception as e:
                logger.critical(f'error in calculate_edge_link_qoe 3MqgPzUk28MOg3HLW8Jh {e}')
                log_critical_error(ex=e, log_name=VCO_CUSTOMER_EDGE)
        logger.info(voice_qal)
        size = 8
        # Divide the 24 hours sample into 200/8 = 24 hour sample
        # minimum sample value
        lowest_linkscore = lowest_qoe(VCO_CUSTOMER_EDGE=VCO_CUSTOMER_EDGE, arr=voice_qal, size=size)
        noof2s = [i for i, x in enumerate(voice_qal) if x == 2]
        noof3s = [i for i, x in enumerate(voice_qal) if x == 3]
        noof4s = [i for i, x in enumerate(voice_qal) if x == 4]
        # Calculate the number of 4s,3s 2s,0s
        if noof3s:
            val = 3
            val3 = CalculateBlackouts(noof3s, voice_qal, VCO_CUSTOMER_EDGE)  # print (val3)
        else:
            val3 = [0, 0]
        if noof2s:
            val2 = 0
            val2 = CalculateBlackouts(noof2s, voice_qal, VCO_CUSTOMER_EDGE)  # print (val2)
        else:
            val2 = [0, 0]
        if noof4s:
            val4 = 0
            val5 = 0
            val4 = CalculateBlackouts(noof4s, voice_qal, VCO_CUSTOMER_EDGE)
            val5 = CalculateBrownouts(noof4s, voice_qal, VCO_CUSTOMER_EDGE)  # print (val5)
        else:
            val4 = [0, 0]
            val5 = [0, 0]
        LinkBrownouts = val5[0]
        LinkBrownoutDurations = val5[1]
        LinkBrownoutDuration = round((LinkBrownoutDurations * 7.12) / 60, 3)
        if LinkBrownoutDuration == 0.0:
            LinkBrownoutDuration = 0
        LinkBlackouts = val3[0] + val2[0] + val4[0]
        LinkBlackoutDurations = val3[1] + val2[1] + val4[1]
        LinkBlackoutDuration = round((LinkBlackoutDurations * 7.12) / 60, 3)
        if LinkBlackoutDuration == 0.0:
            LinkBlackoutDuration = 0
        print(LinkBlackouts, LinkBlackoutDuration, LinkBrownouts, LinkBrownoutDurations, LinkBrownoutDuration)
        # print (EdgeID, LinkUUID, Score,lowest_linkscore, LinkBlackouts, LinkBlackoutDuration,LinkBrownouts, LinkBrownoutDuration, LinkID)
        sql_inserts.mysql_PowerBI_EDGE_INSERT_QOE(mysql_handle, mysql_cursor, edge, vco, VCO_CUSTOMER_EDGE, Date,
                                                  EdgeID, LinkUUID, Score, lowest_linkscore, LinkBlackouts,
                                                  LinkBlackoutDuration, LinkBrownouts, LinkBrownoutDuration)


def update_edge_qoe(mysql_cursor, mysql_handle, customer, edge, vco, VCO_CUSTOMER_EDGE, client):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.INFO)

    current_time = datetime.now()
    for i in range(0, 30):
        STOP = current_time - timedelta(days=i)
        # print (STOP.strftime('%Y-%m-%d 00:00:00'))
        START = STOP - timedelta(hours=24)
        # print (START.strftime('%Y-%m-%d 00:00:00'))
        EdgeID = edge["logicalId"]
        Lastupdate = START.strftime('%Y-%m-%d 00:00:00')
        # print (STOP.strftime('%Y-%m-%dT00:00:00.000000'), START.strftime('%Y-%m-%dT00:00:00.000000'))
        # CHECK FOR LINK QOE UPDATE
        if sql_queries.determine_if_link_qoe_needs_update(mysql_cursor, mysql_handle, Lastupdate, EdgeID,
                                                          VCO_CUSTOMER_EDGE):
            logger.info("QOE Link Quality Information")
            sleep(0.5)
            params = {"enterpriseId": customer["id"], "edgeId": edge["id"], "maxSamples": 200,
                      "interval": {"start": START.strftime('%Y-%m-%dT00:00:00.000000'),
                                   "end": STOP.strftime('%Y-%m-%dT00:00:00.000000')}}
            kwargs = {"timeout": 300}
            logger.info(params)
            # logger.info("Calculating blackouts and duration of the links")
            logger.info("START.strftime('%Y-%m-%d 00:00:00')")
            qoe_metrics = client.call_api('/linkQualityEvent/getLinkQualityEvents', params, **kwargs)
            if qoe_metrics:
                calculate_edge_link_qoe(mysql_cursor, mysql_handle, customer, edge, vco, VCO_CUSTOMER_EDGE, client,
                                        qoe_metrics, STOP, START)
            else:
                logger.info(
                    "QOE Metric is not available for the date")  # else:  #   logger.info("QOE Update is not required")


def update_license_and_link_usage(mysql_cursor, mysql_handle, customer, edge, vco, VCO_CUSTOMER_EDGE, client,
                                  link_metrics, link_series, configuration, edge_config_stack):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.INFO)

    linkn = 0
    Score = 0
    lic_bandwidth = 0
    UPLINK_USAGE = 0
    DOWNLINK_USAGE = 0
    Total_TX_Usage = 0
    Total_RX_Usage = 0
    Total_RX_Bandwidth = 0
    Total_TX_Bandwidth = 0
    date_now = datetime.utcnow()
    Total_BW_List = []
    top_bandwidth_in_mbps = 0
    fifth_top_throughput = 0
    tenth_top_throughput = 0
    feature_set = "Enterprise Subscription"
    b2b_via_gw = False
    b2b_via_hub = False
    pb_via_gw = False
    css_via_gw = False
    nvs_via_gw = False
    pb_internet_via_hub = False
    pb_internet_via_direct = False

    ### Start processing Top Average throughput utilized by Edge #######

    if len(link_series) != 0:

        Sample_Interval = link_series[0]['series'][0]['tickInterval'] / 1000
        for link in link_series:
            bytesRX = link['series'][0]['data']
            bytesRX_list = [0 if v is None else v for v in bytesRX]
            bytesTX = link['series'][1]['data']
            bytesTX_list = [0 if v is None else v for v in bytesTX]
            Link_BW = [x + y for x, y in zip(bytesTX_list, bytesRX_list)]

            if len(Total_BW_List) == 0:
                Total_BW_List = Link_BW

            else:
                Total_BW_List = [x + y for x, y in zip(Total_BW_List, Link_BW)]

        Total_BW_List.sort()

        highest_bw_Bytes = Total_BW_List[-1]
        fifth_highest_bw_Bytes = Total_BW_List[-5]
        tenth_highest_bw_Bytes = Total_BW_List[-10]

        top_bandwidth_in_mbps = round((highest_bw_Bytes * 8) / (Sample_Interval * 1000 * 1000), 3)
        fifth_top_throughput = round((fifth_highest_bw_Bytes * 8) / (Sample_Interval * 1000 * 1000), 3)
        tenth_top_throughput = round((tenth_highest_bw_Bytes * 8) / (Sample_Interval * 1000 * 1000), 3)

    #### Start Processing actual Feature Set used by customer..applied initially for all VCO including on-prem for visibility###

    if edge_config_stack[0]['schemaVersion'] != "2.0.0":
        for configs in edge_config_stack:
            if configs["name"] == "Edge Specific Profile":
                for modules in configs["modules"]:
                    if modules["name"] == "controlPlane":
                        try:
                            if (modules["data"]["segments"][0]['vpn']['enabled'] == True) and (
                                    modules["data"]["segments"][0]['vpn']['edgeToEdge']) == True:
                                if modules["data"]["segments"][0]['vpn']['edgeToEdgeDetail']['useCloudGateway'] == True:
                                    b2b_via_gw = True
                                else:
                                    b2b_via_hub = True
                        except KeyError:
                            logger.info('failed to get Cloud VPN status from Edge or not enabled')

        pb_via_gw, pb_internet_via_direct, pb_internet_via_hub, css_via_gw, nvs_via_gw = process_segment_pb(
            edge_config_stack, 0)

        if pb_via_gw == css_via_gw == nvs_via_gw == False:
            pb_via_gw, pb_internet_via_direct, pb_internet_via_hub, css_via_gw, nvs_via_gw = process_segment_pb(
                edge_config_stack, 1)

    else:
        for configs in edge_config_stack:
            if configs['name'] == 'Edge Specific Profile':
                for modules in configs["modules"]:
                    if modules['name'] == 'controlPlane':
                        if 'vpn' in modules["data"].keys():
                            if modules["data"]['vpn']['edgeToEdge'] == True:
                                if modules["data"]['vpn']['edgeToEdgeDetail']['useCloudGateway'] == True:
                                    b2b_via_gw = True
                                else:
                                    b2b_via_hub = True

        pb_via_gw, pb_internet_via_direct, pb_internet_via_hub, css_via_gw, nvs_via_gw = process_nonsegment_pb(
            edge_config_stack, 0)

        if pb_via_gw == css_via_gw == nvs_via_gw == False:
            pb_via_gw, pb_internet_via_direct, pb_internet_via_hub, css_via_gw, nvs_via_gw = process_nonsegment_pb(
                edge_config_stack, 1)

    if b2b_via_gw:
        feature_set = 'Premium Subscription'
    elif pb_via_gw:
        feature_set = 'Premium Subscription'
    elif css_via_gw:
        feature_set = 'Premium Subscription'
    elif nvs_via_gw:
        feature_set = 'Premium Subscription'
    else:
        feature_set = "Enterprise Subscription"

    for link in link_metrics:
        if "scoreTx" in link.keys():
            sleep(0.5)  # print "there was a score"  # sleep(5)
        else:
            # print "no secure was define"
            link["scoreTx"] = 1
            link["scoreRx"] = 1  # WE NEED TO INVESTIGATE THIS

        if "link" in link.keys():
            if link["link"]["edgeId"] == edge["id"] and link["scoreTx"] != 0 and link["link"][
                "backupState"] == "UNCONFIGURED":
                # print "#linkMetric"
                # print json.dumps(link, indent=4, sort_keys=True)
                # sleep(50)
                Score = ((Score * linkn + (float(link["scoreTx"]) + float(link["scoreRx"]))) * 12.5) / (linkn + 1)
                linkn += 1
                # print Score
                # print float(link["scoreTx"])
                # print float(link["scoreRx"])
                TX_Bandwidth = link["bpsOfBestPathTx"]
                RX_Bandwidth = link["bpsOfBestPathRx"]
                RX_Usage = link["bytesRx"]
                TX_Usage = link["bytesTx"]

                Total_TX_Bandwidth += TX_Bandwidth
                Total_RX_Bandwidth += RX_Bandwidth
                Total_TX_Usage += TX_Usage
                Total_RX_Usage += RX_Usage

    if Total_RX_Bandwidth > 0:
        DOWNLINK_USAGE = ((float(Total_RX_Usage * 8)) / float(((Total_RX_Bandwidth * 60 * 60 * 8 * 5)))) * 100
        if DOWNLINK_USAGE > 100:
            DOWNLINK_USAGE = 100
    if Total_TX_Bandwidth > 0:
        UPLINK_USAGE = ((float(Total_TX_Usage * 8)) / float(((Total_TX_Bandwidth * 60 * 60 * 8 * 5)))) * 100
        if UPLINK_USAGE > 100:
            UPLINK_USAGE = 100

    Bandwidth = int((Total_TX_Bandwidth + Total_RX_Bandwidth) / 1000000)
    lic_bandwidth = int(top_bandwidth_in_mbps)

    if (lic_bandwidth <= 30 and edge["modelNumber"]):
        License = edge["modelNumber"] + "_30M"
    if (lic_bandwidth <= 50 and lic_bandwidth > 30):
        License = edge["modelNumber"] + "_50M"
    if (lic_bandwidth <= 100 and lic_bandwidth > 50):
        License = edge["modelNumber"] + "_100M"
    if (lic_bandwidth <= 200 and lic_bandwidth > 100):
        License = edge["modelNumber"] + "_200M"
    if (lic_bandwidth <= 400 and lic_bandwidth > 200):
        License = edge["modelNumber"] + "_400M"
    if (lic_bandwidth <= 1000 and lic_bandwidth > 400):
        License = edge["modelNumber"] + "_1G"
    if (lic_bandwidth > 1000):
        License = edge["modelNumber"] + "_5G"

    if (lic_bandwidth > 200 and edge["edgeState"] == "CONNECTED" and (
            "edge520" in edge["modelNumber"] or "edge510" in edge["modelNumber"] or "edge500" in edge["modelNumber"])):

        Date = date_now.strftime('%Y-%m-01T00:00:00.000Z')[:-3]
        if determine_if_edge_is_hub(configuration, edge, VCO_CUSTOMER_EDGE):
            Name = "OVERCAPACITY_HUB " + edge["modelNumber"] + " over 200"
            Type = "BADCONFIG"
            sql_inserts.mysql_PowerBI_EDGE_INSERT_EVENT(mysql_handle, mysql_cursor, customer["logicalId"], edge, vco,
                                                        VCO_CUSTOMER_EDGE, Date, Name, Type)
        elif DOWNLINK_USAGE > 5 or UPLINK_USAGE > 5:
            Name = "OVERCAPACITY_HIGHUSAGE " + edge["modelNumber"] + " over 200"
            Type = "BADCONFIG"
            sql_inserts.mysql_PowerBI_EDGE_INSERT_EVENT(mysql_handle, mysql_cursor, customer["logicalId"], edge, vco,
                                                        VCO_CUSTOMER_EDGE, Date, Name, Type)

    if (lic_bandwidth > 350 and edge["edgeState"] == "CONNECTED" and "edge610" in edge["modelNumber"]):
        # print("we found an edge is overcapacity")
        sleep(1)
        Type = "BADCONFIG"
        Date = date_now.strftime('%Y-%m-01T00:00:00.000Z')[:-3]
        if determine_if_edge_is_hub(configuration, edge, VCO_CUSTOMER_EDGE):
            Name = "OVERCAPACITY_HUB " + edge["modelNumber"] + " over 350"
            Type = "BADCONFIG"
            sql_inserts.mysql_PowerBI_EDGE_INSERT_EVENT(mysql_handle, mysql_cursor, customer["logicalId"], edge, vco,
                                                        VCO_CUSTOMER_EDGE, Date, Name, Type)
        elif DOWNLINK_USAGE > 5 or UPLINK_USAGE > 5:
            Name = "OVERCAPACITY_HIGHUSAGE " + edge["modelNumber"] + " over 350"
            Type = "BADCONFIG"
            sql_inserts.mysql_PowerBI_EDGE_INSERT_EVENT(mysql_handle, mysql_cursor, customer["logicalId"], edge, vco,
                                                        VCO_CUSTOMER_EDGE, Date, Name, Type)

    if (lic_bandwidth > 1000 and edge["edgeState"] == "CONNECTED" and "edge540" in edge["modelNumber"]):
        # print("we found an edge is overcapacity")
        sleep(1)
        Type = "BADCONFIG"
        Date = date_now.strftime('%Y-%m-01T00:00:00.000Z')[:-3]
        if determine_if_edge_is_hub(configuration, edge, VCO_CUSTOMER_EDGE):
            Name = "OVERCAPACITY_HUB " + edge["modelNumber"] + " over 1000"
            Type = "BADCONFIG"
            sql_inserts.mysql_PowerBI_EDGE_INSERT_EVENT(mysql_handle, mysql_cursor, customer["logicalId"], edge, vco,
                                                        VCO_CUSTOMER_EDGE, Date, Name, Type)
        elif DOWNLINK_USAGE > 5 or UPLINK_USAGE > 5:
            Name = "OVERCAPACITY_HIGHUSAGE " + edge["modelNumber"] + " over 1000"
            Type = "BADCONFIG"
            sql_inserts.mysql_PowerBI_EDGE_INSERT_EVENT(mysql_handle, mysql_cursor, customer["logicalId"], edge, vco,
                                                        VCO_CUSTOMER_EDGE, Date, Name, Type)

    if (lic_bandwidth > 1500 and edge["edgeState"] == "CONNECTED" and "edge620" in edge["modelNumber"]):
        # print("we found an edge is overcapacity")
        sleep(1)
        Type = "BADCONFIG"
        Date = date_now.strftime('%Y-%m-01T00:00:00.000Z')[:-3]
        if determine_if_edge_is_hub(configuration, edge, VCO_CUSTOMER_EDGE):
            Name = "OVERCAPACITY_HUB " + edge["modelNumber"] + " over 1500"
            Type = "BADCONFIG"
            sql_inserts.mysql_PowerBI_EDGE_INSERT_EVENT(mysql_handle, mysql_cursor, customer["logicalId"], edge, vco,
                                                        VCO_CUSTOMER_EDGE, Date, Name, Type)
        elif DOWNLINK_USAGE > 5 or UPLINK_USAGE > 5:
            Name = "OVERCAPACITY_HIGHUSAGE " + edge["modelNumber"] + " over 1500"
            Type = "BADCONFIG"
            sql_inserts.mysql_PowerBI_EDGE_INSERT_EVENT(mysql_handle, mysql_cursor, customer["logicalId"], edge, vco,
                                                        VCO_CUSTOMER_EDGE, Date, Name, Type)

    if (lic_bandwidth > 2000 and edge["edgeState"] == "CONNECTED" and "edge840" in edge["modelNumber"]):
        # print("we gound an edge is overcapacity")
        sleep(1)
        Type = "BADCONFIG"
        Date = date_now.strftime('%Y-%m-01T00:00:00.000Z')[:-3]
        if determine_if_edge_is_hub(configuration, edge, VCO_CUSTOMER_EDGE):
            Name = "OVERCAPACITY_HUB " + edge["modelNumber"] + " over 2000"
            sql_inserts.mysql_PowerBI_EDGE_INSERT_EVENT(mysql_handle, mysql_cursor, customer["logicalId"], edge, vco,
                                                        VCO_CUSTOMER_EDGE, Date, Name, Type)
        elif DOWNLINK_USAGE > 5 or UPLINK_USAGE > 5:
            Name = "OVERCAPACITY_HIGHUSAGE " + edge["modelNumber"] + " over 2000"
            sql_inserts.mysql_PowerBI_EDGE_INSERT_EVENT(mysql_handle, mysql_cursor, customer["logicalId"], edge, vco,
                                                        VCO_CUSTOMER_EDGE, Date, Name, Type)

    sql_inserts.mysql_PowerBI_EDGE_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer["logicalId"], edge,
                                                            vco, VCO_CUSTOMER_EDGE, "Bandwidth", Bandwidth)
    sql_inserts.mysql_PowerBI_EDGE_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer["logicalId"], edge,
                                                            vco, VCO_CUSTOMER_EDGE, "License", License)
    sql_inserts.mysql_PowerBI_EDGE_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer["logicalId"], edge,
                                                            vco, VCO_CUSTOMER_EDGE, "DOWNLINK_USAGE", DOWNLINK_USAGE)
    sql_inserts.mysql_PowerBI_EDGE_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer["logicalId"], edge,
                                                            vco, VCO_CUSTOMER_EDGE, "UPLINK_USAGE", UPLINK_USAGE)
    sql_inserts.mysql_PowerBI_EDGE_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer["logicalId"], edge,
                                                            vco, VCO_CUSTOMER_EDGE, "Score", UPLINK_USAGE)

    sql_inserts.mysql_PowerBI_LICENSE_UPDATE_USED_BANDWIDTH_FEATURES(mysql_handle, mysql_cursor, edge, vco,
                                                                     VCO_CUSTOMER_EDGE, top_bandwidth_in_mbps,
                                                                     fifth_top_throughput, tenth_top_throughput,
                                                                     feature_set, b2b_via_gw, pb_via_gw, css_via_gw,
                                                                     nvs_via_gw, b2b_via_hub, pb_internet_via_direct,
                                                                     pb_internet_via_hub)

    # TODO: Filter Values
    sql_upserts.upsert_edge_attribute(curs=mysql_cursor, sql_cnx=mysql_handle, edge_id=edge['logicalId'],
                                      name='configured_bandwidth', num=int(Bandwidth), log_name=VCO_CUSTOMER_EDGE)
    sql_upserts.upsert_edge_attribute(curs=mysql_cursor, sql_cnx=mysql_handle, edge_id=edge['logicalId'],
                                      name='calculated_license', text=License, log_name=VCO_CUSTOMER_EDGE)
    sql_upserts.upsert_edge_attribute(curs=mysql_cursor, sql_cnx=mysql_handle, edge_id=edge['logicalId'],
                                      name='downlink_usage', num=int(DOWNLINK_USAGE), log_name=VCO_CUSTOMER_EDGE)
    sql_upserts.upsert_edge_attribute(curs=mysql_cursor, sql_cnx=mysql_handle, edge_id=edge['logicalId'],
                                      name='uplink_usage', num=int(UPLINK_USAGE), log_name=VCO_CUSTOMER_EDGE)


def process_segment_pb(edge_config_stack, profile_level):
    pb_via_gw = False
    pb_internet_via_direct = False
    pb_internet_via_hub = False
    css_via_gw = False
    nvs_via_gw = False

    for modules in edge_config_stack[profile_level]["modules"]:
        if modules["name"] == "QOS":
            if len(modules["data"]['segments']) != 0:
                for rule in modules["data"]['segments'][0]['rules']:
                    if (rule['action']['routeType'] == 'edge2Cloud' and rule['action']['edge2CloudRouteAction'][
                        'routePolicy'] == 'gateway'):
                        pb_via_gw = True

                    elif (rule['action']['routeType'] == 'edge2Cloud' and rule['action']['edge2CloudRouteAction'][
                        'routePolicy'] == 'direct'):
                        pb_internet_via_direct = True

                    elif (rule['action']['routeType'] == 'edge2Cloud' and
                          rule['action']['edge2CloudRouteAction']['routeCfg']['type'] == 'edge'):
                        pb_internet_via_hub = True

                    elif (rule['action']['routeType'] == 'edge2Cloud' and
                          rule['action']['edge2CloudRouteAction']['routeCfg']['type'] == 'cloudSecurityService'):
                        css_via_gw = True

                    elif (rule['action']['routeType'] == 'edge2Cloud' and
                          rule['action']['edge2CloudRouteAction']['routeCfg']['type'] == 'dataCenter'):
                        nvs_via_gw = True

    return (pb_via_gw, pb_internet_via_direct, pb_internet_via_hub, css_via_gw, nvs_via_gw)


def process_nonsegment_pb(edge_config_stack, profile_level):
    pb_via_gw = False
    pb_internet_via_direct = False
    pb_internet_via_hub = False
    css_via_gw = False
    nvs_via_gw = False

    for modules in edge_config_stack[profile_level]["modules"]:
        if modules["name"] == "QOS":
            if len(modules["data"]) != 0:
                for rule in modules["data"]['rules']:
                    if (rule['action']['routeType'] == 'edge2Cloud' and rule['action']['edge2CloudRouteAction'][
                        'routePolicy'] == 'gateway'):
                        pb_via_gw = True

                    elif (rule['action']['routeType'] == 'edge2Cloud' and rule['action']['edge2CloudRouteAction'][
                        'routePolicy'] == 'direct'):
                        pb_internet_via_direct = True

                    elif (rule['action']['routeType'] == 'edge2Cloud' and
                          rule['action']['edge2CloudRouteAction']['routeCfg']['type'] == 'edge'):
                        pb_internet_via_hub = True

                    elif (rule['action']['routeType'] == 'edge2Cloud' and
                          rule['action']['edge2CloudRouteAction']['routeCfg']['type'] == 'cloudSecurityService'):
                        css_via_gw = True

                    elif (rule['action']['routeType'] == 'edge2Cloud' and
                          rule['action']['edge2CloudRouteAction']['routeCfg']['type'] == 'dataCenter'):
                        nvs_via_gw = True

    return (pb_via_gw, pb_internet_via_direct, pb_internet_via_hub, css_via_gw, nvs_via_gw)


def snmpv3_status(mysql_cursor, mysql_handle, customer, edge, vco, VCO_CUSTOMER_EDGE, edge_config_stack):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.INFO)

    snmpv3_bool = False

    for configs in edge_config_stack:
        if configs['name'] == 'Edge Specific Profile':
            for modules in configs['modules']:
                if modules['name'] == 'deviceSettings':
                    if 'snmp' in modules["data"].keys():
                        try:
                            if modules['data']['snmp']['snmpv3'].get('enabled') == True:
                                snmpv3_bool = True
                                break
                            else:
                                snmpv3_bool = False
                                break
                        except KeyError:
                            logger.error('failed to retrive SNMPv3 from Edge')
                    else:
                        for configs in edge_config_stack:
                            if configs['name'] != 'Edge Specific Profile':
                                for modules in configs['modules']:
                                    if modules['name'] == 'deviceSettings':
                                        if 'snmp' in modules['data'].keys():
                                            if modules['data']['snmp']['snmpv3'].get('enabled') == True:
                                                snmpv3_bool = True
                                                break
                                            else:
                                                snmpv3_bool = False
                                                break
                                        else:
                                            snmpv3_bool = False
                                            break

    #    mysql_PowerBI_EDGE_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer["logicalId"], edge, vco,
    #                                               VCO_CUSTOMER_EDGE,
    #                                              "snmpv3_bool", snmpv3_bool)

    sql_upserts.upsert_edge_attribute(curs=mysql_cursor, sql_cnx=mysql_handle, edge_id=edge['logicalId'], name='snmpv3',
                                      used=snmpv3_bool, log_name=VCO_CUSTOMER_EDGE)


def update_segment_firewall(mysql_cursor: cursor, mysql_handle: None, edge: dict, VCO_CUSTOMER_EDGE: str,
                            edge_config_stack: List[dict]):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.INFO)

    if edge_config_stack[0]['schemaVersion'] != "2.0.0":
        edge_specific = process_fw(edge_config_stack, 0)[0]
        profile_specific = process_fw(edge_config_stack, 1)[0]

        if edge_specific is not None:
            firewall_edge_specific = edge_specific
            logger.info("Found Edge specific Firewall Enabled")
        elif profile_specific is not None:
            firewall_edge_specific = profile_specific
            logger.info("Found Profile specific Firewall Enabled")
        else:
            firewall_edge_specific = False

        edge_stateful_firewall = process_fw(edge_config_stack, 0)[4]
        profile_stateful_firewall = process_fw(edge_config_stack, 1)[4]

        if edge_stateful_firewall is not None:
            stateful_firewall = edge_stateful_firewall
            logger.info("Found Edge specific Stateful Firewall Enabled")
        elif profile_stateful_firewall is not None:
            stateful_firewall = profile_stateful_firewall
            logger.info("Found Profile specific Stateful Firewall Enabled")
        else:
            stateful_firewall = False
            logger.info("No Stateful Firewall Status")

        firewall_rules_in_bool = process_fw(edge_config_stack, 0)[1] or process_fw(edge_config_stack, 1)[1]
        firewall_rules_out_bool = process_fw(edge_config_stack, 0)[2] or process_fw(edge_config_stack, 1)[2]

        edge_rules_num = process_fw(edge_config_stack, 0)[3]
        profile_rules_num = process_fw(edge_config_stack, 1)[3]
        firewall_rules_num = edge_rules_num + profile_rules_num

        sql_upserts.upsert_edge_attribute(curs=mysql_cursor, sql_cnx=mysql_handle, edge_id=edge['logicalId'],
                                          name='firewall_edge_specific', used=firewall_edge_specific,
                                          log_name=VCO_CUSTOMER_EDGE)

        sql_upserts.upsert_edge_attribute(curs=mysql_cursor, sql_cnx=mysql_handle, edge_id=edge['logicalId'],
                                          name='firewall_rules_num', num=firewall_rules_num,
                                          used=firewall_edge_specific, log_name=VCO_CUSTOMER_EDGE)

        sql_upserts.upsert_edge_attribute(curs=mysql_cursor, sql_cnx=mysql_handle, edge_id=edge['logicalId'],
                                          name='firewall_rules_out_bool', used=firewall_rules_out_bool,
                                          log_name=VCO_CUSTOMER_EDGE)

        sql_upserts.upsert_edge_attribute(curs=mysql_cursor, sql_cnx=mysql_handle, edge_id=edge['logicalId'],
                                          name='firewall_rules_in_bool', used=firewall_rules_in_bool,
                                          log_name=VCO_CUSTOMER_EDGE)

        sql_upserts.upsert_edge_attribute(curs=mysql_cursor, sql_cnx=mysql_handle, edge_id=edge['logicalId'],
                                          name='stateful_firewall', used=stateful_firewall, log_name=VCO_CUSTOMER_EDGE)
    else:
        logger.info("Non Segment 2.x.x profile handled under non_segment function earlier in code")


def process_fw(edge_config_stack: List[dict], profile_level: int):
    firewall_edge_specific = None
    firewall_rules_in_bool = False
    firewall_rules_out_bool = False
    firewall_rules_num = 0
    stateful_firewall = None

    for modules in edge_config_stack[profile_level]["modules"]:
        if modules["name"] == "firewall":
            if 'firewall_enabled' in modules['data'].keys():
                firewall_edge_specific = modules['data']['firewall_enabled']

            if 'stateful_firewall_enabled' in modules['data'].keys():
                stateful_firewall = modules['data']['stateful_firewall_enabled']

            if 'segments' in modules['data'].keys():
                if len(modules['data']['segments']) != 0:
                    for segment in modules['data']['segments']:
                        if len(segment['outbound']) != 0:
                            firewall_rules_out_bool = True
                            for rule in segment['outbound']:
                                firewall_rules_num = firewall_rules_num + 1

            if 'inbound' in modules['data'].keys():
                if len(modules['data']['inbound']) != 0:
                    firewall_rules_in_bool = True
                    for rule in modules['data']['inbound']:
                        firewall_rules_num = firewall_rules_num + 1

    return [firewall_edge_specific, firewall_rules_in_bool, firewall_rules_out_bool, firewall_rules_num,
            stateful_firewall]


def update_edge_vnf(mysql_cursor: cursor, mysql_handle: None, edge: dict, VCO_CUSTOMER_EDGE: str):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.INFO)

    has_vnf = False
    vnf_type = False
    vnf_vendor = ''
    vnf_on = False

    if "vnfs" in edge.keys() and edge['vnfs'] != None:
        if 'securityVnf' in edge['vnfs'].keys():
            has_vnf = edge['vnfs']['securityVnf']['vms'][0]['data']['insertionEnabled']
            logger.info("Found VNF defined in the Edge")

    if has_vnf:
        logger.info("Getting VNF Type, Vendor, and Power_On Status")
        vnf_type = edge['vnfs']['securityVnf']['vms'][0]['data']['type']
        vnf_vendor = edge['vnfs']['securityVnf']['vms'][0]['data']['vendor']
        vnf_on = not edge['vnfs']['securityVnf']['vms'][0]['data']['vmPowerOff']

    else:
        logger.info("No VNF installed and used in the Edge")

    sql_upserts.upsert_edge_attribute(curs=mysql_cursor, sql_cnx=mysql_handle, edge_id=edge['logicalId'],
                                      name='has_vnf', used=has_vnf, log_name=VCO_CUSTOMER_EDGE)

    sql_upserts.upsert_edge_attribute(curs=mysql_cursor, sql_cnx=mysql_handle, edge_id=edge['logicalId'],
                                      name='vnf_type', used=has_vnf, text=vnf_type, log_name=VCO_CUSTOMER_EDGE)

    sql_upserts.upsert_edge_attribute(curs=mysql_cursor, sql_cnx=mysql_handle, edge_id=edge['logicalId'],
                                      name='vnf_vendor', used=has_vnf, text=vnf_vendor, log_name=VCO_CUSTOMER_EDGE)

    sql_upserts.upsert_edge_attribute(curs=mysql_cursor, sql_cnx=mysql_handle, edge_id=edge['logicalId'], name='vnf_on',
                                      used=vnf_on, log_name=VCO_CUSTOMER_EDGE)


def process_marketing_name(mysql_cursor: cursor, mysql_handle: None, customer: dict, VCO_CUSTOMER_EDGE: str):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.INFO)
    try:
        logger.info("Start Processing Marketing name")

        trimmed_name = \
            customer['name'].replace(' Testing', '').replace(' test', '').replace('-Test', '').replace(' Test',
                                                                                                       '').replace(
                'POC ', '').replace('POC_', '').replace('PoC ', '').replace('PoC_', '').replace(' Poc', '').replace(
                ' poc', '').replace('poc_', '').replace('poc ', '').replace('_PoC', '').replace('_POC', '').replace(
                '_poc', '').replace(' PoC', '').replace('XC ', '').replace('XO ', '').replace(' POC', '').replace('  ',
                                                                                                                  ' ').strip(
                ' ').strip('-').strip(' ').split(' from VCO')[0]

        if trimmed_name:
            if re.search('[^0-9_\'\"|& -][A-Za-z^0-9_\'\"|& -][A-Za-z\'\"& - ]*', trimmed_name).group(0):
                customer_marketing_name = re.search('[^0-9_\'\"|& -][A-Za-z^0-9_\'\"|& -][A-Za-z\'\"& - ]*',
                                                    trimmed_name).group(0)
                logger.info('Found the marketing name of the customer is %s', customer_marketing_name)
        else:
            customer_marketing_name = "Invalid"
            logger.info('Could not find a marketing name for the customer, setting to invalid')

        sql_inserts.mysql_PowerBI_CUSTOMER_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, customer["logicalId"],
                                                                    VCO_CUSTOMER_EDGE, "customer_marketing_name",
                                                                    customer_marketing_name)
    except Exception as e:
        logger.critical('get Marketing Name:ERROR')
        log_critical_error(ex=e, log_name=VCO_CUSTOMER_EDGE)
        return


def edge_update_software_version(edge, sql_cnx, log_name):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': log_name})
    curs = sql_cnx.cursor()
    software_version = edge.get('softwareVersion')
    if software_version == '':
        software_version = None
    logger.info({'edge_id': edge.get('logicalId'), 'name': 'software_version', 'text': software_version})
    try:
        # add this: filter_val=f'software_version-{software_version}'
        sql_upserts.upsert_edge_attribute(curs=curs, edge_id=edge.get('logicalId'), name='software_version',
                                          sql_cnx=sql_cnx, text=software_version, log_name=log_name)
    except mysql.connector.errors.IntegrityError as e:
        logger.error(f'upsert software version failed: {e}')
    return


def update_edge_css(mysql_cursor: cursor, mysql_handle: None, edge: dict, VCO_CUSTOMER_EDGE: str, cfg: Config):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.INFO)

    has_css = False
    css_organization = ''
    css_country = ''
    css_ip = ''
    css_city = ''

    if "cloudServices" in edge.keys() and len(edge['cloudServices']) != 0:
        ## Has_CSS meaning Cloud security service is used regardless of the state (Down, UP, Backup)
        has_css = True
        ### Start processing active Zscaler Tunnel only ####
        for css in range(len(edge['cloudServices'])):
            css_item = edge['cloudServices'][css]
            if css_item['state'] == 'UP':
                css_ip = css_item['nvs_ip']
                logger.info("using maxmind")
                try:
                    client = geoip2.webservice.Client(cfg.maxmind.account_id, cfg.maxmind.license_key)
                except Exception as e:
                    log_critical_error(ex=e, log_name=VCO_CUSTOMER_EDGE)
                    pass
                try:
                    response = client.insights(css_ip)
                    css_organization = response.traits.organization
                    css_country = response.country.name
                    css_city = response.city.name
                    if css_city is None:
                        css_city = css_country
                except ValueError as e:
                    logger.error(e)
                break
    else:
        logger.info("No CSS used in the Edge")

    sql_upserts.upsert_edge_attribute(curs=mysql_cursor, sql_cnx=mysql_handle, edge_id=edge['logicalId'],
                                      name='has_css', used=has_css, log_name=VCO_CUSTOMER_EDGE)

    sql_upserts.upsert_edge_attribute(curs=mysql_cursor, sql_cnx=mysql_handle, edge_id=edge['logicalId'], name='css_ip',
                                      used=has_css, text=css_ip, log_name=VCO_CUSTOMER_EDGE)

    sql_upserts.upsert_edge_attribute(curs=mysql_cursor, sql_cnx=mysql_handle, edge_id=edge['logicalId'],
                                      name='css_organization', used=has_css, text=css_organization,
                                      log_name=VCO_CUSTOMER_EDGE)

    sql_upserts.upsert_edge_attribute(curs=mysql_cursor, sql_cnx=mysql_handle, edge_id=edge['logicalId'],
                                      name='css_country', used=has_css, text=css_country, log_name=VCO_CUSTOMER_EDGE)

    sql_upserts.upsert_edge_attribute(curs=mysql_cursor, sql_cnx=mysql_handle, edge_id=edge['logicalId'],
                                      name='css_city', used=has_css, text=css_city, log_name=VCO_CUSTOMER_EDGE)
