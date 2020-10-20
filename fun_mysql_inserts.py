"""

Copyright 2018-2020 VMware, Inc.
SPDX-License-Identifier: BSD-2-Clause

"""

import logging
import re
from datetime import datetime
from typing import Dict

from mysql.connector import cursor, MySQLConnection


def mysql_PowerBI_SLA_EDGE_INSERT(mysql_handle, mysql_cursor, EdgeID, VCO, EdgeName, EdgeStatus, CustomerName,
                                  Customer_ID):
    query = """INSERT INTO EDGE ( EdgeID, VCO, EdgeName, EdgeStatus, CustomerName,Customer_ID)
                           VALUES (%s, %s, %s, %s, %s, %s)   
                           ON DUPLICATE KEY UPDATE
                            VCO = VALUES(VCO), 
                            EdgeName = VALUES(EdgeName), 
                            EdgeStatus= VALUES(EdgeStatus),
                            lastUpdated = NOW(),
                            CustomerName= VALUES(CustomerName),
                            Customer_ID= VALUES(Customer_ID) 
                            ;
               """
    val = (EdgeID, VCO, EdgeName, EdgeStatus, CustomerName, Customer_ID)
    mysql_cursor.execute(query, val)
    mysql_handle.commit()


def mysql_PowerBI_SLA_CUSTOMER_INSERT(mysql_handle, mysql_cursor, Customer_ID, Customer_Name, VCO):
    query = """INSERT INTO CUSTOMER (  Customer_ID, Customer_Name, VCO)
                           VALUES (%s, %s, %s)   
                           ON DUPLICATE KEY UPDATE
                            Customer_Name = VALUES(Customer_Name), 
                            VCO= VALUES(VCO) 
                            ;
               """
    val = (Customer_ID, Customer_Name, VCO)
    mysql_cursor.execute(query, val)
    mysql_handle.commit()


def mysql_PowerBI_INSERT_LINK(mysql_handle, mysql_cursor, VCO_CUSTOMER_EDGE, EdgeID, LinkUUID, LinkName, ISP, Interface,
                              Latitude, Longitude, NetworkSide, Networktype, LinkIpAddress, MTU, OverlayType, Linktype,
                              LinkMode, VLANID):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.INFO)
    query = """INSERT IGNORE INTO Links (EdgeID, LinkUUID, LinkName,  ISP, Interface, Latitude, Longitude, NetworkSide, Networktype, LinkIpAddress, MTU, OverlayType, Linktype, LinkMode, VLANID)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                LinkName= VALUES(LinkName),
                ISP= VALUES(ISP),
                Interface= VALUES(Interface),
                Latitude= VALUES(Latitude),
                Longitude= VALUES(Longitude),
                NetworkSide= VALUES(NetworkSide),
                Networktype= VALUES(Networktype),
                LinkIpAddress= VALUES(LinkIpAddress),
                MTU= VALUES(MTU),
                OverlayType= VALUES(OverlayType),
                Linktype= VALUES(Linktype),
                LinkMode= VALUES(LinkMode),
                VLANID= VALUES(VLANID)
                ;
    """
    if EdgeID:
        val = (EdgeID, LinkUUID, LinkName, ISP, Interface, Latitude, Longitude, NetworkSide, Networktype, LinkIpAddress,
               MTU, OverlayType, Linktype, LinkMode, VLANID)
    logger.info(val)
    mysql_cursor.execute(query, val)
    mysql_handle.commit()


def mysql_PowerBI_CUSTOMER_INSERT(mysql_handle, mysql_cursor, Customer_ID, Customer_Name, VCO, VCO_CUSTOMER_EDGE):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.INFO)
    query = """INSERT INTO Customer (  Customer_ID_VCO, Customer_Name, VCO)
                           VALUES (%s, %s, %s);
               """
    val = (Customer_ID, Customer_Name, VCO)
    logger.info("INSERTING CUSTOMER")
    logger.info(val)
    mysql_cursor.execute(query, val)
    mysql_handle.commit()


def mysql_PowerBI_EDGE_INSERT(mysql_handle, mysql_cursor, Customer_ID, edge, VCO, VCO_CUSTOMER_EDGE):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.INFO)
    name = re.match("[A-Za-z0-9_ -]{1,60}", edge["name"])
    if name:
        EdgeName = name.group(0)
    else:
        EdgeName = "Invalid"
    query = """INSERT INTO Edge (  EdgeID, Customer_ID_VCO,EdgeName, Edge_Status)
                           VALUES (%s, %s, %s, %s);
               """
    val = (edge['logicalId'], Customer_ID, EdgeName, edge["edgeState"])
    logger.info("INSERTING Edge:")
    logger.info(val)
    mysql_cursor.execute(query, val)
    mysql_handle.commit()


def mysql_PowerBI_EDGE_UPDATE_LOCATION(mysql_handle, mysql_cursor, Customer_ID, edge, VCO, VCO_CUSTOMER_EDGE, City,
                                       State, Country, PostalCode, lat, lon, Geospecific):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.INFO)

    query = """UPDATE Edge 
               SET  City=%s, State=%s,Country=%s,PostalCode=%s,lat=%s,lon=%s,Geospecific=%s
               WHERE EdgeID=%s;
               """
    val = (City, State, Country, PostalCode, lat, lon, Geospecific, edge['logicalId'])
    logger.info("UPDATE City State Country PostalCode lat lon Geospecific ")
    logger.info(val)
    mysql_cursor.execute(query, val)
    mysql_handle.commit()


def mysql_PowerBI_EDGE_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, Customer_ID, edge, VCO, VCO_CUSTOMER_EDGE,
                                                ATTRIBUTE, VALUE):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.INFO)

    query = """UPDATE Edge 
               SET """ + ATTRIBUTE + """=%s
               WHERE EdgeID=%s; """

    val = (VALUE, edge['logicalId'])
    logger.info("UPDATE %s VALUE EDGE ", ATTRIBUTE)
    logger.info(val)
    mysql_cursor.execute(query, val)
    mysql_handle.commit()


def mysql_PowerBI_CUSTOMER_UPDATE_GENERIC_ATTRIBUTE(mysql_handle, mysql_cursor, Customer_ID, VCO_CUSTOMER_EDGE,
                                                    ATTRIBUTE, VALUE):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.INFO)

    query = """UPDATE Customer 
               SET """ + ATTRIBUTE + """=%s
               WHERE Customer_ID_VCO=%s; """

    val = (VALUE, Customer_ID)
    logger.info("UPDATE %s VALUE EDGE ", ATTRIBUTE)
    mysql_cursor.execute(query, val)
    mysql_handle.commit()


def mysql_PowerBI_EDGE_INSERT_QOE(mysql_handle, mysql_cursor, edge, vco, VCO_CUSTOMER_EDGE, Date, EdgeID, LinkUUID,
                                  Score, lowest_linkscore, LinkBlackouts, LinkBlackoutDuration, LinkBrownouts,
                                  LinkBrownoutDuration):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.INFO)
    query = """INSERT  IGNORE  INTO DailyQOE (Date, EdgeID, LinkUUID , Score,lowest_linkscore, LinkBlackouts, LinkBlackoutDuration, LinkBrownouts,LinkBrownoutDuration)
                                            VALUES ( %s, %s, %s, %s, %s, %s, %s,%s,%s)
                                            ON DUPLICATE KEY UPDATE
                                            Date= VALUES(Date),
                                            EdgeID= VALUES(EdgeID),
                                            LinkUUID= VALUES(LinkUUID),
                                            Score= VALUES(Score),
                                            lowest_linkscore = VALUES(lowest_linkscore),
                                            LinkBlackouts= VALUES(LinkBlackouts),
                                            LinkBlackoutDuration= VALUES(LinkBlackoutDuration),
                                            LinkBrownouts= VALUES(LinkBrownouts),
                                            LinkBrownoutDuration= VALUES(LinkBrownoutDuration)
                                            ;
                               """
    val = (Date, EdgeID, LinkUUID, Score, lowest_linkscore, LinkBlackouts, LinkBlackoutDuration, LinkBrownouts,
           LinkBrownoutDuration)
    logger.info(
        "INSERT IGNORE INTO DailyQOE (Date, EdgeID, LinkUUID , Score,lowest_linkscore, LinkBlackouts, LinkBlackoutDuration, LinkBrownouts,LinkBrownoutDuration)")
    logger.info(val)
    mysql_cursor.execute(query, val)
    mysql_handle.commit()


def mysql_PowerBI_LICENSE_UPDATE_USED_BANDWIDTH_FEATURES(mysql_handle, mysql_cursor, edge, vco, VCO_CUSTOMER_EDGE,
                                                         top_bandwidth_in_mbps, fifth_top_throughput,
                                                         tenth_top_throughput, feature_set, b2b_via_gw, pb_via_gw,
                                                         css_via_gw, nvs_via_gw, b2b_via_hub, pb_internet_via_direct,
                                                         pb_internet_via_hub):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.INFO)

    query = """INSERT IGNORE INTO License (EdgeID, highest_throughput_in_mbps, fifth_top_throughput, tenth_top_throughput, feature_set, b2b_via_gw, pb_via_gw, css_via_gw, nvs_via_gw, b2b_via_hub, pb_internet_via_direct, pb_internet_via_hub)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON DUPLICATE KEY UPDATE
                                highest_throughput_in_mbps = VALUES(highest_throughput_in_mbps),
                                fifth_top_throughput = VALUES(fifth_top_throughput),
                                tenth_top_throughput = VALUES(tenth_top_throughput),
                                feature_set = VALUES(feature_set), 
                                b2b_via_gw = VALUES(b2b_via_gw),
                                pb_via_gw = VALUES(pb_via_gw),
                                css_via_gw = VALUES(css_via_gw),
                                nvs_via_gw = VALUES(nvs_via_gw),
                                b2b_via_hub = VALUES(b2b_via_hub),
                                pb_internet_via_direct = VALUES(pb_internet_via_direct),
                                pb_internet_via_hub = VALUES(pb_internet_via_hub)
                               ; 
                        """
    val = (
        edge['logicalId'], top_bandwidth_in_mbps, fifth_top_throughput, tenth_top_throughput, feature_set, b2b_via_gw,
        pb_via_gw, css_via_gw, nvs_via_gw, b2b_via_hub, pb_internet_via_direct, pb_internet_via_hub)
    logger.info(
        "INSERT IGNORE INTO License (EdgeID, highest_throughput_in_mbps, fifth_top_throughput, tenth_top_throughput, feature_set, b2b_via_gw, pb_via_gw, css_via_gw, nvs_via_gw, b2b_via_hub, pb_internet_via_direct, pb_internet_via_hub")
    logger.info(val)
    mysql_cursor.execute(query, val)
    mysql_handle.commit()


def mysql_PowerBI_LICENSE_VC(mysql_handle, mysql_cursor, edge_uuid, vco_customer_edge, license_sku, license_start,
                             license_end, license_active, license_term_months, edition, bandwidth_tier, add_ons):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': vco_customer_edge})
    logger.setLevel(logging.INFO)

    query = """INSERT IGNORE INTO License (EdgeID, sku, start,  end, active, termMonths, edition, bandwidthTier, addOns)
                                             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                                             ON DUPLICATE KEY UPDATE
                                             sku= VALUES(sku),
                                             start= VALUES(start),
                                             end= VALUES(end),
                                             active= VALUES(active),
                                             termMonths= VALUES(termMonths),
                                             edition= VALUES(edition),
                                             bandwidthTier= VALUES(bandwidthTier),
                                             addOns= VALUES(addOns)
                                             ;
                                 """
    val = (
    edge_uuid, license_sku, license_start, license_end, license_active, license_term_months, edition, bandwidth_tier,
    add_ons)
    logger.info(query)
    logger.info(val)
    mysql_cursor.execute(query, val)
    mysql_handle.commit()


def mysql_PowerBI_EDGE_INSERT_LINK(mysql_handle, mysql_cursor, Customer_ID, edge, VCO, VCO_CUSTOMER_EDGE, LinkUUID,
                                   LinkName, ISP, Interface, Latitude, Longitude, NetworkSide, Networktype,
                                   LinkIpAddress, MTU, OverlayType, Linktype, LinkMode, VLANID):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.INFO)

    query = """INSERT IGNORE INTO Links (EdgeID, LinkUUID, LinkName,  ISP, Interface, Latitude, Longitude, NetworkSide, Networktype, LinkIpAddress, MTU, OverlayType, Linktype, LinkMode, VLANID)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s)
                       ON DUPLICATE KEY UPDATE
                       LinkName= VALUES(LinkName),
                       ISP= VALUES(ISP),
                       Interface= VALUES(Interface),
                       Latitude= VALUES(Latitude),
                       Longitude= VALUES(Longitude),
                       NetworkSide= VALUES(NetworkSide),
                       Networktype= VALUES(Networktype),
                       LinkIpAddress= VALUES(LinkIpAddress),
                       MTU= VALUES(MTU),
                       OverlayType= VALUES(OverlayType),
                       Linktype= VALUES(Linktype),
                       LinkMode= VALUES(LinkMode),
                       VLANID= VALUES(VLANID)
                       ;
           """
    val = (edge["logicalId"], LinkUUID, LinkName, ISP, Interface, Latitude, Longitude, NetworkSide, Networktype,
           LinkIpAddress, MTU, OverlayType, Linktype, LinkMode, VLANID)
    logger.info(query)
    logger.info(val)
    mysql_cursor.execute(query, val)
    mysql_handle.commit()


def mysql_PowerBI_EDGE_INSERT_EVENT(mysql_handle, mysql_cursor, Customer_ID, edge, VCO, VCO_CUSTOMER_EDGE, Date, Name,
                                    Type):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.INFO)

    query = """INSERT IGNORE INTO Events ( Date, EdgeID, Name, Type)
                            VALUES (%s, %s, %s, %s)"""
    val = (Date, edge['logicalId'], Name, Type)
    logger.info("Insert ( Date, EdgeID, Name, Type)")
    logger.info(val)
    mysql_cursor.execute(query, val)
    mysql_handle.commit()


def mysql_PowerBI_EDGE_UPDATE_BASIC_ATTRIBUTES(mysql_handle: object, mysql_cursor: object, Customer_ID: object,
                                               edge: object, VCO: object, VCO_CUSTOMER_EDGE: object, Profile_ID: object,
                                               Activation_Status: object, Certificate: object, Version: object,
                                               EdgeName: object, Edge_Status: object, Model: object,
                                               Activated_Day: object, Activated_Days: object, serial: object,
                                               ha_serial: object, streetAddress: object) -> object:
    # CHANGE FROM OBJECT TO SPECIFIC LATER
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.INFO)

    query = """UPDATE Edge 
               SET  Profile_ID=%s, Activation_Status=%s,Certificate=%s,Version=%s,Activated_Day=%s,EdgeName=%s, Edge_Status=%s, Model=%s, Activated_Days=%s, SerialNumber=%s, ha_serial=%s, street_address=%s 
               WHERE EdgeID=%s;
               """
    val = (Profile_ID, Activation_Status, Certificate, Version, Activated_Day, EdgeName, Edge_Status, Model,
           Activated_Days, serial, ha_serial, streetAddress, edge['logicalId'])
    logger.info(
        "UPDATE Profile_ID,Activation_Status ,Certificate,Version,Activated_Day,EdgeName,Edge_Status,Model,Activated_Days,Serial,HaSerial,streetaddress")
    logger.info(val)
    logger.info(query)
    mysql_cursor.execute(query, val)
    mysql_handle.commit()


def update_customer_with_vco_name_partner(mysql_cursor: cursor, mysql_handle, customer: Dict[str, any], vco_link: str,
                                          vco_partner: str, log_name: str):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': log_name})
    logger.setLevel(logging.INFO)

    customer_creation_date = datetime.strptime(customer.get('created', '').split('T')[0], '%Y-%m-%d')
    partner = customer.get('enterpriseProxyName')
    if partner is None:
        partner = vco_partner

    values = {'customer_uuid': customer.get('logicalId'), 'customer_id': customer.get('id'),
              'customer_name': customer.get('name'), 'customer_creation_date': customer_creation_date,
              'vco_link': vco_link, 'partner': partner, 'customer_status': 'ACTIVE'}

    query = """
            UPDATE Customer 
            SET  
            Customer_Name=%(customer_name)s,
            VCO=%(vco_link)s,
            enterprise_id=%(customer_id)s,
            Partner=%(partner)s,
            CustomerCreationDate=%(customer_creation_date)s,
            customer_status=%(customer_status)s,
            lastUpdated = NOW() 
            WHERE Customer_ID_VCO=%(customer_uuid)s;
            """

    logger.info(f'UPDATE CUSTOMER: {values}')
    mysql_cursor.execute(query, values)
    mysql_handle.commit()


def upsert_attribute(curs: cursor, table_name: str, unique_key: str, unique_key_name: str, name: str, used: bool,
                     num: int, text: str, filter_val: str = None, sql_cnx: MySQLConnection = None):
    # Raise Error if input is not a boolean or None
    if used not in [True, False, 0, 1, None]:
        raise TypeError(f'Used parameter is not boolean or None - Parameter was: {used}')

    # Set Default value for filter_val if filter_val is None
    if filter_val is None:
        filter_val = f'{name}-{used}'

    values = {'table_name': table_name, unique_key_name: unique_key, 'name': name, 'used': used, 'num': num,
              'text': text, 'filter_val': filter_val}

    query = f"""
            INSERT INTO {table_name} ({unique_key_name}, name, used, num, text, filter_val)
            VALUES (%({unique_key_name})s, %(name)s, %(used)s, %(num)s, %(text)s, %(filter_val)s)
            ON DUPLICATE KEY UPDATE 
                used=VALUES(used), 
                num=VALUES(num), 
                text=VALUES(text),
                filter_val=VALUES(filter_val);
            """

    curs.execute(query, values)
    # only commit if a handle is provided, leaving the ability to commit multiple attributes with one commit call
    if sql_cnx:
        sql_cnx.commit()
    return
