"""

Copyright 2018-2020 VMware, Inc.
SPDX-License-Identifier: BSD-2-Clause

"""

from datetime import timedelta, datetime
import logging
from typing import Dict

from mysql.connector import MySQLConnection

import fun_mysql_inserts as sql_inserts


def determine_if_customer_needs_update(mysql_cursor, mysql_handle, customerid, VCO_CUSTOMER_EDGE):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.INFO)
    # THIS FUNCNTION WILL DETERMINE IF CUSTOMER NEEDS UPDATE AND RETURN YES/NO
    mysql_cursor.execute("SELECT lastUpdated from Customer WHERE Customer_ID_VCO  = '%s'" % (customerid))
    result = mysql_cursor.fetchall()
    date = datetime.utcnow()
    date_before = date - timedelta(hours=20)
    date_before = date_before.strftime('%Y-%m-%d')
    for row in result:
        updatedate = row[0].strftime('%Y-%m-%d')
        logger.info(customerid + " UPDATED AT:" + str(row[0]))
        if updatedate < date_before:
            logger.info("UPDATING CUSTOMER BECAUSE IT HAS NOT BEEN UPDATED LAST Day")
            return True
        else:
            logger.info("NO CUSTOMER UPDATE NEEDED")
            return False

    logger.info("UPDATING CUSTOMER BECAUSE IT HAS NEVER BEEN UPDATED")
    return True


def determine_if_edge_needs_update(mysql_cursor, mysql_handle, EdgeID, VCO_CUSTOMER_EDGE):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.INFO)
    # THIS FUNCNTION WILL DETERMINE IF CUSTOMER NEEDS UPDATE AND RETURN YES/NO
    mysql_cursor.execute("SELECT lastUpdated from Edge WHERE EdgeID  = '%s'" % (EdgeID))
    result = mysql_cursor.fetchall()
    date = datetime.utcnow()
    date_before = date - timedelta(days=8)
    date_before = date_before.strftime('%Y-%m-%d')
    for row in result:
        updatedate = row[0].strftime('%Y-%m-%d')
        logger.info(EdgeID + " UPDATED AT:" + str(row[0]))
        if updatedate < date_before:
            logger.info("UPDATING EDGE BECAUSE IT HAS NOT BEEN UPDATED LAST 8 DAYS")
            return True
        else:
            logger.info("NO EDGE UPDATE NEEDED BASED ON LASTUPDATE")
            return False

    logger.info("UPDATING EDGE BECAUSE IT HAS NEVER BEEN UPDATED")
    return True


def determine_if_link_qoe_needs_update(mysql_cursor, mysql_handle, Lastupdate, EdgeID, VCO_CUSTOMER_EDGE):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.INFO)
    logger.debug(Lastupdate)
    # Lastupdate= "2020-05-01 00:00:00"
    # ysql_cursor.execute("SELECT Date from DailyQOE  WHERE EdgeID  = '%s'" % (EdgeID))
    mysql_cursor.execute("SELECT Date from DailyQOE  WHERE EdgeID  = '%s' AND Date = '%s'" % (EdgeID,Lastupdate))
    logger.debug("SELECT Date from DailyQOE  WHERE EdgeID  = '%s' AND Date = '%s'" % (EdgeID,Lastupdate))
    result = mysql_cursor.fetchall()
    if result:
        logger.debug("NO QOE UPDATE NEEDED")
        return False
    else:
        logger.info("UPDATING QOE BECAUSE IT HAS NOT BEEN UPDATED")
        return True


def determine_if_velo_qoe_needs_update(mysql_cursor, mysql_handle, Lastupdate, EdgeID, VCO_CUSTOMER_EDGE):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.INFO)
    print(Lastupdate)
    # Lastupdate= "2020-03-29 00:00:00"
    # Lastupdate= "2020-05-01 00:00:00"
    mysql_cursor.execute("SELECT Date from VeloDailyQOE  WHERE EdgeID  = '%s' AND Date = '%s'" % (EdgeID, Lastupdate))
    print("SELECT Date from VeloDailyQOE  WHERE EdgeID  = '%s' AND Date = '%s'" % (EdgeID, Lastupdate))
    result = mysql_cursor.fetchall()
    if result:
        logger.info(result)
        logger.info("VELOCLOUD QOE NO UPDATE NEEDED")
        return False
    else:
        logger.info("UPDATING VELOCLOUD QOE BECAUSE IT HAS NOT BEEN UPDATED")
        return True


def determine_if_edge_needs_location_update(mysql_cursor, mysql_handle, EdgeID, VCO_CUSTOMER_EDGE):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.INFO)
    mysql_cursor.execute("SELECT Country from Edge WHERE EdgeID  = '%s'" % (EdgeID))
    result = mysql_cursor.fetchall()
    date = datetime.utcnow()
    for row in result:
        country = row[0]
        if country == "Not set" or country == "not defined" or country == "not set" or len(country) < 3:
            logger.info("Edge needs location update")
            return True
        else:
            logger.info("Edge doesn't need location" + country)
            return False
    return True


def determine_if_any_edge_has_attribute_in_customer(mysql_cursor, mysql_handle, CustomerID, VCO_CUSTOMER_EDGE,
                                                    Attribute, Value):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.INFO)

    query = """SELECT * from Edge WHERE  Customer_ID_VCO = %s and Edge_Status = "CONNECTED" and """ + Attribute + """= %s ;"""
    val = (CustomerID, Value)
    mysql_cursor.execute(query, val)
    result = mysql_cursor.fetchall()
    for row in result:
        return False
    return True


def determine_if_customer_exists_in_mysql_creates_if_not(mysql_cursor, mysql_handle, customerid, Customer_Name, VCO,
                                                         VCO_CUSTOMER_EDGE):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.INFO)
    # THIS FUNCNTION WILL DETERMINE IF CUSTOMER EXISTS
    mysql_cursor.execute("SELECT lastUpdated from Customer WHERE Customer_ID_VCO  = '%s'" % (customerid))
    result = mysql_cursor.fetchall()

    for row in result:
        logger.info("UPDATING CUSTOMER PRESENT IN DATABASE")
        return

    logger.info("CUSTOMER NOT PRESENT IN DATABASE CREATE CUSTOMER IN DATABASE")
    sql_inserts.mysql_PowerBI_CUSTOMER_INSERT(mysql_handle, mysql_cursor, customerid, Customer_Name, VCO,
                                              VCO_CUSTOMER_EDGE)
    return True


def determine_if_edge_exists_in_mysql_creates_if_not(mysql_cursor, mysql_handle, customerid, edge, VCO,
                                                     VCO_CUSTOMER_EDGE):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
    logger.setLevel(logging.INFO)
    # THIS FUNCNTION WILL DETERMINE IF EDGE EXISTS
    mysql_cursor.execute("SELECT lastUpdated from Edge WHERE EdgeID  = '%s'" % (edge["logicalId"]))
    result = mysql_cursor.fetchall()

    for row in result:
        logger.info("UPDATING EDGE PRESENT IN DATABASE")
        return

    logger.info("EDGE NOT PRESENT IN DATABASE CREATE EDGE IN DATABASE")
    sql_inserts.mysql_PowerBI_EDGE_INSERT(mysql_handle, mysql_cursor, customerid, edge, VCO, VCO_CUSTOMER_EDGE)
    return True


def get_edge_attributes(sql_cnx: MySQLConnection, edge_uuid: str, log_name) -> Dict[str, Dict[str, any]]:
    """
    Get all edge attributes
    :param sql_cnx: SQL Connection
    :param edge_uuid: Edge logicalId/UUID
    :param log_name: name of logger
    :return: Dictionary of attributes by attribute name
    """
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': log_name})
    logger.info('Starting get_edge_attributes')

    curs = sql_cnx.cursor(dictionary=True)

    stmt = """SELECT * FROM EdgeAttributes WHERE edge_uuid = %(edge_uuid)s"""
    curs.execute(stmt, {'edge_uuid': edge_uuid})
    result = curs.fetchall()

    return_result = {attribute.get('name'): attribute for attribute in result}

    logger.info('Done with get_edge_attributes')
    return return_result
