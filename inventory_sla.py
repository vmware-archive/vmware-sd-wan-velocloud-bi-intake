"""

Copyright 2018-2020 VMware, Inc.
SPDX-License-Identifier: BSD-2-Clause

"""

import argparse
import random
import sys
from time import sleep
import re

import mysql.connector
import urllib3
import yaml
import logging
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning

from Objects.Config import Config
from VCOClient import VcoRequestManager, ApiException
import fun_mysql_inserts as sql_inserts

###################
# Parse Input
###################

parser = argparse.ArgumentParser()
parser.add_argument('--logging_file', type=str, help='logging File', required=True)
parser.add_argument('--cf', type=str, help='config file location', required=False)
args = parser.parse_args()

# setup config
if args.cf:
    cf = args.cf
else:
    cf = 'DataFiles/config.yml'
cfg = Config(cfg=args.cf)
cfg.parse_config()

with open(cfg.files.vco_list) as f:
    vco_list = yaml.load(f, Loader=yaml.BaseLoader)

###################
# SETUP SYSLOG AND LOCAL LOGGING ##
###################

local_logger = logging.getLogger('MAIN')
local_logger.setLevel(logging.INFO)

# create a file handler
handler = logging.FileHandler(args.logging_file)
handler.setLevel(logging.INFO)

# create a logging format
formatter = logging.Formatter('%(asctime)s - %(funcName)s - %(lineno)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# add the handlers to the logger
local_logger.addHandler(handler)

##
# THIS CODE ADDS LOGGER TO CONSOLE NOT SUPER USEFULL WITH THREADING
##
console = logging.StreamHandler()
console.setLevel(logging.INFO)
# tell the handler to use this format
console.setFormatter(formatter)
local_logger.addHandler(console)
urllib3.disable_warnings()
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# Set up SQL Connection
cnx = mysql.connector.connect(host=cfg.mysql_prod.host, database=cfg.mysql_prod.db, user=cfg.mysql_prod.user,
                              password=cfg.mysql_prod.password)
mycursor = cnx.cursor()

for vco in random.sample(list(vco_list), 50):
    print(vco_list[vco]['link'])

    # if vco_list[vco]['link'] == "vco11-usvi1.velocloud.net":
    #  pass
    # else:
    #  continue

    urllib3.disable_warnings()
    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

    if 'token' in vco_list[vco].keys():
        client = VcoRequestManager(vco_list[vco]['link'], verify_ssl=False)
        client._session.headers.update({'Authorization': "Token " + vco_list[vco]['token']})
    else:
        try:
            client = VcoRequestManager(vco_list[vco]['link'], verify_ssl=False)
            client.authenticate(vco_list[vco]['username'], vco_list[vco]['password'], is_operator=True)

        except:
            local_logger.error("Unable to connect")
            local_logger.error("Unexpected error:", sys.exc_info()[0])

    #####################
    # Get Customer List #
    #####################

    params = {"networkId": 1, "with": []}
    kwargs = {"timeout": 300}
    get_customer = []
    get_customer = client.call_api('/network/getNetworkEnterprises', params, **kwargs)
    for customer in get_customer:

        name = re.match('^[A-Za-z0-9_\'\"|& -]{1,60}', customer["name"])

        if name:
            CustomerName = name.group(0)
        else:
            CustomerName = "Invalid"
        mysql_PowerBI_SLA_CUSTOMER_INSERT(cnx, mycursor, customer["logicalId"], CustomerName, vco_list[vco]['link'])

        #################
        # Get Edge List #
        #################

        print(CustomerName)

        params = {"enterpriseId": customer["id"], "with": []}
        kwargs = {"timeout": 300}
        get_edges = client.call_api('/enterprise/getEnterpriseEdges', params, **kwargs)
        sleep(0.05)

        for edge in get_edges:
            name = re.match("[A-Za-z0-9_ -]{1,60}", edge["name"])
            if name:
                EdgeName = name.group(0)
            else:
                EdgeName = "Invalid"
            if edge["logicalId"]:
                sql_inserts.mysql_PowerBI_SLA_EDGE_INSERT(cnx, mycursor, edge["logicalId"], vco_list[vco]['link'],
                                                          EdgeName, edge["edgeState"], CustomerName,
                                                          customer["logicalId"])

cnx.close()
