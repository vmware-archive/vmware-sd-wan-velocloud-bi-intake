#!/usr/bin/env python

"""

Copyright 2018-2020 VMware, Inc.
SPDX-License-Identifier: BSD-2-Clause

"""


# LOAD PACKAGES
import mysql.connector
import argparse
import concurrent.futures
import logging
import os
import sys
import yaml

import powerbi_main_fun
from Objects.Config import Config

os.chdir(os.path.dirname(sys.argv[0]))

# Parse Input
parser = argparse.ArgumentParser()
parser.add_argument('--logging_file', type=str, help='logging File', required=True)
parser.add_argument('--VCO', type=str, help='VCO', required=False)
parser.add_argument('--CUSTOMER', type=int, help='CUSTOMER', required=False)
parser.add_argument('--EDGE', type=int, help='EDGE', required=False)
parser.add_argument('--debug', help='Debug Mode - Wont pass errors', action='store_true', required=False, default=False)
parser.add_argument('--cf', type=str, help='config file location', required=False)
parser.add_argument('--slack', help='slack notifications', action='store_true', required=False, default=False)

args = parser.parse_args()

# setup config
if args.cf:
    cf = args.cf
else:
    cf = 'DataFiles/config.yml'
cfg = Config(cfg=cf)
cfg.parse_config()

# SETUP SYSLOG AND LOCAL LOGGING ##

logger = logging.getLogger('MAIN')
logger.setLevel(logging.INFO)

# create a file handler
handler = logging.FileHandler(args.logging_file)
handler.setLevel(logging.INFO)

# create a logging format
formatter = logging.Formatter(
    '%(asctime)s - %(VCO_CUSTOMER_EDGE)s - %(funcName)s - %(lineno)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

VCO_CUSTOMER_EDGE = 'MAIN'
local_logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})

cnx = mysql.connector.connect(host=cfg.mysql_prod.host, database=cfg.mysql_prod.db, user=cfg.mysql_prod.user,
                              password=cfg.mysql_prod.password)

mycursor = cnx.cursor()

with open(cfg.files.vco_list) as f:
    vco_list = yaml.load(f, Loader=yaml.FullLoader)

# NEEDS DEBUG FUNCTION TO RUN FOR SPECIFIC EDGE/CUSTOMER/VCO
if args.VCO:
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(formatter)
    logger.addHandler(console)

    local_logger.info(f'starting single VCO: {args.VCO} - Customer: {args.CUSTOMER}')
    powerbi_main_fun.process_vco(vco=args.VCO, cfg=cfg, arg_customer=args.CUSTOMER, slack_notifications=args.slack,
                                 debug=args.debug, vco_list=vco_list)

    quit()


with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    for vco in vco_list:
        local_logger.info(vco_list[vco]['link'])

        executor.submit(powerbi_main_fun.process_vco, vco=vco, cfg=cfg, slack_notifications=args.slack,
                        debug=args.debug, vco_list=vco_list)
        local_logger.info(f'SUBMITTED: {vco_list.get(vco, {}).get("link")}')

    executor.shutdown()
local_logger.info('ALL DONE')
