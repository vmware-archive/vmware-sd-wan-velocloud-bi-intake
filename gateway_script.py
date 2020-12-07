"""

Copyright 2018-2020 VMware, Inc.
SPDX-License-Identifier: BSD-2-Clause

"""


import mysql.connector
import argparse
import concurrent.futures
import json
import logging
import os
import sys
import yaml
from itertools import islice
from datetime import datetime,timedelta
import requests
import urllib
import re
import urllib3
import certifi
from geopy.geocoders import Nominatim
import geoip2.webservice
from time import sleep
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import Functions.vco_calls as vco_calls
from Objects.Config import Config
from VCOClient import VcoRequestManager
# LOAD AUX FILES

parser = argparse.ArgumentParser()
parser.add_argument('--start_range', type=int, help='start_vco', required=False)
parser.add_argument('--end_range', type=int, help='end_vco', required=False)
parser.add_argument('--logging_file', type=str, help='logging File', required=True)
parser.add_argument('--VCO', type=str, help='VCO', required=False)

args = parser.parse_args()
# setup config

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

console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(formatter)
logger.addHandler(console)
VCO_CUSTOMER_EDGE = 'MAIN'
local_logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': VCO_CUSTOMER_EDGE})
urllib3.disable_warnings()
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

cnx = mysql.connector.connect(host=cfg.mysql_prod.host, database=cfg.mysql_prod.db,
                              user=cfg.mysql_prod.user, password=cfg.mysql_prod.password)

mycursor = cnx.cursor()


with open(cfg.files.vco_list) as f:
    vco_list = yaml.load(f, Loader=yaml.BaseLoader)


def uo(args, **kwargs):
    return urllib.request.urlopen(args, cafile=certifi.where(), **kwargs)



def gateway_update_process(client, mycursor, VCO_CUSTOMER_EDGE):
    date = datetime.utcnow()
    date_before = date - timedelta(hours=1)
    kwargs = {"timeout": 200}
    params = {"with": ["edgeCount", "edgeConfigUpdate"]}
    get_customers_reply = client.call_api('network/getNetworkEnterprises', params, **kwargs)
    #print (get_customers_reply)  
    kwargs = {"timeout": 200}
    params = {"with": ["site", "roles", "pools", "dataCenters", "certificates", "enterprises", "handOffEdges", "enterpriseAssociationCounts"] }
    get_gateways = client.call_api('network/getNetworkGateways', params, **kwargs)
    #print json.dumps(get_gateways, indent=4, sort_keys=True)
    local_logger.info( "Pulled Gateway API Call")
    
    for gw in get_gateways:
     local_logger.info(gw["name"])
     #if gw["name"] == "vcg162-usil1":
     if gw["gatewayState"] == "CONNECTED":
         #if gw["gatewayState"]:
      Date = datetime.now().strftime('%Y-%m-%d 00:00:00')
      GatewayName = gw["name"]
      GatewayID = gw["logicalId"]
      GWVersion =  gw["buildNumber"]
      #GWCity = gw["site"]["city"]
      #GWState = gw["site"]["state"]
      #GWCountry = gw["site"]["country"]
      GWCity = "Not set"
      GWState = "Not set"
      GWCountry = "Not set"
      geospecific = "Not set"
      GWPostalCode = "Not set"
      geolocator = Nominatim(user_agent="get link details")
      geolocator.urlopen = uo
      #print (gw["ipAddress"])
      # Try to get location using geolocation
      try:
        geos = [json.loads(line) for line in open('DataFiles/country.json', 'r')]
        try:
            ### NOTE THIS CODE NEEDS TO BE IMPROVED, WE SHOULD TAKE IN ACCOUNT WHEN WE DONT HAVE LAT AND LONG BUT WE HAVE AN ADDRESS THAT WE CAN USE TO DETERMINE LAT AND LON
            if gw["site"]["lat"] != None and gw["site"]["lon"] != None:
                lat = gw["site"]["lat"]
                lon = gw["site"]["lon"]
                geoval = '%s,%s' % (gw["site"]["lat"], gw["site"]["lon"])
                location = geolocator.reverse(geoval, language="en-US,en")
                sleep(10)  # sleeping since there is a limit of quota usage
                data = location.raw
                data = data['address']
                local_logger.info(data)
                if 'state' in data:
                    GWState = str(data['state'])
                elif gw["site"]["state"] != None:
                    GWState = gw["site"]["state"]

                if 'city' in data:
                    GWCity = str(data['city'])
                elif 'county' in data:
                    GWCity = str(data['county'])
                elif gw["site"]["city"] != None:
                    GWCity = gw["site"]["city"]

                if 'country' in data:
                    GWCountry = str(data["country"])
                elif gw["site"]["country"] != None:
                    GWCountry = gw["site"]["country"]

                if 'postcode' in data:
                    str(data['postcode'])
                    GWPostalCode = str(data['postcode'])
                    if re.findall('[^A-Za-z0-9_  .-]', GWPostalCode):
                        GWPostalCode = gw["site"]["postalCode"]
                    else:
                        local_logger.info("regular string")
                        GWPostalCode = GWPostalCode
                else:
                    GWPostalCode = gw["site"]["postalCode"]


                for geo in geos:
                    #if geo["Country"] == Country or geo["ISO"] == Country:
                    if geo["ISO"].lower() == data['country_code'].lower():
                        geospecific = geo["REG"]

            else:
                 logger.info("using maxmind")
                 client = geoip2.webservice.Client(73615, 'WZgmKOkO3ywZ')
                 response = client.insights(gw['ipAddress'])
                 lat = response.location.latitude
                 lon = response.location.longitude
                 geoval = '%s,%s' % (lat, lon)
                 location = geolocator.reverse(geoval, language="en-US,en")
                 sleep(10)
                 data = location.raw
                 data = data['address']
                 local_logger.info(data)
                 if 'state' in data:
                     GWState = str(data['state'])
                 elif gw["site"]["state"] != None:
                    GWState = gw["site"]["state"]

                 if 'city' in data:
                     GWCity = str(data['city'])
                 elif 'county' in data:
                     GWCity = str(data['county'])
                 elif gw["site"]["city"] != None:
                     GWCity = gw["site"]["city"]

                 if 'country' in data:
                     GWCountry = str(data["country"])
                 elif gw["site"]["country"] != None:
                     GWCountry = gw["site"]["country"]
 
                 if 'postcode' in data:
                     str(data['postcode'])
                     GWPostalCode = str(data['postcode'])
                     if re.findall('[^A-Za-z0-9_  .]', GWPostalCode):
                         GWPostalCode = gw["site"]["postalCode"]
                     else:
                         local_logger.info("regular string")
                         GWPostalCode = GWPostalCode
                 else:
                     GWPostalCode = gw["site"]["postalCode"]


                 for geo in geos:
                     #if geo["Country"] == Country or geo["ISO"] == Country:
                     if geo["ISO"].lower() == data['country_code'].lower():
                         geospecific = geo["REG"]

        except Exception as e:
            local_logger.critical(e)
      except:
         local_logger.critical("UNABLE TO BUILD LOCATION")
      GWLAT = gw["site"]["lat"]
      GWLON = gw["site"]["lon"]
      GWActivationtime = gw["activationTime"]
      GWActivationState = gw["activationState"]
      GWCurrentstatus = gw["gatewayState"]
      GWLogicalID = gw["logicalId"]
      GWuptime = gw["systemUpSince"]
      if gw["connectedEdges"] != None:
          GWconnectededges = gw["connectedEdges"]
      else:
          GWconnectededges = "0"
      if gw["utilizationDetail"]["cpu"] != None:
          GWCPU = gw["utilizationDetail"]["cpu"]
      else:
          GWCPU = "0"
      if gw["utilizationDetail"]["load"] != None:
         GWload = gw["utilizationDetail"]["load"]
      else:
         GWload = "0"
      if gw["utilizationDetail"]["memory"] != None:
         GWMemory = gw["utilizationDetail"]["memory"]
      else:
         GWMemory = "0"
      if gw["site"]["contactEmail"] == "support@velocloud.net" and  gw["handOffDetail"] is None:
            local_logger.info("Cloud gateway")
            GatewayType = "None"
      else:
        local_logger.info("partner gateway")
      GatewayType = "ALLOW"
      GWpki = gw["endpointPkiMode"]
      gwpool = gw["pools"]
      current_time = datetime.now()
      start_new = current_time  - timedelta(hours=24)
      try:
       kwargs = {"timeout": 200}
       params = { "gatewayId": gw["id"] ,"interval": {"start": start_new }, "metrics": ["cpuPct", "memoryPct", "flowCount", "handoffQueueDrops", "tunnelCount"] }
       get_met = client.call_api('metrics/getGatewayStatusMetrics', params, **kwargs)
       #print json.dumps(get_met, indent=4, sort_keys=True)
       local_logger.info ("Gateway Metrics API call pulled")
       GWCPU  = get_met["cpuPct"]["max"]
       gw_flow_count= get_met["flowCount"]["max"]
       gw_handoff= get_met["handoffQueueDrops"]["max"]
       GWMemory= get_met["memoryPct"]["max"]
       gw_tunnel = get_met["tunnelCount"]["max"]
      except:
        gw_flow_count = 0
        gw_handoff = 0
        gw_tunnel =  0
      #Date = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
      Date = datetime.now().strftime('%Y-%m-%d 00:00:00')
      query = """INSERT IGNORE INTO Gateways (Date,GatewayID, GatewayName, GWVersion, GWCity, GWState, GWCountry, GWLAT, GWLON, GWActivationtime,
                   GWActivationState, GWCurrentstatus, GWuptime, GWconnectededges, GWCPU, GWMemory,GWload, GWpki, GatewayType ,gw_flow_count, gw_handoff,  gw_tunnel, geospecific, GWPostalCode)
                                     VALUES (%s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s,%s)
                                     ON DUPLICATE KEY UPDATE
                                     Date = VALUES(DATE),
                                     GatewayName = VALUES(GatewayName),
                                     GWVersion = VALUES(GWVersion),
                                     GWCity = VALUES(GWCity),
                                     GWState = VALUES(GWState),
                                     GWCountry = VALUES(GWCountry),
                                     GWLAT = VALUES(GWLAT),
                                     GWLON = VALUES(GWLON),
                                     GWActivationtime = VALUES(GWActivationtime),
                                     GWActivationState = VALUES(GWActivationState),
                                     GWCurrentstatus = VALUES(GWCurrentstatus),
                                     GWuptime = VALUES(GWuptime),
                                     GWconnectededges = VALUES(GWconnectededges),
                                     GWCPU = VALUES(GWCPU),
                                     GWMemory = VALUES(GWuptime),
                                     GWload = VALUES(GWload),
                                     GWpki = VALUES(GWpki),
                                     GatewayType = VALUES(GatewayType),
                                     gw_flow_count = VALUES(gw_flow_count),
                                     gw_handoff = VALUES(gw_handoff),
                                     gw_tunnel = VALUES(gw_tunnel),
                                     geospecific = VALUES(geospecific),
                                     GWPostalCode  = VALUES(GWPostalCode)
                                      ;
                           """
      print(Date,GatewayID, GatewayName, GWVersion, GWCity, GWState, GWCountry, GWLAT, GWLON, GWActivationtime,GWActivationState, GWCurrentstatus, GWuptime, GWconnectededges, GWCPU, GWMemory ,GWload, GWpki, GatewayType,gw_flow_count, gw_handoff,  gw_tunnel,geospecific,GWPostalCode)
      if GatewayID:
          val = (Date, GatewayID,GatewayName, GWVersion, GWCity, GWState, GWCountry, GWLAT, GWLON, GWActivationtime,GWActivationState, GWCurrentstatus, GWuptime, GWconnectededges, GWCPU, GWMemory, GWload, GWpki, GatewayType,gw_flow_count, gw_handoff,  gw_tunnel,geospecific,GWPostalCode)
      mycursor.execute(query, val)
      cnx.commit()
      local_logger.info("Updated Gateway details")
      try:
       for edgelist in gw["connectedEdgeList"]:
        EdgeID =  edgelist["vceid"]
        GatewayName = gw["name"]
        GatewayID = gw["logicalId"]
        #Date = date_start_string
        Date = datetime.now().strftime('%Y-%m-%d 00:00:00')
        #Date = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        query = """INSERT IGNORE INTO gatewayrelation (EdgeID, GatewayID, Date) 
                                     VALUES (%s, %s, %s)
                                     ON DUPLICATE KEY UPDATE
                                     Date= VALUES(Date)
                                      ;
                           """
        #print (Date,GatewayID, EdgeID)
        if EdgeID:
          val = (EdgeID, GatewayID, Date)
        mycursor.execute(query, val)
        cnx.commit()
      except:
         pass
      local_logger.info("Updated Gateway Edge relation details")

    cnx.close()
if args.VCO:
    if 'token' in vco_list[args.VCO].keys():
        client = VcoRequestManager(vco_list[args.VCO]['link'], verify_ssl=False)
        client._session.headers.update({'Authorization': "Token " + vco_list[args.VCO]['token']})
    else:
        try:
            client = VcoRequestManager(vco_list[args.VCO]['link'], verify_ssl=False)
            client.authenticate(vco_list[args.VCO]['username'], vco_list[args.VCO]['password'],
                                is_operator=True)

        except Exception:
            local_logger.critical('powerbi_main_script error gXqY3cf752xmKFW87g7')
            local_logger.error("Unable to connect")
            local_logger.error("Unexpected error:", sys.exc_info()[0])
    gateway_update_process(client, mycursor, VCO_CUSTOMER_EDGE)

with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    for vco in vco_list:
            local_logger.info(vco_list[vco]['link'])
            vco_info = vco_list.get(vco)
            vco_info['name'] = vco
            print (vco_info)
            vco_client, conn_err_msg = vco_calls.connect_to_vco(vco=vco_info)
            if vco_client:
                local_logger.info('Connected')
            else:
                 local_logger.critical(f'Not Connected - {conn_err_msg}')
            client = vco_client
            gateway_update_process(client, mycursor, VCO_CUSTOMER_EDGE)
            # local_logger.info(vco_list[vco]['link'])
  
