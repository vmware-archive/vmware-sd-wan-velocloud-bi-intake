"""

Copyright 2018-2020 VMware, Inc.
SPDX-License-Identifier: BSD-2-Clause

With a properly authenticated VCO Connection, these calls MUST NOT return any errors to the caller
All data calls MUST return a tuple
    On successful calls:
        First item MUST be the object/s requested
        Second item SHOULD be a None type object or a string
    On failed calls:
        First item MUST be a None type object
        Second item MUST be the reason for the failure
Data fields SHOULD be sanitized before returning data
Sanitization functions SHOULD live in the sanitization module
    This saves sanitization duplication in other sections
Sanitization MUST NOT raise expected exceptions
Sanitization MUST NOT use a naked except
    This allows only unexpected errors to bubble up
    If you get an unexpected error downstream of the data call, apply or fix sanitization function
Type hinting MUST be used on all parameters and returns

"""

import json
from datetime import timedelta, datetime, timezone
from time import sleep
from typing import Optional, Dict, List, Tuple

import requests
from requests.exceptions import ConnectTimeout, ReadTimeout
from requests.packages.urllib3.exceptions import InsecureRequestWarning

from Functions.data_sanitization import sanitize_text
from VCOClient import VcoRequestManager, ApiException

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

def call_api_wrapper(vco_client: VcoRequestManager, call_name: str, method: str, params: dict, timeout: int = None,
                     sleep_duration: float = .5) -> Tuple[Optional[any], Optional[str]]:
    """
    function for standard error and return handling
    :param vco_client:
    :param call_name:
    :param method:
    :param params:
    :param timeout:
    :param sleep_duration:
    :return:
    """
    if timeout:
        original_timeout = vco_client.timeout
        vco_client.timeout = timeout

    data = None
    msg = None

    try:
        data = vco_client.call_api(method, params)
    except (ConnectTimeout, ReadTimeout):
        msg = 'Connection Timed Out'
    except ApiException as e:
        if str(e) == 'methodError':
            msg = f'{call_name} Failed - VCO doesnt support it or bad method'
        elif 'privilege' in str(e):
            msg = f'{call_name} Failed - lack the necessary privileges for this customer'
        else:
            msg = f'Critical Unexpected Failure in {call_name} - {str(e)}'
    except json.decoder.JSONDecodeError:
        msg = f'{call_name} generated an error at the server'

    # Sleep Always? or only after a successful Call?
    sleep(sleep_duration)

    if timeout:
        vco_client.timeout = original_timeout

    return data, msg


# region VCO_Specific_calls


def connect_to_vco(vco: Dict[str, any]) -> Tuple[Optional[VcoRequestManager], Optional[str]]:
    vco_client = VcoRequestManager(vco['link'], verify_ssl=False, timeout=3)

    try:
        token = vco.get('token')
        if token:
            vco_client.update_token(vco.get('token'))
        else:
            vco_client.authenticate(vco.get('username'), vco.get('password'), is_operator=True)
        _get_vco_public_address(vco_client=vco_client)
        return vco_client, None
    except (ConnectTimeout, ReadTimeout):
        return None, 'Connection Timed Out'
    except ApiException as e:
        if 'tokenError' in str(e):
            return None, str(e)
        else:
            raise e


def _get_vco_public_address(vco_client: VcoRequestManager) -> bool:
    """
    Using this as a test for API connectivity to see if credentials work
    Purposefully not catching errors so they can be caught by the connect phase
    """
    params = {'name': 'network.public.address'}
    public_address = vco_client.call_api('/systemProperty/getSystemProperty', params)['value']
    if public_address:
        return True
    else:
        return False


def get_vco_version(vco_client: VcoRequestManager) -> Tuple[Optional[str], Optional[str]]:
    """
    Return VCO Version
    """
    params = {'name': 'product.version'}
    method = '/systemProperty/getSystemProperty'
    call_name = 'getSystemProperty:Version'
    data, msg = call_api_wrapper(vco_client=vco_client, call_name=call_name, method=method, params=params)
    data = data.get('value')
    return data, msg


def get_system_properties(vco_client: VcoRequestManager) -> Tuple[Optional[List[Dict[str, any]]], Optional[str]]:
    """
    Return all system properties
    """
    params = {}
    method = '/systemProperty/getSystemProperties'
    call_name = 'getSystemProperties'
    data, msg = call_api_wrapper(vco_client=vco_client, call_name=call_name, method=method, params=params)
    return data, msg


def get_vco_customers(vco_client: VcoRequestManager, timeout: int = 300) -> Tuple[
    Optional[List[Dict[str, any]]], Optional[str]]:
    """
    Return list of VCO Customers after sanitizing both name and enterpriseProxyName (partner name)
    :param vco_client: Authenticated VcoRequestManager instance that can be used to make the call
    :param timeout: amount of time to wait for VCO call to complete
    :return: List of VCO Customers
    """
    params = {'with': ['edgeCount', 'edgeConfigUpdate']}
    method = '/network/getNetworkEnterprises'
    call_name = 'getNetworkEnterprises'
    data, msg = call_api_wrapper(vco_client=vco_client, call_name=call_name, method=method, params=params,
                                 timeout=timeout)

    if data:
        for customer in data:
            customer['name'] = sanitize_text(customer.get('name'))
            customer['enterpriseProxyName'] = sanitize_text(customer.get('enterpriseProxyName'))

    return data, msg


# endregion
# region Customer_Specific_Calls
# Should reference a customer


def get_ent_route_table(vco_client: VcoRequestManager, customer_id: int, timeout: int = 300) -> Tuple[
    Optional[Dict[str, List[Dict[str, any]]]], Optional[str]]:
    params = {'enterpriseId': customer_id}
    method = '/enterprise/getEnterpriseRouteTable'
    call_name = 'getEnterpriseRouteTable'
    data, msg = call_api_wrapper(vco_client=vco_client, call_name=call_name, method=method, params=params,
                                 timeout=timeout)

    return data, msg


def get_customer_edges(vco_client: VcoRequestManager, customer_id: int, timeout: int = 300) -> Tuple[
    Optional[List[Dict[str, any]]], Optional[str]]:
    # TODO: check if all VCO Versions of 3.2.2 and higher return licenses even if not enabled then switch this to use
    #  vco_version

    method = '/enterprise/getEnterpriseEdges'
    call_name = 'getEnterpriseEdges'
    params = {'enterpriseId': customer_id, 'with': ['site', 'configuration', 'recentLinks', 'licenses', 'vnfs']}
    data, msg = call_api_wrapper(vco_client=vco_client, call_name=call_name, method=method, params=params,
                                 timeout=timeout)
    if data is None:
        params = {'enterpriseId': customer_id, 'with': ['site', 'configuration', 'recentLinks', 'vnfs']}
        data, msg = call_api_wrapper(vco_client=vco_client, call_name=call_name, method=method, params=params)
        if data:
            msg = f'getEnterpriseEdges with licenses failed - got without license'
        sleep(0.5)

    if data:
        for edge in data:
            edge['name'] = sanitize_text(edge.get('name'))

    return data, msg


def get_customer_users(vco_client: VcoRequestManager, customer_id: int, timeout: int = 200) -> Tuple[
    Optional[List[Dict[str, any]]], Optional[str]]:
    method = '/enterprise/getEnterpriseUsers'
    call_name = 'getEnterpriseUsers'
    params = {'enterpriseId': customer_id}
    data, msg = call_api_wrapper(vco_client=vco_client, call_name=call_name, method=method, params=params,
                                 timeout=timeout)

    return data, msg


def check_privilege_level(vco_client: VcoRequestManager, customer_id: int) -> Tuple[Optional[bool], Optional[str]]:
    """

    :param vco_client:
    :param customer_id:
    :return:
    """

    privilege_level = True
    method = '/role/getEnterpriseDelegatedPrivileges'
    call_name = 'getEnterpriseDelegatedPrivileges'
    params = {'enterpriseId': customer_id}
    data, msg = call_api_wrapper(vco_client=vco_client, call_name=call_name, method=method, params=params)

    if data:
        if len(data) == 0:
            privilege_level = None
            msg = 'No Privileges'
        for privilege in data:
            if privilege.get('isDeny') == 1:
                privilege_level = False
                msg = f'We are missing permissions on this customer - {privilege}'
    else:
        privilege_level = None
    return privilege_level, msg


def get_ent_services(vco_client: VcoRequestManager, customer_id: int) -> Tuple[
    Optional[List[Dict[str, any]]], Optional[str]]:
    params = {'enterpriseId': customer_id, 'with': ['configuration', 'profileCount', 'edgeUsage']}
    method = '/enterprise/getEnterpriseServices'
    call_name = 'getEnterpriseServices'
    data, msg = call_api_wrapper(vco_client=vco_client, call_name=call_name, method=method, params=params)
    return data, msg


def get_ent_configs(vco_client: VcoRequestManager, customer_id: int) -> Tuple[
    Optional[List[Dict[str, any]]], Optional[str]]:
    params = {'enterpriseId': customer_id, 'with': ['edgeCount', 'modules', 'refs']}
    method = '/enterprise/getEnterpriseConfigurations'
    call_name = 'getEnterpriseConfigurations'
    data, msg = call_api_wrapper(vco_client=vco_client, call_name=call_name, method=method, params=params)
    return data, msg


def get_identifiable_applications(vco_client: VcoRequestManager, customer_id: int) -> Tuple[
    Optional[List[Dict[str, any]]], Optional[str]]:
    params = {'enterpriseId': customer_id}
    method = '/enterprise/getIdentifiableApplications'
    call_name = 'getIdentifiableApplications'
    data, msg = call_api_wrapper(vco_client=vco_client, call_name=call_name, method=method, params=params)
    return data, msg


# endregion
# region Edge_Specific_Calls


def get_edge_config_stack(vco_client: VcoRequestManager, customer_id: int, edge_id: str) -> Tuple[
    Optional[List[Dict[str, any]]], Optional[str]]:
    params = {'enterpriseId': customer_id, 'edgeId': edge_id, 'with': ['modules']}
    method = '/edge/getEdgeConfigurationStack'
    call_name = 'getEdgeConfigurationStack'
    data, msg = call_api_wrapper(vco_client=vco_client, call_name=call_name, method=method, params=params)
    return data, msg


def get_edge_device_metrics(vco_client: VcoRequestManager, edge_id: int, customer_id: int):
    start = round((datetime.now(tz=timezone.utc) - timedelta(minutes=15)).timestamp()) * 1000

    params = {'edgeId': edge_id, 'limit': 100, 'enterpriseId': customer_id, 'interval': {'start': start},
              'metrics': ['packetsRx', 'packetsTx']}
    method = '/metrics/getEdgeDeviceMetrics'
    call_name = 'getEdgeDeviceMetrics'
    data, msg = call_api_wrapper(vco_client=vco_client, call_name=call_name, method=method, params=params)
    return data, msg

# endregion
