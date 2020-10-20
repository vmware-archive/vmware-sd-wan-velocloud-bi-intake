"""

Copyright 2018-2020 VMware, Inc.
SPDX-License-Identifier: BSD-2-Clause

"""

import re
from typing import List, Dict, Union


def sanitize_text(some_string: str, encoding: str = 'latin1'):
    """
    Encode the string with given encoding to strip out characters that aren't compatible with the database
    If resulting string is empty, set it to Invalid
    Valid encodings can be found here: https://docs.python.org/3/library/codecs.html#standard-encodings
    :param some_string: String to be santized
    :param encoding: Encoding to use for
    :return:
    """

    if some_string is None:
        return None
    byte_string = some_string.encode(encoding=encoding, errors='ignore')
    clean_string = byte_string.decode(encoding=encoding, errors='ignore')
    if len(clean_string) == 0:
        clean_string = 'Invalid'
    return clean_string


def clean_customers(customer_list: List[Dict[str, any]], vco_name: str,
                    moved_customers: Dict[str, List[str]] = None, arg_customer: int = None) -> List[Dict[str, any]]:
    """
    Create a new customer list given a raw customer list from a VCO
    Default action is to add all customers to a new list and return the new list
    If passed a moved_customers list, it will remove any customers who have been moved to a new VCO
    If passed an arg customer it will return a list with only that customer in it -
        or an empty list if a moved_customer list has been provided and the customer has been moved
    :param customer_list: Raw customer list from the VCO
    :param vco_name: VCO Name for matching with the moved_customer list
    :param moved_customers: Dict of customer IDs and the VCOs they were moved from
    :param arg_customer: Customer ID of customer that we want to run by themselves
    :return: List of customers we want to run after being cleaned
    """

    new_customer_list = []
    for customer in customer_list:
        customer_logical_id = customer.get('logicalId')

        # Logic for just running a single customer - If this customer id is not the arg customer, continue
        if arg_customer:
            if int(arg_customer) != int(customer.get('id')):
                continue

        # Ignore customers on the ignore customer list if this VCO is in their old VCO List
        if moved_customers is not None:
            if customer_logical_id in moved_customers:
                if vco_name in moved_customers.get(customer_logical_id):
                    continue

        new_customer_list.append(customer)
    return new_customer_list
