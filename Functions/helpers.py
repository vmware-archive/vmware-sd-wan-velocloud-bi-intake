"""

Copyright 2018-2020 VMware, Inc.
SPDX-License-Identifier: BSD-2-Clause

"""

import decimal
import logging
import logging.handlers
from datetime import datetime, timedelta, timezone
from Objects.Config import Config
from dateutil import parser
from typing import List, Dict, Tuple
import Functions.data_sanitization as ds
import traceback

c = decimal.getcontext().copy()
c.prec = 6
decimal.setcontext(c)


def snake_to_camel_case(snake_str: str) -> str:
    """
    We capitalize the first letter of each component except the first one
    with the 'title' method and join them together.
    """
    components = snake_str.split('_')
    return components[0] + ''.join(x.title() for x in components[1:])


def camel_to_snake_case(camel_str: str) -> str:
    """
    add _ before a capital letter then lowercase the entire string
    """
    return ''.join(['_' + i.lower() if i.isupper() else i for i in camel_str]).lstrip('_')


def bool_fix(some_obj) -> None:
    """
    Iterates through the non protected attributes of an object and translates True/False strings to their boolean and
    None String to None type
    Modifies the object in place
    """
    # ToDo: potentially make this account for lower case as well?
    for attr in dir(some_obj):
        if '__' not in attr:
            item = getattr(some_obj, attr)
            if item == 'True':
                setattr(some_obj, attr, True)
            if item == 'False':
                setattr(some_obj, attr, False)
            if item == 'None' or item == '':
                setattr(some_obj, attr, None)
    return


def fix_type(attr: str, data, annotations: Dict[str, str], item: object = None, sanitize_text: bool = False) -> any:
    """
    coerce the type of data based on annotations
    if an object is provided in the item parameter, we will automatically attempt to set the given attr on that object
    """
    anno = annotations[attr]
    if sanitize_text:
        if data is not None:
            rdata = ds.sanitize_text(data)
            if rdata:
                data = rdata.group(0)
            else:
                data = 'Invalid'
    if data in ['None', 'none', '', None]:
        data = None
    elif anno == 'datetime':
        try:
            data = parser.parse(data)
        except parser.ParserError:
            data = None
    elif anno == 'int':
        data = int(data)
    elif anno == 'float':
        data = float(data)
    elif anno == 'decimal.Decimal':
        data = c.create_decimal_from_float(data)
    elif anno == 'bool':
        if data in ['True', 'true', 1, '1']:
            data = True
        elif data in ['False', 'false', 0, '0']:
            data = False

    if item:
        setattr(item, attr, data)
    return data


def item_updated_recently(item: any, days_since_last_update: int):
    """
    Checks
    """
    date = (datetime.now(timezone.utc) - timedelta(days=days_since_last_update))

    if days_since_last_update > 1:
        days = 'days'
    else:
        days = 'day'
    if item.last_updated is None:
        t = f'Logical ID Doesnt exist in DB - Needs Update'
        v = None
    elif item.last_updated < date:
        t = f'Not updated in at least {days_since_last_update} {days} - {item.last_updated.strftime("%Y-%m-%d")}'
        v = False
    else:
        t = f'Updated in the last {days_since_last_update} {days} - {item.last_updated.strftime("%Y-%m-%d")}'
        v = True
    item.needs_update = v
    item.logger.info(t)
    return v


def update_config(config: Config, arg_list):
    if arg_list.logging_file:
        config.files.logging = arg_list.logging_file
    if arg_list.vco_list_file:
        config.files.vco_list = arg_list.vco_list_file
    if arg_list.inactive_cust_vco:
        config.files.inactive_cust_vco = arg_list.inactive_cust_vco
    return


def copy_obj_attributes(from_obj, to_obj, attributes: List[str], debug: bool = False) -> Tuple[bool, str]:
    """
    Iterates through the attributes list and updates changed values from the from_obj to the to_obj

    :param from_obj: Object the values will come from
    :param to_obj: Object the values will be copied to
    :param attributes: List of Attributes you want to copy
    :param debug: enables printing of attribute: from: and to: values
    :return: Text for debug logging
    """
    debug_text: str = 'att: value'
    changed = False
    for att in attributes:
        from_obj_value = getattr(from_obj, att)
        to_obj_value = getattr(to_obj, att)
        if type(from_obj_value) == 'float':
            from_obj_value = round(from_obj_value, 3)
        if type(to_obj_value) == 'float':
            to_obj_value = round(to_obj_value, 3)
        if debug:
            print(f'{att}: from: {from_obj_value} - to: {to_obj_value}')
        if to_obj_value != from_obj_value:
            changed = True
            setattr(to_obj, att, from_obj_value)
            debug_text = f'{debug_text} - {att}: {from_obj_value}'
    return changed, debug_text


def setup_logging(log_file: str = None, log_name: str = 'MAIN', file_level: str = 'INFO', console_level: str = 'INFO',
                  level: str = 'INFO', debug: bool = False):
    """
    :param log_file: path to log file
    :param log_name: base name for logger
    :param file_level: Level for logging to file
    :param console_level:
    :param debug: whether or not to log to console
    :param level:
    :return:
    """

    file_level = getattr(logging, file_level)
    console_level = getattr(logging, console_level)
    level = getattr(logging, level)

    local_logger = logging.getLogger(log_name)
    local_logger.setLevel(level)

    # create a logging format
    log_format = '%(asctime)s - %(levelname)s - %(funcName)s:%(lineno)s - %(name)s - %(message)s'
    formatter = logging.Formatter(log_format)

    # create a file handler
    if log_file:
        f_handler = logging.FileHandler(log_file)
        f_handler.setLevel(file_level)
        f_handler.setFormatter(formatter)
        local_logger.addHandler(f_handler)

    # THIS CODE ADDS LOGGER TO CONSOLE NOT SUPER USEFUL WITH THREADING
    if debug:
        console = logging.StreamHandler()
        console.setLevel(console_level)
        console.setFormatter(formatter)
        local_logger.addHandler(console)

    return local_logger


def log_critical_error(ex, log_name: str = 'main'):
    logger = logging.LoggerAdapter(logging.getLogger('MAIN'), {'VCO_CUSTOMER_EDGE': log_name})
    ex_traceback = ex.__traceback__
    tb_lines = [line.replace('\n', '') for line in traceback.format_exception(ex.__class__, ex, ex_traceback)]
    tb_lines.pop(0)
    logger.critical(tb_lines)
