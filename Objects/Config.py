"""

Copyright 2018-2020 VMware, Inc.
SPDX-License-Identifier: BSD-2-Clause

"""

from typing import Optional

import yaml


class Config:
    def __init__(self, *, cfg: str) -> None:
        """
        Create Config Object from a config yaml file.
        Create sub objects for each section of the config file and parse automatically
        Config section headers should be represented here as an attribute in snake case with an appropriate section
        object as the value for the attribute
        IE: MYSQL_DEV in the config file becomes self.mysql_dev and is a SectSQL type
        """
        with open(cfg) as f:
            self.raw_cfg = yaml.load(f, Loader=yaml.BaseLoader)
        self.mysql_prod = SectSQL()
        self.files = SectFiles()
        self.slack = SectSlack()
        self.maxmind = SectMaxMind()

    def __repr__(self):
        return "{}({!r})".format(self.__class__.__name__, self.__dict__)

    def parse_config(self) -> None:
        """
        Parse the config file and add the information to the appropriate section
        Automatically converts ints if the original section attribute value is an int
        Converts '' to None, and True/False strings to Boolean values
        """
        # ToDO add support for Lists, Dicts, Datetimes, and Bool
        for s_name, sect_info in self.raw_cfg.items():
            sect = getattr(self, s_name.lower())
            for a_name, attribute in sect_info.items():
                if type(getattr(sect, a_name.lower())) == int:
                    setattr(sect, a_name.lower(), int(attribute))
                else:
                    setattr(sect, a_name.lower(), attribute)
        self.raw_cfg = None
        return


class Sect(object):
    def __init__(self):
        """
        Base object for config sections. Having these allows code auto complete and calling . instead of _ formats
        Create as Sect{section identifier} and attributes should be the same as the config attributes in the config file
        IE: using the MYSQL_DEV in the config file, there is user/password/host/port/db, all of those should appear in
        as attributes of the Object.
        For ints use an int as a default value and the parsing will automatically change it to an int
        """
        return

    def __repr__(self):
        return "{}({!r})".format(self.__class__.__name__, self.__dict__)


class SectSQL(Sect):
    def __init__(self) -> None:
        super().__init__()
        # Section to make a connection to a MYSQL Database
        self.host: Optional[str] = None
        self.port: int = 0
        self.user: Optional[str] = None
        self.password: Optional[str] = None
        self.db: Optional[str] = None
        return


class SectFiles(Sect):
    def __init__(self) -> None:
        super().__init__()
        # Static and config file locations
        self.logging: Optional[str] = None
        self.vco_list: Optional[str] = None
        self.countries: Optional[str] = None
        return


class SectSlack(Sect):
    def __init__(self) -> None:
        super().__init__()
        # Slack Config Options
        self.url: Optional[str] = None
        return


class SectMaxMind(Sect):
    def __init__(self) -> None:
        super().__init__()
        # Slack Config Options
        self.account_id: Optional[str] = None
        self.license_key: Optional[str] = None
        return
