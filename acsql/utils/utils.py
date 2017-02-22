"""This module contains a function that reads in the acsql config file
and returns its settings.

Authors
-------
    Matthew Bourque, 2017

Use
---

    The functions within this module are intened to be imported by
    various acsql modules and scripts, as such:

    from acsql.utils.utils import SETTINGS

Dependencies
------------
    External library dependencies include:

    (1) sqlalchemy
"""

import os
import yaml

__config__ = os.path.realpath(os.path.join(os.getcwd(),
                                           os.path.dirname(__file__)))


def get_settings():
    """Returns the settings that are located in the acsql config file.

    Returns
    -------
    settings : dict
        A dictionary with setting key/value pairs.
    """

    with open(os.path.join(__config__, 'config.yaml'), 'r') as f:
        settings = yaml.load(f)

    return settings


SETTINGS = get_settings()
