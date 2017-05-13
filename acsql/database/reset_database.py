#! /usr/bin/env python

"""Reset all tables in the ``acsql`` database.

Authors
-------
    Matthew Bourque, 2017

Use
---
    This script is intended to be used in the command line:
    ::

        python reset_database.py

Dependencies
------------
    External library dependencies include:

    - ``acsql``
"""

from acsql.database.database_interface import base
from acsql.utils.utils import SETTINGS


if __name__ == '__main__':

    prompt = ('About to reset all tables for database instance {}. Do you '
              'wish to proceed? (y/n)\n'.format(SETTINGS['connection_string']))
    response = input(prompt)

    if response.lower() == 'y':
        print('Resetting database.')
        base.metadata.drop_all()
        base.metadata.create_all()
