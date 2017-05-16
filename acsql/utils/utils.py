"""This module contains several functions that are useful to various
modules within the ``acsql`` package.  See individual function
docstrings for further information.

Authors
-------
    Matthew Bourque

Use
---

    The functions within this module are intened to be imported by
    various acsql modules and scripts, as such:
    ::

        from acsql.utils.utils import insert_or_update
        from acsql.utils.utils import SETTINGS
        from acsql.utils.utils import setup_logging

    There also exists static importable data:
    ::

        from acsql.utils.utils import FILE_EXTS
        from acsql.utils.utils import TABLE_DEFS

Dependencies
------------
    External library dependencies include:

    - ``acsql``
    - ``astropy``
    - ``numpy``
    - ``sqlalchemy``
"""

import datetime
import getpass
import glob
import logging
import os
import socket
import sys
import yaml

import astropy
import numpy
import sqlalchemy
from sqlalchemy import Table
from sqlalchemy.exc import DataError
from sqlalchemy.exc import IntegrityError
from sqlalchemy.exc import InternalError

import acsql

__config__ = os.path.realpath(os.path.join(os.getcwd(),
                                           os.path.dirname(__file__)))

# Define possible detector/filetype/extension combinations
WFC_FILE_EXTS = {'jif': [0, 1, 2, 3, 4, 5, 6],
                 'jit': [0, 1, 2, 3, 4, 5, 6],
                 'flt': [0, 1, 2, 3, 4, 5, 6],
                 'flc': [0, 1, 2, 3, 4, 5, 6],
                 'drz': [0, 1, 2, 3],
                 'drc': [0, 1, 2, 3],
                 'raw': [0, 1, 2, 3, 4, 5, 6],
                 'crj': [0, 1, 2, 3, 4, 5, 6],
                 'crc': [0, 1, 2, 3, 4, 5, 6],
                 'spt': [0, 1],
                 'asn': [0, 1]}

SBC_FILE_EXTS = {'jif': [0, 1, 2],
                 'jit': [0, 1, 2],
                 'flt': [0, 1, 2, 3],
                 'drz': [0, 1, 2, 3],
                 'raw': [0, 1, 2, 3],
                 'spt': [0, 1],
                 'asn': [0, 1]}

HRC_FILE_EXTS = {'jif': [0, 1, 2],
                 'jit': [0, 1, 2],
                 'flt': [0, 1, 2, 3],
                 'drz': [0, 1, 2, 3],
                 'raw': [0, 1, 2, 3],
                 'crj': [0, 1, 2, 3],
                 'spt': [0, 1],
                 'asn': [0, 1]}

# Define ingestable filetypes
VALID_FILETYPES = ['jif', 'jit', 'flt', 'flc', 'drz', 'drc', 'raw', 'crj',
                   'crc', 'spt', 'asn']

# Define value proposal types
VALID_PROPOSAL_TYPES = ['CAL/ACS', 'CAL/OTA', 'CAL/STIS', 'CAL/WFC3',
                        'ENG/ACS', 'GO', 'GO/DD', 'GO/PAR', 'GTO/ACS',
                        'GTO/COS', 'NASA', 'SM3/ACS', 'SM3/ERO', 'SM4/ACS',
                        'SM4/COS', 'SM4/ERO', 'SNAP']


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


def setup_logging(module):
    """Configures a log file that logs the execution of the given
    module.  Log files are written to the log_dir that is set in the
    config.yaml configuration file.  The filename of the log file is
    <module>_<timestamp>.log.

    Parameters
    ----------
    module : str
        The name of the module to log.
    """

    SETTINGS = get_settings()

    # Configure logging
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M')
    filename = '{0}_{1}.log'.format(module, timestamp)
    logfile = os.path.join(SETTINGS['log_dir'], filename)
    logging.basicConfig(
        filename=logfile,
        format='%(asctime)s %(levelname)s: %(message)s',
        datefmt='%m/%d/%Y %H:%M:%S',
        level=logging.INFO)

    # Log environment information
    logging.info('User: {0}'.format(getpass.getuser()))
    logging.info('System: {0}'.format(socket.gethostname()))
    logging.info('Python Version: {0}'.format(sys.version.replace('\n', '')))
    logging.info('Python Path: {0}'.format(sys.executable))
    logging.info('Numpy Version: {0}'.format(numpy.__version__))
    logging.info('Numpy Path: {0}'.format(numpy.__path__[0]))
    logging.info('Astropy Version: {0}'.format(astropy.__version__))
    logging.info('Astropy Path: {0}'.format(astropy.__path__[0]))
    logging.info('SQLAlchemy Version: {0}'.format(sqlalchemy.__version__))
    logging.info('SQLAlchemy Path: {0}'.format(sqlalchemy.__path__[0]))


def get_table_defs():
    """Return a dictionary containing the columns for each database
    table, as taken from the table_definition text files.

    Returns
    -------
    table_defs : dict
        A dictionary whose keys are detector/file_type/extension
        configurations (e.g. 'wfc_flt_0') and whose values are lists
        of column names for the corresponding table.
    """

    # Get table definition files
    table_def_directory = os.path.realpath(os.path.join(os.getcwd(),
                                           os.path.dirname(__file__)))
    table_def_directory = table_def_directory.replace('utils', 'database/table_definitions/')
    table_def_files = glob.glob(os.path.join(table_def_directory, '*.txt'))

    table_defs = {}

    for table_def_file in table_def_files:

        configuration = os.path.basename(table_def_file).split('.txt')[0]
        with open(table_def_file, 'r') as f:
            contents = f.readlines()
        contents = [item.strip() for item in contents]
        columns = [item.split(',')[0] for item in contents]
        table_defs[configuration] = columns

    return table_defs

TABLE_DEFS = get_table_defs()


def insert_or_update(table, data_dict):
    """Insert or update a record in the given ``table`` with the data
    in the ``data_dict``.

    A record is inserted if the primary key of the record does not
    already exist in the ``table``.  A record is updated if it does
    already exist.

    Parameters
    ----------
    table : str
        The name of the table to insert/update into.
    data_dict : dict
        A dictionary containing the data to insert/update.
    """

    table_obj = getattr(acsql.database.database_interface, table)
    session, base, engine = acsql.database.database_interface.\
        load_connection(SETTINGS['connection_string'])

    # Check to see if a record exists for the rootname
    query = session.query(table_obj)\
        .filter(getattr(table_obj, 'rootname') == data_dict['rootname'])
    query_count = query.count()

    # If there are no results, then perform an insert
    if not query_count:
        tab = Table(table.lower(), base.metadata, autoload=True)
        insert_obj = tab.insert()
        try:
            insert_obj.execute(data_dict)
        except (DataError, IntegrityError, InternalError) as e:
            logging.warning('\tUnable to insert {} into {}: {}'.format(
                            data_dict['rootname'], table, e))

    else:
        query.update(data_dict)

    session.commit()
    session.close()
    engine.dispose()
