#! /usr/bin/env python

"""Updates the ``table_definitions`` text files that store header
keyword/data type pairs (see ``make_tabledefs.py`` module documentation
for further details).

A given ``logfile`` is used to scrape for logging ``WARNINGS`` that
warn of missing ``acsql`` database columns (i.e. header keywords that
exist in the file header but does not exist as a database column in the
appropriate header table).  The new header keywords are appended to the
appropriate ``table_definitions`` text file based on the ``detector``,
``filetype`` and ``extension`` (e.g. ``WFC_raw_0.txt``).

Additionally, the ``ALTER TABLE`` commands needed to add the
corresponding columns are printed to the standard output.  ``acsql``
users can then use these commands within ``MySQL`` to add the
appropriate columns.

Authors
-------
    Matthew Bourque

Use
---
    This module is intended to be used when an ``acsql`` user sees fit
    to add any new header keywords to the database.  The module can
    be used from the command line as such:
    ::

        python update_tabledefs.py <logfile>

    Required arguments:
    ::

        logfile: The path to an ``acsql.ingest.ingest.py`` log to be
            used to determine new header keywords.

Dependencies
------------
    External library dependencies include:

    - ``acsql``
    - ``numpy``
    - ``stak`` (``https://github.com/spacetelescope/stak``)

Notes
-----
    The ``stak.Hselect`` dependency still depends on Python 2 at the
    time of this writing.
"""

import argparse
import os

import numpy as np
from stak import Hselect

from acsql.utils.utils import SETTINGS


def parse_args():
    """Parse command line arguments. Returns ``args`` object

    Returns
    -------
    args : obj
        An argparse object containing all of the arguments
    """

    # Create help strings
    logfile_help = 'The path to the logfile to use to determine new header '
    logfile_help += 'keywords.'

    # Add arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('logfile',
                        action='store',
                        help=logfile_help)

    # Parse args
    args = parser.parse_args()

    # Ensure that the logfile exists
    assert os.path.exists(args.logfile), '{} does not exist.'\
        .format(args.logfile)

    return args


def update_tabledefs(logfile):
    """The main function of the ``update_tabledefs`` module.  See
    module documentation for further details.

    Parameters
    ----------
    logfile : str
        The path to an ``acsql.ingest.ingest.py`` log to be used to
        determine new header keywords.
    """

    # Read in the logfile
    with open(logfile) as f:
        data = f.readlines()

    # Grab out the appropriate WARNING lines
    warnings = [item.strip() for item in data if 'not in' in item]
    warnings = [item.split(':') for item in warnings]

    # Determine metadata for each warning
    rootnames = [item[-2].strip() for item in warnings]
    keywords = [item[-1].strip().split(' not in')[0] for item in warnings]
    tables = [item[-1].strip().split('not in ')[-1] for item in warnings]
    filetypes = [item.split('_')[1] for item in tables]
    paths = [os.path.join(SETTINGS['filesystem'], rootname[0:4], rootname,
             '{}_{}.fits'.format(rootname, filetype)) for rootname, filetype
             in zip(rootnames, filetypes)]

    # Take a set of the warnings to avoid duplications
    files_to_test = ['{},{},{}'.format(path, keyword, table) for path,
                     keyword, table in zip(paths, keywords, tables)]
    keywords_to_add = set([(keyword, table) for keyword, table in zip(keywords,
                                                                      tables)])

    # For each keyword to add, use test file to determine data type
    command_list = []
    for keyword, table in keywords_to_add:
        test_file = [f for f in files_to_test if '{},{}'
                     .format(keyword, table) in f][0]
        test_file = test_file.split(',')[0]

        # Use hselect to determine data type
        try:
            hsel = Hselect(test_file, keyword, extension=(int(table[-1]),))
            dtype = hsel.table[keyword].dtype
        except:
            print('Cannot determine datatype for {}. Defaulting to String'
                  .format(keyword))
            dtype = np.dtype('S80')

        if dtype in [np.dtype('S68'), np.dtype('S80')]:
            col_type = 'String'
            db_type = 'VARCHAR(50)'
        elif dtype in [np.int64]:
            col_type = 'Integer'
            db_type = 'INTEGER'
        elif dtype in [bool]:
            col_type = 'Bool'
            db_type = 'BOOLEAN'
        elif dtype in [np.float64]:
            col_type = 'Float'
            db_type = 'FLOAT'
        else:
            print('Could not find type match: {}:{}'.format(keyword, dtype))

        # Update the appropriate table definitions file
        tabledefs_file = 'table_definitions/{}.txt'.format(table.lower())
        with open(tabledefs_file, 'r') as f:
            data = f.readlines()
        existing_keywords = [item.strip().split(',')[0] for item in data]
        with open(tabledefs_file, 'a') as f:
            if keyword not in existing_keywords:
                f.write('{}, {}\n'.format(keyword, col_type))
        print('Updated {} with {}'.format(tabledefs_file, keyword))

        # Add ALTER TABLE command to list of commands
        command = 'ALTER TABLE {} ADD {} {};'.format(table.lower(),
                                                    keyword.lower(), db_type)
        command_list.append(command)

    # Print out the ALTER TABLE commands
    print('\n\nALTER TABLE commands to execute for database:\n')
    for command in command_list:
        print(command)

if __name__ == '__main__':

    args = parse_args()
    update_tabledefs(args.logfile)
