#! /usr/bin/env python

"""
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
    assert os.path.exists(args.logfile), '{} does not exist.'.format(args.logfile)

    return args


def update_tabledefs(logfile):
    """
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
    detectors = [item.split('_')[0] for item in tables]
    filetypes = [item.split('_')[1] for item in tables]
    paths = [os.path.join(SETTINGS['filesystem'], rootname[0:4], rootname,
             '{}_{}.fits'.format(rootname, filetype)) for rootname, filetype
             in zip(rootnames, filetypes)]

    # Take a set of the warnings to avoid duplications
    files_to_test = ['{},{},{}'.format(path, keyword, table) for path, keyword, table in zip(paths, keywords, tables)]
    keywords_to_add = set([(keyword, table) for keyword, table in zip(keywords, tables)])

    # For each keyword to add, use test file to determine data type
    for keyword, table in keywords_to_add:
        test_file = [f for f in files_to_test if '{},{}'.format(keyword,table) in f][0]
        test_file = test_file.split(',')[0]

        # Use hselect to determine data type
        try:
            hsel = Hselect(test_file, keyword, extension=(int(table[-1]),))
            dtype = hsel.table[keyword].dtype
        except:
            print('Cannot determine datatype for {}. Defaulting to String'.format(keyword))
            dtype = np.dtype('S80')

        if dtype in [np.dtype('S68'), np.dtype('S80')]:
            col_type = 'String'
        elif dtype in [np.int64]:
            col_type = 'Integer'
        elif dtype in [bool]:
            col_type = 'Bool'
        elif dtype in [np.float64]:
            col_type = 'Float'
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

if __name__ == '__main__':

    args = parse_args()
    update_tabledefs(args.logfile)
