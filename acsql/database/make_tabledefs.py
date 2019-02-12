#! /usr/bin/env/ python

"""Creates static text files that hold header keywords and keyword
data types for each ACS filetype (and each extension).  Each text file
corresponds to a header table in the ``acsql`` database.

Authors
-------
    - Sara Ogaz
    - Matthew Bourque

Use
---
    This module is intended to be run via the command line as such:
    ::

        python make_tabledefs.py

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

import glob
import os

import numpy as np

from acsql.utils import utils
from acsql.utils.utils import get_files_with_ftype
from acsql.utils.utils import filter_files_by_size
from acsql.utils.utils import build_keyword_list


def make_tabledefs(detector):
    """
    Function to auto-produce the table_definition files.

    Note that due to how ``hselect`` handles ``ASN`` files, they must
    be handeled separately.

    Parameters
    ----------
    detector : str
        The detector (e.g. ``wfc``).
    """

    file_exts = getattr(utils, '{}_FILE_EXTS'.format(detector.upper()))
    foreign_keys = getattr(utils, 'FOREIGN_KEY_HEADER_KEYWORDS')
    skippable_keys = getattr(utils, 'SKIPPABLE_HEADER_KEYWORDS')
    drizzle_exp = getattr(utils, 'DRIZZLE_EXP')
    local_dir = os.path.realpath(os.path.dirname(__file__))
    table_def_dir = os.path.join(local_dir, 'table_definitions')
    file_dir = getattr(utils, "SETTINGS")['filesystem']

    for ftype in file_exts:
        
        all_files = get_files_with_ftype(ftype)

        for ext in file_exts[ftype]:
        
            all_files = filter_files_by_size(all_files, ext)

            filename = '{}_{}_{}.txt'.format(detector, ftype, ext)
            key_list = build_keyword_list(all_files, ext)

            print('Making file {}'.format(filename))
            with open(os.path.join(table_def_dir, filename), 'w') as f:
                for keyword,keytype in key_list:
                    if keyword in foreign_keys:
                        continue
                    elif keyword in skippable_keys:
                        continue
                    elif drizzle_exp.match(keyword) is not None:
                        continue
                    elif keytype == str:
                        ptype = 'String'
                    elif keytype == int:
                        ptype = 'Integer'
                    elif keytype == bool:
                        ptype = 'Bool'
                    elif keytype == float:
                        ptype = 'Float'
                    else:
                        error_str = 'Could not find type match: {}:{}'
                        print(error_str.format(keyword, keytype))

                    # If the column has a hyphen, switch it to underscore
                    if '-' in keyword:
                        keyword = keyword.replace('-', '_')

                    f.write('{}, {}\n'.format(keyword, ptype))


if __name__ == '__main__':

    make_tabledefs('wfc')
    make_tabledefs('sbc')
    make_tabledefs('hrc')
