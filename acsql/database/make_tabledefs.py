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
#from stak import Hselect

from acsql.utils import utils
from acsql.utils.utils import SETTINGS


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

    for ftype in file_exts:

        all_files = glob.glob('table_definitions/test_files/test_{}_{}*.fits'\
            .format(detector, ftype))

        for ext in file_exts[ftype]:

            filename = 'table_definitions/{}_{}_{}.txt'.format(detector,
                ftype, ext)
            hsel = Hselect(all_files, '*', extension=(ext,))

            print('Making file {}'.format(filename))
            with open(filename, 'w') as f:
                for col in hsel.table.itercols():
                    if col.name in ['ROOTNAME', 'Filename', 'FILENAME', 'Ext']:
                        continue
                    elif col.dtype in [np.dtype('S68'), np.dtype('S80')]:
                        ptype = 'String'
                    elif col.dtype in [np.int64]:
                        ptype = 'Integer'
                    elif col.dtype in [bool]:
                        ptype = 'Bool'
                    elif col.dtype in [np.float64]:
                        ptype = 'Float'
                    else:
                        print('Could not find type match: {}:{}'.format(
                            col.name, col.dtype))

                    f.write('{}, {}\n'.format(col.name, ptype))


if __name__ == '__main__':

    make_tabledefs('wfc')
    make_tabledefs('sbc')
    make_tabledefs('hrc')
