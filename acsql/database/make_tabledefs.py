#! /usr/bin/env/ python

"""
"""

import glob
import os
from stak import Hselect
import numpy as np


def make_tabledefs(detector, file_folder):
    """
    Function to auto-produce the table_definition files
    for acsql project.  Uses stak.Hselect.

    Parameters
    ----------
    detector : str
        The detector (e.g. 'wfc').
    file_folder: str
        The directory to which the table definition file will be
        written.
    """
    file_types = {'jif': [0,1,2,3], 'jit': [0,1,2,3], 'flt': [0,1,2,3],
                  'flc': [0,1,2,3], 'drz': [0,1,2,3], 'drc': [0,1,2,3],
                  'raw': [0,1,2,3], 'crj': [0,1,2,3], 'crc': [0,1,2,3],
                  'spt': [0,1], 'asn': [0,1]}
    for ftype in file_types:
        # Get filelist
        file_paths = os.path.join(file_folder, 'test_files/',
                                  '*{}.fits'.format(ftype))
        all_files = glob.glob(file_paths)

        for ext in file_types[ftype]:
            file_name = "{}_{}_{}.txt".format(detector, ftype, ext)
            # Run hselect to gather datatypes for all keywords
            hsel = Hselect(all_files, '*', extension=(ext,))

            print("Making file {}".format(file_name))
            with open(file_name, 'w') as f:
                for col in hsel.table.itercols():
                    if col.dtype in [np.dtype('S68'), np.dtype('S80')]:
                        ptype = "String"
                    elif col.dtype in [np.int64]:
                        ptype = "Integer"
                    elif col.dtype in [bool]:
                        ptype = "Bool"
                    elif col.dtype in [np.float64]:
                        ptype = "Float"
                    else:
                        print("Couldn't find type match: {}:{}".format(
                            col.name, col.dtype))

                    f.write("{},  {}\n".format(col.name, ptype))

if __name__ == "__main__":
    make_tabledefs('wfc', 'table_definitions/')
