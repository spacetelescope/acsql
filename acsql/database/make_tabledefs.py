import glob
import os
from stak import Hselect
import numpy as np


def make_tabledefs(file_folder):
    """
    Function to auto-produce the table_definition files
    for acsql project.  Uses stak.Hselect.

    :param file_folder: string
        folder/s where files live
    :return:
    """
    file_types = {'jif': [0], 'jit': [0], 'flt': [0, 1, 2, 3], 'flc': [0,1,2,3],
                  'drz': [0], 'drc': [0], 'spt': [0], 'raw': [0], 'trl': [0]}
    for ftype in file_types:
        # Get filelist
        file_paths = os.path.join(file_folder, "*{}.fits".format(ftype))
        all_files = glob.glob(file_paths)
        print file_paths

        for ext in file_types[ftype]:
            file_name = "acs_{}_{}.txt".format(ftype, ext)
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
                        print "Couldn't find type match: {}:{}".format(
                            col.name, col.dtype)

                    f.write("{},  {}\n".format(col.name, ptype))

if __name__=="__main__":
    make_tabledefs("/user/bourque/acsql/*")