#! /usr/bin/env python

"""

"""

import glob
import os

from acsql.utils.utils import SETTINGS


def get_files_by_filetype(filetype):
    """Return a list of files in the MAST cache that match the given
    filetype.

    Parameters
    ----------
    filetype : str
        The filetype (e.g. ``raw``, ``flt``, etc.)

    Returns
    -------
    files : list
        A list of files in the MAST cache of the given filetype.
    """

    print('Searching for {} files'.format(filetype))
    search = os.path.join(SETTINGS['filesystem'], 'jd*', '*', '*{}.fits'.format(filetype))
    files = glob.glob(search)

    return files


if __name__ == '__main__':

    # Initialize plot

    filetypes = ['raw', 'flt', 'flc', 'drz', 'drc', 'spt', 'jit', 'jif', 'crj', 'crc', 'asn']

    for filetype in filetypes:
        files = get_files_by_filetype(filetype)

        print(len(files))