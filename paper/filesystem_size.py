#! /usr/bin/env python

"""Create a data file that contains the date of each observation along
with the observation's total file size and detector.

The total file size of an observation is determined by aggregating the
individual file sizes of each filetype in the ``acsql`` filesystem.
The data are written to the file ``file_sizes.dat`` in the ``figures``
directory.  See the ``plot_file_sizes.py`` script for plotting the
data.

This script using parallel processing over multiple cores.  It is
recommended that the user execute this script on a server.

Authors
-------

    Matthew Bourque

Use
---
    This script is intended to be executed via the command line as
    such:
    ::

        python filesystem_size.py

Dependencies
------------

    - ``acsql``
"""

import glob
import multiprocessing
import os

from acsql.database.database_interface import session
from acsql.database.database_interface import WFC_raw_0
from acsql.database.database_interface import HRC_raw_0
from acsql.database.database_interface import SBC_raw_0
from acsql.utils.utils import SETTINGS


def calculate_file_size(args):
    """Calculates the file size for the given observation and writes
    the data to the data file.

    Parameters
    ----------
    args : tuple
        The ``multiprocessing`` arguments.  The 0th element is the
        ``date``, the 1st element is the ``path``, and the 3rd
        element is the ``detector``
    """

    # Parse multiprocessing args
    date = args[0]
    path = args[1]
    detector = args[2]

    print('Processing {}'.format(path))

    # Determine total file size for each rootname
    filenames = glob.glob(os.path.join(path, '*.fits'))
    filesizes = [os.path.getsize(filename) for filename in filenames]
    total_size = sum(filesizes) / (10**12)

    # Write the data to a file
    with open('figures/file_sizes.dat', 'a') as f:
        f.write('{},{},{}\n'.format(date, total_size, detector))


def main():
    """The main function.  See module docstrings for further details."""

    # Get rootnames and date-obs from every file
    results_wfc = session.query(WFC_raw_0.rootname, getattr(WFC_raw_0, 'date-obs')).all()
    results_hrc = session.query(HRC_raw_0.rootname, getattr(HRC_raw_0, 'date-obs')).all()
    results_sbc = session.query(SBC_raw_0.rootname, getattr(SBC_raw_0, 'date-obs')).all()

    # Parse results
    rootnames = [item[0] for item in results_wfc]
    detectors = ['WFC' for item in results_wfc]
    dates = [item[1] for item in results_wfc]

    rootnames.extend([item[0] for item in results_hrc])
    detectors.extend(['HRC' for item in results_hrc])
    dates.extend([item[1] for item in results_hrc])

    rootnames.extend([item[0] for item in results_sbc])
    detectors.extend(['SBC' for item in results_sbc])
    dates.extend([item[1] for item in results_sbc])

    # Build full path to the files
    paths = [os.path.join(SETTINGS['filesystem'], '{}/{}?'.format(rootname[0:4], rootname)) for rootname in rootnames]

    # Parallize operations
    mp_args = [(date, path, detector) for date, path, detector in zip(dates, paths, detectors)]
    pool = multiprocessing.Pool(processes=30)
    pool.map(calculate_file_size, mp_args)
    pool.close()
    pool.join()


if __name__ == '__main__':

    main()
