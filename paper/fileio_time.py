#! /usr/bin/env python

import datetime
import os

from astropy.io import fits
import matplotlib.pyplot as plt
import numpy as np

from acsql.database.database_interface import session
from acsql.database.database_interface import Master
from acsql.database.database_interface import WFC_raw_0
from acsql.utils.utils import SETTINGS


if __name__ == '__main__':

    num_files_list = np.arange(1, 1001)
    times_header, times_database = [], []

    for num_files in num_files_list:

        # Gather files to test with
        results = session.query(Master.path)\
            .filter(Master.path.like('%q'))\
            .filter(Master.detector == 'WFC')\
            .limit(num_files).all()
        filenames = [SETTINGS['filesystem'] + item[0] + '/{}_raw.fits'.format(item[0].split('/')[-1]) for item in results]

        # Get DATE-OBS via header
        start = datetime.datetime.now()
        for filename in filenames:
            date_obs = fits.getval(filename, 'DATE-OBS', 0)
        end = datetime.datetime.now()
        time_header = (end-start).total_seconds()

        # Get DATE-OBS via database
        start = datetime.datetime.now()
        for filename in filenames:
            result = session.query(WFC_raw_0.date_obs)\
                .filter(WFC_raw_0.rootname == filename.split('/')[-1].split('_')[0][:-1]).one()
        end = datetime.datetime.now()
        time_database = (end-start).total_seconds()

        times_header.append(time_header)
        times_database.append(time_database)

        print(num_files, time_header, time_database)

    # Make the plot
    plt.style.use('bmh')
    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax.plot(num_files_list[1:], times_database[1:], color='green', label='acsql Database')
    ax.plot(num_files_list[1:], times_header[1:], color='red', label='astropy.fits')
    ax.set_xlabel('# of files')
    ax.set_ylabel('Time (seconds)')
    ax.set_title('Time to retrieve DATE-OBS from header')
    plt.legend(loc='upper left')
    plt.tight_layout()
    plt.savefig('figures/fileio_time_plhstins1.png')
