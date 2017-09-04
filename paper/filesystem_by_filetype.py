#! /usr/bin/env python

"""Plots the number of files in the acsql filesytem, broken down by
filetype.

Authors
-------

    Matthew Bourque

Use
---

    This script is intended to be executed via the command line as
    such:
    ::

        python filesystem_by_filetype.py

Dependencies
------------

    - ``acsql``
    - ``matplotlib``
    - ``numpy``
"""

from collections import defaultdict
import glob
import os

import matplotlib.pyplot as plt
import numpy as np

from acsql.utils.utils import SETTINGS


if __name__ == '__main__':

    parent_dirs = ['j5', 'j6', 'j7', 'j8', 'j9', 'ja', 'jb', 'jc', 'jd']
    filetypes = ['spt', 'raw', 'flt', 'jit', 'jif', 'flc', 'drz', 'asn', 'drc', 'crj', 'crc']

    # Only do one parent directory at a time, since the whole filesystem is too big
    # for glob.glob
    for parent_dir in parent_dirs:

        print('Processsing parent directory {}'.format(parent_dir))

        # Get list of all files
        search = os.path.join(SETTINGS['filesystem'], '{}*'.format(parent_dir), '*', '*.fits')
        all_files = glob.glob(search)

        num_files = []
        for filetype in filetypes:
            files = [file for file in all_files if '{}.fits'.format(filetype) in file]
            num_files.append(len(files))

        with open('filesystem_by_filetype_{}.dat'.format(parent_dir), 'w') as f:
            for filetype, amount in zip(filetypes, num_files):
                f.write('{},{}\n'.format(filetype,amount))

    # Aggregate results
    data_files = glob.glob('filesystem_by_filetype_*.dat')
    amounts_dict = defaultdict(int)
    for data_file in data_files:
        with open(data_file) as f:
            data = f.readlines()

        data = [item.strip().split(',') for item in data]
        filetypes = [item[0] for item in data]
        amounts = [int(item[1]) for item in data]

        for filetype, amount in zip(filetypes, amounts):
            amounts_dict[filetype] += amount

    # Get back to lists to plot
    filetypes = list(amounts_dict.keys())
    num_files = list(amounts_dict.values())

    # Sort in descending order
    num_files, filetypes = (list(x) for x in zip(*sorted(zip(num_files, filetypes))))
    num_files = num_files[::-1]
    filetypes = filetypes[::-1]

    # Make the plot
    plt.style.use('bmh')
    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax.bar(np.arange(len(filetypes)), num_files, align='center', color='green')
    ax.set_ylabel('# of files')
    ax.set_xlim((-1, len(filetypes)))
    ax.set_xticks(np.arange(len(filetypes)))
    ax.set_xticklabels(filetypes)
    ax.tick_params(axis=u'both', which=u'both', length=0)
    ax.grid('off')
    plt.tight_layout()
    plt.savefig('figures/num_files_by_filetype.png')

    print(sum(num_files))
