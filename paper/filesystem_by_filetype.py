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

import glob
import os

import matplotlib.pyplot as plt
import numpy as np

from acsql.utils.utils import SETTINGS


if __name__ == '__main__':

    # Get list of all files
    search = os.path.join(SETTINGS['filesystem'], 'j*', '*', '*.fits')
    all_files = glob.glob(search)

    filetypes = ['spt', 'raw', 'flt', 'jit', 'jif', 'flc', 'drz', 'asn', 'drc', 'crj', 'crc']

    num_files = []
    for filetype in filetypes:
        files = [file for file in all_files if '{}.fits'.format(filetype) in file]
        num_files.append(len(files))

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

    print(len(all_files))
