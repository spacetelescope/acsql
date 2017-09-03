#! /usr/bin/env python

"""Plots the number of records in each acsql database table.

Authors
-------

    Matthew Bourque

Use
---

    This script is intended to be executed via the command line as
    such:
    ::

        python database_by_table.py

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

from acsql.database import database_interface
from acsql.database.database_interface import session
from acsql.utils.utils import HRC_FILE_EXTS
from acsql.utils.utils import SBC_FILE_EXTS
from acsql.utils.utils import SETTINGS
from acsql.utils.utils import WFC_FILE_EXTS


if __name__ == '__main__':

    # Build list of all tables
    hrc_tables, sbc_tables, wfc_tables = [], [], []
    for filetype in HRC_FILE_EXTS:
        for extension in HRC_FILE_EXTS[filetype]:
            hrc_tables.append('HRC_{}_{}'.format(filetype, extension))
    for filetype in SBC_FILE_EXTS:
        for extension in SBC_FILE_EXTS[filetype]:
            sbc_tables.append('SBC_{}_{}'.format(filetype, extension))
    for filetype in WFC_FILE_EXTS:
        for extension in WFC_FILE_EXTS[filetype]:
            wfc_tables.append('WFC_{}_{}'.format(filetype, extension))

    # Get the class objects for each table
    hrc_class_objs = [getattr(database_interface, table) for table in hrc_tables]
    sbc_class_objs = [getattr(database_interface, table) for table in sbc_tables]
    wfc_class_objs = [getattr(database_interface, table) for table in wfc_tables]

    hrc_num_records = []
    for table in hrc_class_objs:
        hrc_num_records.append(session.query(table).count())

    sbc_num_records = []
    for table in sbc_class_objs:
        sbc_num_records.append(session.query(table).count())

    wfc_num_records = []
    for table in wfc_class_objs:
        wfc_num_records.append(session.query(table).count())

    print(wfc_num_records)


    # plt.style.use('bmh')
    # fig = plt.figure()
    # ax = fig.add_subplot(111)
    # ax.bar(np.arange(len(filetypes)), num_files, align='center', color='green')
    # ax.set_ylabel('# of files')
    # ax.set_xlim((-1, len(filetypes)))
    # ax.set_xticks(np.arange(len(filetypes)))
    # ax.set_xticklabels(filetypes)
    # ax.tick_params(axis=u'both', which=u'both', length=0)
    # ax.grid('off')
    # plt.tight_layout()
    # plt.savefig('figures/num_files_by_filetype.png')

    # print(len(all_files))
