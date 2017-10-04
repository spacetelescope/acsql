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

    # Determine number of records in each table
    hrc_num_records = []
    for table in hrc_class_objs:
        print('Querying {}'.format(table))
        hrc_num_records.append(session.query(table).count())

    sbc_num_records = []
    for table in sbc_class_objs:
        print('Querying {}'.format(table))
        sbc_num_records.append(session.query(table).count())

    wfc_num_records = []
    for table in wfc_class_objs:
        print('Querying {}'.format(table))
        wfc_num_records.append(session.query(table).count())

    # Get number of records for master and datasets tables
    master_num_records = session.query((getattr(database_interface, 'Master'))).count()
    datasets_num_records = session.query((getattr(database_interface, 'Datasets'))).count()

    # Aggregate results into two lists
    table_names = wfc_tables
    table_names.extend(hrc_tables)
    table_names.extend(sbc_tables)
    table_names.extend(['Datasets', 'Master'])

    num_records = wfc_num_records
    num_records.extend(hrc_num_records)
    num_records.extend(sbc_num_records)
    num_records.extend([datasets_num_records, master_num_records])

    # Build list of colors
    colors = ['blue' for item in wfc_tables]
    colors.extend(['green' for item in hrc_tables])
    colors.extend(['red' for item in sbc_tables])
    colors.extend(['orange', 'orange'])

    # Reverse the list so that plot is more aesthetically pleasing
    table_names = table_names[::-1]
    num_records = num_records[::-1]
    colors = colors[::-1]

    # Make the plot
    plt.style.use('bmh')
    plt.rcParams['font.size'] = 10
    fig = plt.figure(figsize=(8,20))
    ax = fig.add_subplot(111)
    ax.barh(np.arange(len(table_names)), num_records, align='center', color=colors)
    ax.set_xlabel('# of records')
    ax.set_ylim((-1, len(table_names)))
    ax.set_yticks(np.arange(len(table_names)))
    ax.set_yticklabels(table_names)
    ax.tick_params(axis=u'both', which=u'both', length=0)
    ax.yaxis.grid(False)
    ax.xaxis.grid(True)
    plt.tight_layout()
    plt.savefig('figures/database_records.png')

    print(sum(num_records))
