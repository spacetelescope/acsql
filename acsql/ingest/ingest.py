#! /usr/bin/env python

"""Ingests new data into the ascql database.

Authors
-------
    Matthew Bourque, 2017

Use
---

"""

import glob
import os
import time

from astropy.io import fits

from acsql.database.database_interface import Master
from acsql.database.database_interface import session
from acsql.utils.utils import SETTINGS


def get_files_to_ingest():
    """
    """

    # Query the database to determine which rootnames already exist
    results = session.query(Master.rootname).all()
    db_rootnames = set([item for item in results])

    # Gather list of rootnames that exist in the filesystem
    fsys_paths = glob.glob(os.path.join(SETTINGS['filesystem'], 'j*', '*'))
    fsys_rootnames = set([os.path.basename(item) for item in fsys_paths])

    # Determine new rootnames to ingest
    new_rootnames = fsys_rootnames - db_rootnames

    # Re-retreive the full paths
    files_to_ingest = [item for item in fsys_paths if os.path.basename(item) in new_rootnames]

    return new_rootnames


def ingest():
    """
    """

    files_to_ingest = get_files_to_ingest()

    print(len(files_to_ingest))


if __name__ == '__main__':

    ingest()