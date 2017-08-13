"""Ingests a given rootname (and its associated files) into the
``ascql`` database.  The tables that are updated are the ``master``
table, the ``datasets`` table, and any appropriate header tables
(e.g. ``wfc_raw_0``) based on the available filetypes and header
extensions.

Authors
-------
    - Matthew Bourque
    - Sara Ogaz

Use
---
    This module is intended to be imported from and used by the
    ``ingest_production`` script as such:
    ::

        from acsql.ingest.ingest import ingest
        ingest(rootname)

Dependencies
------------
    External library dependencies include:

    - ``acsql``
    - ``astropy``
    - ``sqlalchemy``
"""

from datetime import date
import glob
import logging
import os
import urllib.request

from astropy.io import fits
from astropy.io.fits.verify import VerifyError
from sqlalchemy import Table
from sqlalchemy.exc import IntegrityError

from acsql.database.database_interface import Datasets
from acsql.database.database_interface import load_connection
from acsql.ingest.make_file_dict import get_metadata_from_test_files
from acsql.ingest.make_file_dict import make_file_dict
from acsql.ingest.make_jpeg import make_jpeg
from acsql.ingest.make_thumbnail import make_thumbnail
from acsql.utils.utils import insert_or_update
from acsql.utils.utils import SETTINGS
from acsql.utils.utils import TABLE_DEFS
from acsql.utils.utils import VALID_FILETYPES
from acsql.utils.utils import VALID_PROPOSAL_TYPES


def get_proposal_type(proposid):
    """Return the ``proposal_type`` for the given ``proposid``.

    The ``proposal_type`` is the type of proposal (e.g. ``CAL``,
    ``GO``, etc.).  The ``proposal_type`` is scraped from the MAST
    proposal status webpage for the given ``proposid``.  If the
    ``proposal_type`` cannot be determined, a ``None`` value is returned.

    Parameters
    ----------
    proposid : str
        The proposal ID (e.g. ``12345``).

    Returns
    -------
    proposal_type : int or None
        The proposal type (e.g. ``CAL``).
    """

    if not proposid:
        proposal_type = None
    else:
        try:
            url = 'http://www.stsci.edu/cgi-bin/get-proposal-info?id='
            url += '{}&submit=Go&observatory=HST'.format(proposid)
            webpage = urllib.request.urlopen(url)
            proposal_type = webpage.readlines()[11].split(b'prop_type">')[-1]
            proposal_type = proposal_type.split(b'</a>')[0].decode()
        except:
            logging.warning('Cannot determine proposal type for {}'\
                .format(proposid))
            proposal_type = None

    # Check for bad proposal types
    if proposal_type not in VALID_PROPOSAL_TYPES:
        logging.warning('Cannot determine proposal type for {}'\
            .format(proposid))
        proposal_type = None

    return proposal_type


def update_datasets_table(file_dict):
    """Insert/update an entry for the file in the ``datasets`` table.

    Parameters
    ----------
    file_dict : dict
        A dictionary containing various data useful for the ingestion
        process.
    """

    session, base, engine = load_connection(SETTINGS['connection_string'])

    # Check to see if a record exists for the rootname
    query = session.query(Datasets)\
        .filter(Datasets.rootname == file_dict['rootname'])
    query_count = query.count()

    # If there are no results, then perform an insert
    if not query_count:

        data_dict = {}
        data_dict['rootname'] = file_dict['rootname']
        data_dict[file_dict['filetype']] = file_dict['basename']

        tab = Table('datasets', base.metadata, autoload=True)
        insert_obj = tab.insert()
        try:
            insert_obj.execute(data_dict)
        except IntegrityError as e:
            logging.warning('{}: Unable to insert {} into datasets table: {}'\
                .format(file_dict['full_rootname'], file_dict['basename'], e))

    # If there are results, add the filename to the existing entry
    else:
        data_dict = query.one().__dict__
        del data_dict['_sa_instance_state']
        data_dict[file_dict['filetype']] = file_dict['basename']
        try:
            query.update(data_dict)
        except IntegrityError as e:
            logging.warning('{}: Unable to update {} in datasets table: {}'\
                .format(file_dict['full_rootname'], file_dict['basename'], e))

    session.commit()
    session.close()
    engine.dispose()

    logging.info('{}: Updated datasets table for {}.'\
        .format(file_dict['rootname'], file_dict['filetype']))


def update_header_table(file_dict, ext):
    """Insert/update an entry for the file in the appropriate header
    table (e.g. ``wfc_raw_0``).

    The header table that get updated depend on the detector, filetype,
    and extension.

    Parameters
    ----------
    file_dict : dict
        A dictionary containing various data useful for the ingestion
        process.
    ext : int
        The header extension.
    """

    # Check if header is an ingestable header before proceeding
    valid_extnames = ['PRIMARY', 'SCI', 'ERR', 'DQ', 'UDL', 'jit', 'jif',
                      'ASN', 'WHT', 'CTX']
    ext_exists = True
    try:
        header = fits.getheader(file_dict['filename'], ext)
        if ext == 0:
            extname = 'PRIMARY'
        else:
            extname = header['EXTNAME']
    except IndexError:
        ext_exists = False
        extname = None

    # Ingest the header if it is ingestable
    if ext_exists and extname in valid_extnames:

        table = "{}_{}_{}".format(file_dict['detector'].upper(),
                                  file_dict['filetype'].lower(),
                                  str(ext))

        exclude_list = ['HISTORY', 'COMMENT', 'ROOTNAME', 'FILENAME', '']
        input_dict = {'rootname': file_dict['rootname'],
                      'filename': file_dict['basename']}

        try:
            for key, value in header.items():
                key = key.strip()

                # Switch hypens to underscores
                if '-' in key:
                    key = key.replace('-', '_')

                if key in exclude_list or value == "":
                    continue
                elif key not in TABLE_DEFS[table.lower()]:
                    logging.warning('{}: {} not in {}'\
                        .format(file_dict['full_rootname'], key, table))
                    continue

                input_dict[key.lower()] = value

            insert_or_update(table, input_dict)
            logging.info('{}: Updated {} table.'.format(file_dict['rootname'],
                                                      table))

        except VerifyError as e:
            logging.warning('\tUnable to insert {} into {}: {}'.format(
                file_dict['rootname'], table, e))


def update_master_table(rootname_path):
    """Insert/update an entry in the ``master`` table for the given
    file.

    Parameters
    ----------
    rootname_path
        The path to the rootname directory in the MAST cache.
    """

    rootname = os.path.basename(rootname_path)[:-1]
    path = rootname_path[-15:]
    proposid = get_metadata_from_test_files(rootname_path, 'proposid')
    proposal_type = get_proposal_type(proposid)

    # Insert a record in the master table
    data_dict = {'rootname': rootname,
                  'path': path,
                  'first_ingest_date': date.today().isoformat(),
                  'last_ingest_date': date.today().isoformat(),
                  'detector': get_metadata_from_test_files(rootname_path,
                                                           'detector'),
                  'proposal_type': proposal_type}
    insert_or_update('Master', data_dict)
    logging.info('{}: Updated master table.'.format(rootname))


def ingest(rootname_path, filetype='all'):
    """The main function of the ingest module.  Ingest a given rootname
    (and its associated files) into the various tables of the ``acsql``
    database.

    If for some reason the file is unable to be ingested, a warning is
    logged.

    Parameters
    ----------
    rootname_path : str
        The path to the rootname directory in the MAST cache.
    """

    rootname = os.path.basename(rootname_path)[:-1]
    logging.info('{}: Begin ingestion'.format(rootname))

    # Update the master table for the rootname
    update_master_table(rootname_path)

    if filetype == 'all':
        search = '*.fits'
    else:
        search = '*{}.fits'.format(filetype)
    file_paths = glob.glob(os.path.join(rootname_path, search))

    for filename in file_paths:
        filetype = os.path.basename(filename).split('.')[0][10:]
        if filetype in VALID_FILETYPES:

            # Make dictionary that holds all the information you would ever
            # want about the file
            file_dict = make_file_dict(filename)

            # Update header tables
            if 'file_exts' in file_dict:
                for ext in file_dict['file_exts']:
                    update_header_table(file_dict, ext)

                # Update datasets table
                update_datasets_table(file_dict)

                # Make JPEGs and Thumbnails
                if file_dict['filetype'] in ['raw', 'flt', 'flc']:
                    make_jpeg(file_dict)
                if file_dict['filetype'] == 'flt':
                    make_thumbnail(file_dict)

    logging.info('{}: End ingestion'.format(rootname))
