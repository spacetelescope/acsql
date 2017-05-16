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
    - ``numpy``
    - ``PIL``
    - ``sqlalchemy``
"""

import copy
from datetime import date
import glob
import logging
import os
import shutil
import urllib.request

from astropy.io import fits
from astropy.io.fits.verify import VerifyError
import numpy as np
from sqlalchemy import Table
from sqlalchemy.exc import IntegrityError
from PIL import Image

from acsql.database.database_interface import Datasets
from acsql.database.database_interface import load_connection
from acsql.utils import utils
from acsql.utils.utils import insert_or_update
from acsql.utils.utils import SETTINGS
from acsql.utils.utils import TABLE_DEFS
from acsql.utils.utils import VALID_FILETYPES
from acsql.utils.utils import VALID_PROPOSAL_TYPES


def get_detector(test_file):
    """Return the ``detector`` associated with the given ``test_file``,
    if possible.

    Parameters
    ----------
    test_file : str
        The path to the file to attempt to get the ``detector`` header
        keyword from.

    Returns
    -------
    detector : str
        The detector (e.g. ``WFC``)
    """

    if 'jit' in test_file:
        detector = fits.getval(test_file, 'config', 0)
        if detector == 'S/C': #FGS observation
            detector = None
        else:
            detector = detector.lower().split('/')[1]
    else:
        detector = fits.getval(test_file, 'detector', 0).lower()

    return detector


def get_metadata_from_test_files(rootname_path, keyword):
    """Return the value of the given ``keyword`` and ``rootname_path``.

    The given ``rootname_path`` is checked for various filetypes that
    are beleived to have the ``keyword`` that is sought, in order
    of most likeliness: ``raw``, ``flt``, ``spt``, ``drz``, and
    ``jit``.  If a candidate file is found, it is used to determine
    the value of the ``keyword`` in the primary header.  If no
    candidate file exists, or the ``keyword`` value cannot be
    determined from the primary header, a ``value`` of ``None`` is
    returned, essentially ending the ingestion process for the given
    rootname.

    Parameters
    ----------
    rootname_path : str
        The path to the rootname in the MAST cache.
    keyword : str
        The header keyword to determine the value of (e.g.
        ``detector``)

    Returns
    -------
    value : str or None
        The header keyword value.
    """

    raw = glob.glob(os.path.join(rootname_path, '*raw.fits'))
    flt = glob.glob(os.path.join(rootname_path, '*flt.fits'))
    spt = glob.glob(os.path.join(rootname_path, '*spt.fits'))
    drz = glob.glob(os.path.join(rootname_path, '*drz.fits'))
    jit = glob.glob(os.path.join(rootname_path, '*jit.fits'))

    for test_files in [raw, flt, spt, drz, jit]:
        try:
            test_file = test_files[0]
            if keyword == 'detector':
                value = get_detector(test_file)
            elif keyword == 'proposid':
                value = get_proposid(test_file)
            break
        except (IndexError, KeyError):
            value = None

    if not value:
        logging.warning('Cannot determine {} for {}'\
            .format(keyword, rootname_path))

    return value


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
            logging.warning('\tCannot determine proposal type for {}'\
                .format(proposid))
            proposal_type = None

    # Check for bad proposal types
    if proposal_type not in VALID_PROPOSAL_TYPES:
        logging.warning('\tCannot determine proposal type for {}'\
            .format(proposid))
        proposal_type = None

    return proposal_type


def get_proposid(filename):
    """Return the proposal ID from the primary header of the given
    ``filename``.

    Parameters
    ----------
    filename : str
        The path to the file to get the ``proposid`` form.

    Returns
    -------
    proposid : int
        The proposal ID (e.g. ``12345``).
    """

    proposid = fits.getval(filename, 'proposid', 0)

    return proposid


def make_file_dict(filename):
    """Create a dictionary that holds information that is useful for
    the ingestion process.  This dictionary can then be passed around
    the various functions of the module.

    Parameters
    ----------
    filename : str
        The path to the file.

    Returns
    -------
    file_dict : dict
        A dictionary containing various data useful for the ingestion
        process.
    """

    file_dict = {}

    # Filename related keywords
    file_dict['filename'] = os.path.abspath(filename)
    file_dict['dirname'] = os.path.dirname(filename)
    file_dict['basename'] = os.path.basename(filename)
    file_dict['rootname'] = file_dict['basename'].split('_')[0][:-1]
    file_dict['full_rootname'] = file_dict['basename'].split('_')[0]
    file_dict['filetype'] = file_dict['basename'].split('.fits')[0].split('_')[-1]
    file_dict['proposid'] = file_dict['basename'][0:4]

    # Metadata kewords
    file_dict['detector'] = get_metadata_from_test_files(file_dict['dirname'], 'detector')
    if file_dict['detector']:
        file_dict['file_exts'] = getattr(utils, '{}_FILE_EXTS'.format(file_dict['detector'].upper()))[file_dict['filetype']]

    # JPEG related kewords
    if file_dict['filetype'] in ['raw', 'flt', 'flc']:
        file_dict['jpg_filename'] = file_dict['basename'].replace('.fits', '.jpg')
        file_dict['jpg_dst'] = os.path.join(SETTINGS['jpeg_dir'], file_dict['proposid'], file_dict['jpg_filename'])
        file_dict['thumbnail_filename'] = file_dict['basename'].replace('.fits', '.thumb')
        file_dict['thumbnail_dst'] = os.path.join(SETTINGS['thumbnail_dir'], file_dict['proposid'], file_dict['thumbnail_filename'])
    else:
        file_dict['jpg_filename'] = None
        file_dict['jpg_dst'] = None
        file_dict['thumbnail_filename'] = None
        file_dict['thumbnail_dst'] = None

    return file_dict


def make_jpeg(file_dict):
    """Creates a JPEG for the given file.

    Parameters
    ----------
    file_dict : dict
        A dictionary containing various data useful for the ingestion
        process.
    """

    logging.info('{}: Creating JPEG'.format(file_dict['rootname']))

    hdulist = fits.open(file_dict['filename'], mode='readonly')
    data = hdulist[1].data

    # If the image is full-frame WFC, add on the other extension
    if len(hdulist) > 4 and hdulist[0].header['detector'] == 'WFC':
        if hdulist[4].header['EXTNAME'] == 'SCI':
            data2 = hdulist[4].data
            height = data.shape[0] + data2.shape[0]
            width = data.shape[1]
            new_array = np.zeros((height, width))
            new_array[0:height/2, :] = data
            new_array[height/2:height, :] = data2
            data = new_array

    # Clip the top and bottom 1% of pixels.
    sorted_data = copy.copy(data)
    sorted_data = sorted_data.ravel()
    sorted_data.sort()
    top = sorted_data[int(len(sorted_data) * 0.99)]
    bottom = sorted_data[int(len(sorted_data) * 0.01)]
    top_index = np.where(data > top)
    data[top_index] = top
    bottom_index = np.where(data < bottom)
    data[bottom_index] = bottom

    # Scale the data.
    data = data - data.min()
    data = (data / data.max()) * 255.
    data = np.flipud(data)
    data = np.uint8(data)

    # Create parent JPEG directory if necessary
    jpg_dir = os.path.dirname(file_dict['jpg_dst'])
    if not os.path.exists(jpg_dir):
        os.makedirs(jpg_dir)
        logging.info('{}: Created directory {}'\
            .format(file_dict['rootname'], jpg_dir))

    # Write the image to a JPEG
    image = Image.fromarray(data)
    image.save(file_dict['jpg_dst'])

    # Close the hdulist
    hdulist.close()


def make_thumbnail(file_dict):
    """Creates a 128 x 128 pixel 'thumbnail' JPEG for the given file.

    Parameters
    ----------
    file_dict : dict
        A dictionary containing various data useful for the ingestion
        process.
    """

    logging.info('{}: Creating Thumbnail'.format(file_dict['rootname']))

    # Create parent Thumbnail directory if necessary
    thumb_dir = os.path.dirname(file_dict['thumbnail_dst'])
    if not os.path.exists(thumb_dir):
        try:
            os.makedirs(thumb_dir)
            logging.info('{}: Created directory {}'\
                .format(file_dict['rootname'], thumb_dir))
        except FileExistsError:
            pass

    # Make a copy of the JPEG in the thumbnail directory
    shutil.copyfile(file_dict['jpg_dst'], file_dict['thumbnail_dst'])

    # Open the copied JPEG and reduce its size
    image = Image.open(file_dict['thumbnail_dst'])
    image.thumbnail((128, 128), Image.ANTIALIAS)
    image.save(file_dict['thumbnail_dst'], 'JPEG')


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
    file_dict : dict
        A dictionary containing various data useful for the ingestion
        process.
    """

    rootname = os.path.basename(rootname_path)[:-1]
    proposid = get_metadata_from_test_files(rootname_path, 'proposid')
    proposal_type = get_proposal_type(proposid)

    # Insert a record in the master table
    data_dict = {'rootname': rootname,
                  'path': rootname_path,
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
