#! /usr/bin/env python

"""Ingests new data into the ascql database.

Authors
-------
    Matthew Bourque, 2017

Use
---

"""

import copy
import glob
import logging
import os
import shutil

from astropy.io import fits
import numpy as np
from sqlalchemy import Table
from datetime import date
from PIL import Image

from acsql.database.database_interface import Master, session, base
from acsql.utils.utils import FILE_EXTS, SETTINGS, setup_logging


def get_rootnames_to_ingest():
    """Return a list of paths to rootnames in the filesystem that need
    to be ingested (i.e. do not already exist in the acsql database).

    Returns
    -------
    rootnames_to_ingest : list
        A list of full paths to rootnames that exist in the filesystem
        but not in the acsql database.
    """

    # Query the database to determine which rootnames already exist
    # results = session.query(Master.rootname).all()
    # db_rootnames = set([item for item in results])
    db_rootnames = set()

    # Gather list of rootnames that exist in the filesystem
    fsys_paths = glob.glob(os.path.join(SETTINGS['filesystem'], 'j9*', '*'))
    fsys_rootnames = set([os.path.basename(item) for item in fsys_paths])

    # Determine new rootnames to ingest
    new_rootnames = fsys_rootnames - db_rootnames

    # Re-retreive the full paths
    rootnames_to_ingest = [item for item in fsys_paths if
                           os.path.basename(item) in new_rootnames]

    return rootnames_to_ingest


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
    file_dict['basename'] = os.path.basename(filename)
    file_dict['rootname'] = file_dict['basename'].split('_')[0][:-1]
    file_dict['filetype'] = file_dict['basename'].split('.fits')[0].split('_')[-1]

    # Metadata kewords
    if file_dict['filetype'] in ['raw', 'flt', 'flc', 'drz']:
        file_dict['detector'] = fits.getval(file_dict['filename'], 'detector', 0)

    # JPEG related kewords
    if file_dict['filetype'] in ['raw', 'flt', 'flc']:
        file_dict['jpg_filename'] = file_dict['basename'].replace('.fits', '.jpg')
        file_dict['jpg_dst'] = os.path.join(SETTINGS['jpeg_dir'], file_dict['jpg_filename'])
        file_dict['thumbnail_filename'] = file_dict['basename'].replace('.fits', '.thumb')
        file_dict['thumbnail_dst'] = os.path.join(SETTINGS['thumbnail_dir'], file_dict['thumbnail_filename'])
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

    logging.info('\tCreating JPEG')

    hdulist = fits.open(file_dict['filename'], mode='readonly')
    data = hdulist[1].data

    # If the image is full-frame WFC, add on the other extension
    if len(hdulist) > 4 and hdulist[0].header['detector'] == 'WFC':
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

    # Write the image to a JPEG
    image = Image.fromarray(data)
    image.save(file_dict['jpg_dst'])

    # Close the hdulist
    hdulist.close()


def make_thumbnail(file_dict):
    """Creates a 128 x 128 pixel 'thumnail' JPEG for the given file.

    Parameters
    ----------
    file_dict : dict
        A dictionary containing various data useful for the ingestion
        process.
    """

    logging.info('\tCreating Thumbnail')

    shutil.copyfile(file_dict['jpg_dst'], file_dict['thumbnail_dst'])

    image = Image.open(file_dict['thumbnail_dst'])
    image.thumbnail((128, 128), Image.ANTIALIAS)
    image.save(file_dict['thumbnail_dst'], 'JPEG')


def update_header_table(file_dict, ext):
    """
    """

    header = file_dict['hdulist'][ext].header

    exclude_list = ['HISTORY', 'COMMENT','ROOTNAME', '']
    input_dict = {'rootname': file_dict['rootname']}
                  #'basename': file_dict['basename']}

    for key, value in header.items():
        if key.strip() in exclude_list or value == "":
            continue
        input_dict[key.lower()] = value

    # Update the database
    table_name = "{}_{}_{}".format(aperture.lower(),filetype.lower(),
                                 str(extension))
    current_tab = Table(table_name, base.metadata, autoload=True)
    insert_obj = current_tab.insert()
    insert_obj.execute(input_dict)
    logging.info('\t\tUpdated {} table.'.format(table_name))


def update_master_table(rootname_path):
    """
    """

    logging info('\tIngesting {}'.format(rootname_path))

    # Insert a record in the master table
    master_tab = Table('master', base.metadata, autoload=True)
    insert_obj = master_tab.insert()
    input_dict = {'rootname': rootname_path.split("/")[-2][:-1],
                  'path': rootname_path,
                  'first_ingest_date': date.today().isoformat(),
                  'last_ingest_date': date.today().isoformat(),
                  'detector': 'WFC'}
    insert_obj.execute(input_dict)
    logging.info('\t\tUpdated master table.')


def ingest():
    """The main function of the ingest module."""

    rootnames_to_ingest = get_rootnames_to_ingest()
    # rootnames_to_ingest = ['/user/ogaz/acsql/test_files/jczgu1k0q/',
    #                       '/user/ogaz/acsql/test_files/jczgu1kcq/']

    for rootname_path in rootnames_to_ingest[0:1]:

        update_master(rootname_path)
        file_paths = glob.glob(os.path.join(rootname_path, '*'))

        for filename in file_paths:

            # Make dictionary that holds all the information you would ever
            # want about the file
            file_dict = make_file_dict(filename)

            for ext in FILE_EXTS[file_dict['filetype']]:
                update_header_table(file_dict, ext)

            # # Make JPEGs and Thumbnails  # Make JPEGs and Thumbnails
            # if file_dict['filetype'] in ['raw', 'flt', 'flc']:
            #     make_jpeg(file_dict)
            # if file_dict['filetype'] == 'flt':
            #     make_thumbnail(file_dict)


if __name__ == '__main__':

    module = os.path.basename(__file__).strip('.py')
    setup_logging(module)
    ingest()
