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

from astropy.io import fits
import numpy as np
from PIL import Image

from acsql.database.database_interface import Master
from acsql.database.database_interface import session
from acsql.utils.utils import SETTINGS
from acsql.utils.utils import setup_logging


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
    fsys_paths = glob.glob(os.path.join(SETTINGS['filesystem'], 'j*', '*'))
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
    else:
        file_dict['jpg_filename'] = None
        file_dict['jpg_dst'] = None

    return file_dict


def make_jpeg(file_dict):
    """Creates a JPEG for the given file.

    Parameters
    ----------
    filename : str
        The path to the file.
    filetype : str
        The filetype.  Can be either 'raw', 'flt', or 'flc'
    """

    logging.info('\tCreating JPEG')

    hdulist = fits.open(file_dict['filename'], mode='readonly')
    data = hdulist[1].data

    print(file_dict['filename'], hdulist[0].header['detector'])

    # If the image is full-frame, add on the other extension
    if len(hdulist) > 4 and hdulist[0].header['detector'] == 'wfc':
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
    jpeg_filename = os.path.basename(file_dict['filename']).replace('.fits', '.jpg')
    jpeg_filename = os.path.join(SETTINGS['jpeg_dir'], jpeg_filename)
    image = Image.fromarray(data)
    image.save(jpeg_filename)

    # Close the hdulist
    hdulist.close()


def ingest():
    """The main function of the ingest module."""

    rootnames_to_ingest = get_rootnames_to_ingest()
    for rootname_path in rootnames_to_ingest:
        file_paths = glob.glob(os.path.join(rootname_path, '*'))
        for filename in file_paths:

            # Make dictionary that holds all the information you would ever
            # want about the file
            file_dict = make_file_dict(filename)

            # Update the database

            # Make JPEGs and Thumbnails
            if file_dict['filetype'] in ['raw', 'flt', 'flc']:
                make_jpeg(file_dict)


if __name__ == '__main__':

    module = os.path.basename(__file__).strip('.py')
    setup_logging(module)

    ingest()
