"""Create a "Quicklook" JPEG for the given observation.

A JPEG is created for every ``raw``, ``flt``, and ``flc`` file and is
placed into the ``acsql`` filesystem of JPEGs.  The JPEGs are then
used by the ``acsql`` web application to easily view ACS observaitons.

Authors
-------
    Matthew Bourque

Use
---
    This module is inteneded to be imported and used by
    ``acsql.ingest.ingest.py`` as such:
    ::

        from acsql.ingest.make_jpeg import make_jpeg
        make_jpeg(file_dict)

Dependencies
------------
    External library dependencies include:

    - ``astropy``
    - ``numpy``
    - ``PIL``
"""

import copy
import logging
import os

from astropy.io import fits
import numpy as np
from PIL import Image


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
