"""Create a "Quicklook" Thumbail for the given observation.

A Thumbail image is created from a given JPEG file (see module
documentation for ``make_jpeg.py`` for further details). A
thumbnail is a JPEG image reduced to 128 x 128 pixel size.  The
thumbails are used by the ``acsql`` web application for quickly viewing
many JPEGs.

Authors
-------
    Matthew Bourque

Use
---
    This module is inteneded to be imported and used by
    ``acsql.ingest.ingest.py`` as such:
    ::

        from acsql.ingest.make_thumbnail import make_thumbnail
        make_thumbnail(file_dict)

Dependencies
------------
    External library dependencies include:

    - ``PIL``
"""

import logging
import os
import shutil

from PIL import Image


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
