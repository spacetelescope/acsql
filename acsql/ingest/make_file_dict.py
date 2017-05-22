"""Create a dictionary containing useful information for the ingestion
process.

The ``file_dict`` contains various information that can be used by
``ingest.py`` (e.g. filesystem paths, observational metadata) and can
be used as a data container that can be easily passed around to various
functions.

Authors
-------
    Matthew Bourque

Use
---
    This module and its functionars are intended to be imported and
    used by ``acsql.ingest.ingest.py`` as such:
    ::

        from ascql.ingest.make_file_dict import get_detector
        from ascql.ingest.make_file_dict import get_metadata_from_test_files
        from ascql.ingest.make_file_dict import get_proposid
        from acsql.ingest.make_file_dict import make_file_dict

        make_file_dict(filename)
        get_detector(filename)
        get_metadata_from_test_files(rootname_path, keyword)
        get_proposid(filename)

Dependencies
------------
    External library dependencies include:

    - ``astropy``
"""

import glob
import logging
import os

from astropy.io import fits

from acsql.utils import utils
from acsql.utils.utils import SETTINGS


def get_detector(filename):
    """Return the ``detector`` associated with the given ``filename``,
    if possible.

    Parameters
    ----------
    filename : str
        The path to the file to attempt to get the ``detector`` header
        keyword from.

    Returns
    -------
    detector : str
        The detector (e.g. ``WFC``)
    """

    if 'jit' in filename:
        detector = fits.getval(filename, 'config', 0)
        if detector == 'S/C':  # FGS observation
            detector = None
        else:
            detector = detector.lower().split('/')[1]
    else:
        detector = fits.getval(filename, 'detector', 0).lower()

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

    proposid = str(fits.getval(filename, 'proposid', 0))

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
    file_dict['proposid_int'] = get_metadata_from_test_files(file_dict['dirname'], 'proposid')

    # Metadata kewords
    file_dict['detector'] = get_metadata_from_test_files(file_dict['dirname'], 'detector')
    if file_dict['detector']:
        file_dict['file_exts'] = getattr(utils, '{}_FILE_EXTS'.format(file_dict['detector'].upper()))[file_dict['filetype']]

    # JPEG related kewords
    if file_dict['filetype'] in ['raw', 'flt', 'flc']:
        file_dict['jpg_filename'] = file_dict['basename'].replace('.fits', '.jpg')
        file_dict['jpg_dst'] = os.path.join(SETTINGS['jpeg_dir'], file_dict['proposid_int'], file_dict['jpg_filename'])
        file_dict['thumbnail_filename'] = file_dict['basename'].replace('.fits', '.thumb')
        file_dict['thumbnail_dst'] = os.path.join(SETTINGS['thumbnail_dir'], file_dict['proposid_int'], file_dict['thumbnail_filename'])
    else:
        file_dict['jpg_filename'] = None
        file_dict['jpg_dst'] = None
        file_dict['thumbnail_filename'] = None
        file_dict['thumbnail_dst'] = None

    return file_dict
