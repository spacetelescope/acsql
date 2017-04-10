#! /usr/bin/env python

"""
"""

import argparse
import glob
import logging
import os

from acsql.ingest.ingest import ingest
from acsql.utils.utils import FILE_EXTS, SETTINGS, setup_logging


def get_rootnames_to_ingest():
    """Return a list of paths to rootnames in the filesystem that need
    to be ingested (i.e. do not already exist in the ``acsql``
    database).

    Returns
    -------
    rootnames_to_ingest : list
        A list of full paths to rootnames that exist in the filesystem
        but not in the ``acsql`` database.
    """

    # Query the database to determine which rootnames already exist
    # results = session.query(Master.rootname).all()
    # db_rootnames = set([item for item in results])
    db_rootnames = set()

    # Gather list of rootnames that exist in the filesystem
    fsys_paths = glob.glob(os.path.join(SETTINGS['filesystem'], 'j9c*', '*'))
    fsys_rootnames = set([os.path.basename(item) for item in fsys_paths])

    # Determine new rootnames to ingest
    new_rootnames = fsys_rootnames - db_rootnames

    # Re-retreive the full paths
    rootnames_to_ingest = [item for item in fsys_paths if
                           os.path.basename(item) in new_rootnames]

    return rootnames_to_ingest


def ingest_production(filetype, ingest_filelist):
    """Perform ingestion on the given filelist of rootnames (or if not
    provided, any new rootnames that exist in the MAST filesystem but
    not in the ``acsql`` database) for the given ``filetype`` (or all
    filetypes if ``filetype`` == `all`).

    Parameters
    ----------
    filetype : str
        The filetype to ingest (e.g. ``flt``, or ``all``)
    ingest_filelist : str or None
        The path to a file that contains rootnames to ingest.  If
        ``None``, then the acsql database and MAST filesystem are
        used to determine new rootnames to ingest.
    """

    if ingest_filelist:
        rootnames_to_ingest = ingest_filelist
    else:
        rootnames_to_ingest = get_rootnames_to_ingest()
    # rootnames_to_ingest = ['/user/ogaz/acsql/test_files/jczgu1k0q/',
    #                       '/user/ogaz/acsql/test_files/jczgu1kcq/']

    for rootname in rootnames_to_ingest[0:1]:
        ingest(rootname, filetype)


def parse_args():
    """Parse command line arguments. Returns ``args`` object

    Returns
    -------
    args : obj
        An argparse object containing all of the arguments
    """

    valid_filetypes = list(FILE_EXTS.keys())
    valid_filetypes.extend(['all'])

    # Create help strings
    filetype_help = 'The filetypes to ingest.  Can be one of the following: '
    filetype_help += '{}.  If "all", then all '.format(valid_filetypes)
    filetype_help += 'availble filetypes for each rootname will be ingested. '
    filetype_help += 'If a specific filetype is given, then only that '
    filetype_help += 'filetype will be ingested. "all" is the default option.'
    ingest_filelist_help = 'A file containing a list of rootnames to ingest. '
    ingest_filelist_help += 'If not provided, then the acsql database is used '
    ingest_filelist_help += 'to determine which files get ingested.'

    # Add arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-f --filetype',
        dest='filetype',
        action='store',
        required=False,
        default='all',
        help=filetype_help)
    parser.add_argument('-i --ingest_filelist',
        dest='ingest_filelist',
        action='store',
        required=False,
        default=None,
        help=ingest_filelist_help)

    # Parse args
    args = parser.parse_args()

    # Test the args
    test_args(args)

    return args


def test_args(args):
    """Test the command line arguments to ensure that they are valid.

    Parameters
    ----------
    args : obj
        An argparse objects containing all of the arguments.

    Raises
    ------
    AssertionError
        If any of the argument conditions fail.
    """

    # Ensure the filetype is valid
    valid_filetypes = list(FILE_EXTS.keys())
    valid_filetypes.extend(['all'])
    assert args.filetype in valid_filetypes,\
        '{} is not a valid filetype'.format(args.filetype)

    # Ensure that the ingest_filelist exists
    if args.ingest_filelist:
        assert os.path.exists(ingest_filelist),\
            '{} does not exist.'.format(args.ingest_filelist)


if __name__ == '__main__':

    module = os.path.basename(__file__).strip('.py')
    setup_logging(module)

    args = parse_args()
    ingest_production(args.filetype, args.ingest_filelist)
