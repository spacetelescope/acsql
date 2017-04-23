#! /usr/bin/env python

"""Contains various functions to perform useful queries of the
``acsql`` database.

The available queries are:

    1. all_filenames
    2. filters_for_rootname(rootname)
    3. filter_distribution()
    4. rootnames_for_target(targname)
    5. filenames_for_calibration(calibration_keyword, value)
    6. goodmean_for_dataset(dataset)
    7. rootnames_with_postflash()
    8. non_asn_rootnames()
    9. asn_in_date_rage()

See each function's docstrings for further details.

Each function returns the ``sqlalchemy.query`` object for the query
performed so that the user may perform the query themselves and
perform additional operations with the query and/or its results.

Authors
-------
    Sara Ogaz
    Matthew Bourque

Use
---
    This script is intended to be imported as such:

    >>> from acsql.database import queries

    ``queries`` can then be used to perform individual queries, e.g.:

    >>> query = queries.filter_distribution()

    Each function will print the results to the screen, but the user
    may also perform the query and handle the results themselves, e.g.:

    >>> results = query.all()
"""

from sqlalchemy import and_
from sqlalchemy import exists
from sqlalchemy import func

from acsql.database.database_interface import session
from acsql.database.database_interface import Master
from acsql.database.database_interface import Datasets
from acsql.database.database_interface import WFC_asn_0
from acsql.database.database_interface import WFC_raw_0
from acsql.database.database_interface import WFC_flt_1
from acsql.database.database_interface import WFC_flt_4


def all_filenames(rootname):
    """Queries for all filenames that exist for the given rootname.

    Parameters
    ----------
    rootname : str
        The rootname to query by.

    Returns
    -------
    query : obj
        The query object that contains attributes and methods for
        performing the query.
    """

    # No proper way to do this yet until we enforce a foreign key
    # constraint in the database

    pass


def filters_for_rootname(rootname):
    """Queries for the FILTER1/FILTER2 combination for the geven
    observation.

    Parameters
    ----------
    rootname : str
        The rootname to query by.

    Returns
    -------
    query : obj
        The query object that contains attributes and methods for
        performing the query.
    """

    query = session.query(WFC_raw_0.filter1, WFC_raw_0.filter2)\
        .filter(WFC_raw_0.rootname == rootname)
    query_results = query.one()

    print('\nQuery performed:\n {}'.format(str(query)))
    print('{}: {}'.format(rootname, query_results))

    return query


def filter_distribution():
    """Queries for the FILTER1/FILTER2 combination for the given
    observation.

    Parameters
    ----------
    rootname : str
        The rootname to query by.

    Returns
    -------
    query : obj
        The query object that contains attributes and methods for
        performing the query.
    """

    query = session.query(WFC_raw_0.filter1, WFC_raw_0.filter2,
        func.count(WFC_raw_0.filter1))\
            .group_by(WFC_raw_0.filter1, WFC_raw_0.filter2)
    query_results = query.all()
    db_count = session.query(WFC_raw_0).count()

    print('\nQuery performed:\n {}'.format(str(query)))

    for result in query_results:
        perc_used = round((result[2] / db_count) * 100., 2)
        print('\t{}/{}: {}%'.format(result[0], result[1], perc_used))

    return query


def rootnames_for_target(targname):
    """Queries for the rootname and filename for a given target.

    Parameters
    ----------
    targname : str
        The target name (e.g. 'NGC104')

    Returns
    -------
    query : obj
        The query object that contains attributes and methods for
        performing the query.
    """

    query = session.query(WFC_raw_0.rootname, WFC_raw_0.filename,
        WFC_raw_0.targname)\
            .filter(WFC_raw_0.targname == targname)
    query_results = query.all()

    print('\nQuery performed:\n {}'.format(str(query)))

    for result in query_results:
        print(result)

    return query


def filenames_for_calibration(calibration_keyword, value):
    """Queries for the filenames that used a given calibration mode.

    The 'calibration' mode is defined by the type of calibration and
    the calibration reference file used
    (e.g. 'BIASFILE = jref$06u15056j_bia.fits')

    Parameters
    ----------
    calibration_keyword : str
        The calibration file to query on (e.g. DARKFILE, BIASFILE)
    value : str
        The calibration file value (e.g. jref$06u15056j_bia.fits)

    Returns
    -------
    query : obj
        The query object that contains attributes and methods for
        performing the query.
    """

    calibration_keyword_obj = getattr(WFC_raw_0, calibration_keyword)
    query = session.query(WFC_raw_0.filename)\
        .filter(calibration_keyword_obj == value)
    query_results = query.all()

    print('\nQuery performed:\n {}'.format(str(query)))

    for result in query_results:
        print(result[0])

    return query


def goodmean_for_dataset(dataset):
    """Queries for the GOODMEAN values for a given dataset

    The GOODMEAN describes the mean of all 'good' (i.e. non-flagged)
    pixels in the image.

    Parameters
    ----------
    dataset : str
        Any portion of (or entire) rootname (e.g. 'jd2615qi', or
        'jd2615').

    Returns
    -------
    query : obj
        The query object that contains attributes and methods for
        performing the query.
    """

    query = session.query(Master.rootname, WFC_flt_1.goodmean,
        WFC_flt_4.goodmean)\
            .join(WFC_flt_1)\
            .join(WFC_flt_4)\
            .filter(Master.rootname.like('{}%'.format(dataset)))
    query_results = query.all()

    print('\nQuery performed:\n {}'.format(str(query)))

    for result in query_results:
        print(result)

    return query


def rootnames_with_postflash():
    """Queries for rootnames and FLASHDURs for non-DARK observations
    that have a FLASHDUR > 0.

    Returns
    -------
    query : obj
        The query object that contains attributes and methods for
        performing the query.
    """

    query = session.query(Master.rootname, WFC_raw_0.flashdur)\
        .join(WFC_raw_0)\
        .filter(WFC_raw_0.flashdur > 0)\
        .filter(WFC_raw_0.targname != 'DARK')

    query_results = query.all()

    print('\nQuery performed:\n {}'.format(str(query)))

    for result in query_results:
        print(result)

    return query


def non_asn_rootnames():
    """Queries for rootnames that are not part of an association.

    Returns
    -------
    query : obj
        The query object that contains attributes and methods for
        performing the query.
    """

    query = session.query(Master.rootname)\
        .filter(~exists().where(and_(Master.rootname == WFC_asn_0.rootname)))
    query_results = query.all()

    print('\nQuery performed:\n {}'.format(str(query)))

    for result in query_results:
        print(result[0])

    return query


def asn_in_date_range(begin_date, end_date):
    """Queries for ASN filenames for observations that occur between
    the ``begin_date`` and ``end_date``.

    Parameters
    ----------
    begin_date : str
        The start of the date range (in the format YYYY-MM-DD).
    end_date : str
        The end of the date range (in the format YYYY-MM-DD).

    Returns
    -------
    query : obj
        The query object that contains attributes and methods for
        performing the query.
    """

    date_obs = getattr(WFC_raw_0, 'date-obs')  # Python doesn't like hyphens
    query = session.query(WFC_asn_0.filename)\
        .filter(date_obs >= begin_date)\
        .filter(date_obs <= end_date)
    query_results = query.all()

    print('\nQuery performed:\n {}'.format(str(query)))

    for result in query_results:
        print(result[0])

    return query
