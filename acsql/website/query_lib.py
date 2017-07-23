"""Contains various functions to support the ``/database/`` webpage of
the ``acsql`` web application.

Functions include those that parse, build, validate, and return
``SQLAlchemy`` ``query`` objects in order to perform a database query
through the web application.

Authors
-------

    - Matthew Bourque
    - Meredith Durbin

Use
---

    This module is intended to be imported and used by the
    ``acsql_webapp`` module as such:
    ::

        from query_lib import generate_csv
        from query_lib import get_query_results

        generate_csv(output_columns, results)
        get_query_results(query_form_dict)

Dependencies
------------

    - ``acsql``
    - ``sqlalchemy``
"""

from sqlalchemy import create_engine
from sqlalchemy import literal_column
from sqlalchemy import or_
from sqlalchemy.orm import sessionmaker

from acsql.database.database_interface import Master
from acsql.database.database_interface import WFC_raw_0
from acsql.database.database_interface import HRC_raw_0
from acsql.database.database_interface import SBC_raw_0
from acsql.utils.utils import SETTINGS


def _apply_query_filter(table, key, values, query):
    """Apply a filter to the given ``query`` based on the ``table``,
    ``key``, and ``value``.

    Parameters
    ----------
    table : obj
        The ``SQLAlchemy`` table object associated with the table to
        apply the filter to.
    key : str
        The keyword for the column to filter on.
    values : obj
        The requested values of the ``key``.
    query : obj
        The ``SQLAlchemy`` ``query`` objecgt to perform the filtering
        on.

    Returns
    -------
    query : obj
        The ``SQLAlchemy`` ``query`` object with filter applied.
    """

    # Fields that accpet comma-separated lists and wildcards
    csv_keys = ['rootname', 'targname', 'pr_inv_l', 'pr_inv_f']

    # Fields that allow operators
    operator_keys = ['date_obs', 'exptime']

    # Parse the key/value pairs for comma-separated values
    if key in csv_keys:
        parsed_value = values[0].replace(' ', '').split(',')
        parsed_value = [item.replace('*', '%') for item in parsed_value]
        conditions = [getattr(table, key).like(val) for val in parsed_value]
        query = query.filter(or_(*conditions))

    # Parse the key/value pairs for operator keys
    elif key in operator_keys:
        if values['op'] == 'between':
            if len(values) == 3:
                if float(values['val1'].replace('-', '')) < float(values['val2'].replace('-', '')):
                    query = query.filter(getattr(table, key).between(values['val1'], values['val2']))
                elif float(values['val1'].replace('-', '')) > float(values['val2'].replace('-', '')):
                    query = query.filter(getattr(table, key).between(values['val2'], values['val1']))
                elif float(values['val1'].replace('-', '')) == float(values['val2'].replace('-', '')):
                    raise ValueError('Values submitted for field "{}" must be different.'.format(key))
                else:
                    raise ValueError('Invalid values submitted for field "{}".'.format(key))
            else:
                query = query.filter(getattr(table, key).op('=')(values['val1']))
        else:
            query = query.filter(getattr(table, key).op(values['op'])(values['val1']))

    # Else the filtering is straightforward
    query = query.filter(getattr(table, key).in_(values))

    return query


def _build_queries(output_columns):
    """Builds queries of appropriate tables and columns

    Parameters
    ----------
    output_columns : list
        List of columns desired for query output

    Returns
    -------
    wfc_query : obj
        Query object for requested collumns in WFC database tables.
    hrc_query : obj
        Query object for requested collumns in HRC database tables.
    sbc_query : obj
        Query object for requested collumns in SBC database tables.
    """

    # Determine which columns belong to which tables
    master_cols = [getattr(Master, col) for col in output_columns if hasattr(Master, col)]
    wfc_cols = [getattr(WFC_raw_0, col) for col in output_columns if hasattr(WFC_raw_0, col)]
    hrc_cols = [getattr(HRC_raw_0, col) for col in output_columns if hasattr(HRC_raw_0, col)]
    sbc_cols = [getattr(SBC_raw_0, col) for col in output_columns if hasattr(SBC_raw_0, col)]

    # Determine which columns are unique to a specific table
    wfc_only = [col for col in output_columns if hasattr(WFC_raw_0, col)
                and not hasattr(Master, col)
                and not hasattr(HRC_raw_0, col)
                and not hasattr(SBC_raw_0, col)]
    hrc_only = [col for col in output_columns if hasattr(HRC_raw_0, col)
                and not hasattr(Master, col)
                and not hasattr(WFC_raw_0, col)
                and not hasattr(SBC_raw_0, col)]
    sbc_only = [col for col in output_columns if hasattr(SBC_raw_0, col)
                and not hasattr(Master, col)
                and not hasattr(WFC_raw_0, col)
                and not hasattr(HRC_raw_0, col)]

    # Combine columns amongst tables
    master_wfc = master_cols + wfc_cols + [literal_column('"--"').label(col) for col in hrc_only] + \
                 [literal_column('"--"').label(col) for col in sbc_only]
    master_hrc = master_cols + hrc_cols + [literal_column('"--"').label(col) for col in wfc_only] + \
                 [literal_column('"--"').label(col) for col in sbc_only]
    master_sbc = master_cols + sbc_cols + [literal_column('"--"').label(col) for col in wfc_only] + \
                 [literal_column('"--"').label(col) for col in hrc_only]

    if len(master_cols) == 0:

        # For WFC queries
        if len(wfc_cols) == 0:
            wfc_query = False
        else:
            session = _get_session()
            wfc_query = session.query(*master_wfc)
            session.close()

        # For HRC queries
        if len(hrc_cols) == 0:
            hrc_query = False
        else:
            session = _get_session()
            hrc_query = session.query(*master_hrc)
            session.close()

        # For SBC queries
        if len(sbc_cols) == 0:
            sbc_query = False
        else:
            session = _get_session()
            sbc_query = session.query(*master_sbc)
            session.close()

    else:
        session = _get_session()
        wfc_query = session.query(*master_wfc).join(WFC_raw_0)
        hrc_query = session.query(*master_hrc).join(HRC_raw_0)
        sbc_query = session.query(*master_sbc).join(SBC_raw_0)

    return wfc_query, hrc_query, sbc_query


def _convert_query_form_dict(query_form_dict):
    """Converts raw output from ``form.to_dict()`` to a format that is
    more useable for ``acsql`` database queries.

    Parameters
    ----------
    query_form_dict : dict
        The dictionary returned by ``form.to_dict()``.

    Returns
    -------
    query_form_dict : dict
        A new dictionary with blank entried removed and operator key
        entries reformatted.
    """

    # Keys that allow operators (e.g. greater than)
    operator_keys = ['date_obs', 'exptime']

    # Remove blank entries from form data
    query_form_dict = {key: value for key, value in list(query_form_dict.items()) if value != ['']}

    # Combine data returned from fields with operator dropdowns
    for operator_key in operator_keys:
        operator_dict = {}
        for key, value in list(query_form_dict.items()):
            if key == operator_key + '-op':
                operator_dict['op'] = value[0]
            elif key == operator_key + '-val1':
                operator_dict['val1'] = value[0]
            elif key == operator_key + '-val2':
                operator_dict['val2'] = value[0]
        if len(operator_dict) > 1:
            query_form_dict[operator_key] = operator_dict

    return query_form_dict


def generate_csv(output_columns, results):
    """Create a CSV file of the database query ouput.

    Parameters
    ----------
    output_columns : list
        A list of columns desired for the output file.
    results : list
        A list of results from the database query
    """

    header = ','.join(output_columns) + '\n'
    yield header

    for result in results:
        if len(result) == 1:
            yield str(result[0]) + '\n'
        else:
            yield ','.join(map(str, result)) + '\n'


def get_query_results(query_form_dict):
    """Returns a dictionary with the results of the requested query
    along with some additional metadata.  Calls on several internal
    functions to build and perform the query in order to abstract
    out its complexity.

    Parameters
    ----------
    query_form_dict : dict
        A dictionary containing information about the requested query,
        such as the ``output_format``, ``output_columns`` and the
        requested values.

    Returns
    -------
    query_results_dict : dict
        A dictionary containing the query results as well as some
        metadata such as ``output_format`` and number of results.
    """

    # Determine output format
    output_format = query_form_dict.pop('output_format')
    if output_format == ['thumbnails']:
        output_columns = ['rootname', 'detector', 'proposal_type',
                          'expstart', 'filter1', 'filter2',
                          'exptime', 'targname']
        if 'output_columns' in query_form_dict:
            query_form_dict.pop('output_format', None)
    else:
        output_columns = query_form_dict.pop('output_columns')

    # Remove blank entries from form data
    query_form_dict = _convert_query_form_dict(query_form_dict)

    # Build the query
    wfc_query, hrc_query, sbc_query = _build_queries(output_columns)

    # Perform filtering on the query
    for key, value in list(query_form_dict.items()):
        if hasattr(Master, key):
            if wfc_query:
                wfc_query = _apply_query_filter(Master, key, value, wfc_query)
            if hrc_query:
                hrc_query = _apply_query_filter(Master, key, value, hrc_query)
            if sbc_query:
                sbc_query = _apply_query_filter(Master, key, value, sbc_query)
        if wfc_query and hasattr(WFC_raw_0, key) and not hasattr(Master, key):
            wfc_query = _apply_query_filter(WFC_raw_0, key, value, wfc_query)
        if hrc_query and hasattr(HRC_raw_0, key) and not hasattr(Master, key):
            hrc_query = _apply_query_filter(HRC_raw_0, key, value, hrc_query)
        if sbc_query and hasattr(SBC_raw_0, key) and not hasattr(Master, key):
            sbc_query = _apply_query_filter(SBC_raw_0, key, value, sbc_query)

    # Combine the results
    query = _merge_query(wfc_query, hrc_query, sbc_query)

    # Perform the query
    if query:
        query_results = query.all()
        num_results = len(query_results)
    else:
        query_results = False
        num_results = 0

    # Put results in a dictinoary
    query_results_dict = {}
    query_results_dict['num_results'] = num_results
    query_results_dict['query_results'] = query_results
    query_results_dict['output_format'] = output_format
    query_results_dict['output_columns'] = output_columns

    return query_results_dict


def _get_session():
    """Return a ``session`` object to be used as a conenction to the
    ``acsql`` database.

    Returns
    -------
    session : obj
        A ``SQLAlchemy`` ``session`` object which serves as a
        connection to the ``acsql`` database.
    """

    engine = create_engine(SETTINGS['connection_string'], echo=False, pool_timeout=100000)
    Session = sessionmaker(bind=engine)
    session = Session()

    return session


def _merge_query(wfc_query, hrc_query, sbc_query):
    """Merge the results from the queries from each table

    Parameters
    ----------
    wfc_query : obj
        The ``SQLAlchemy`` ``query`` object for request columns of the
        WFC table.
    hrc_query : obj
        The ``SQLAlchemy`` ``query`` object for request columns of the
        HRC table.
    sbc_query : obj
        The ``SQLAlchemy`` ``query`` object for request columns of the
        SBC table.

    Returns
    -------
    query : obj
        The ``SQLAlchemy`` ``query`` object with merging applied.
    """

    # Turn off the query if the table is not needed
    if wfc_query:
        if str(wfc_query.statement).find('WHERE') == -1:
            wfc_query = False
    if hrc_query:
        if str(hrc_query.statement).find('WHERE') == -1:
            hrc_query = False
    if sbc_query:
        if str(sbc_query.statement).find('WHERE') == -1:
            sbc_query = False

    # If combination needed
    if wfc_query and hrc_query and sbc_query:
        query = wfc_query.union_all(hrc_query, sbc_query)
    elif wfc_query and not hrc_query and sbc_query:
        query = wfc_query.union_all(sbc_query)
    elif wfc_query and hrc_query and not sbc_query:
        query = wfc_query.union_all(hrc_query)
    elif not wfc_query and hrc_query and sbc_query:
        query = hrc_query.union_all(sbc_query)

    # If no combination needed
    elif wfc_query and not hrc_query and not sbc_query:
        query = wfc_query
    elif not wfc_query and hrc_query and not sbc_query:
        query = hrc_query
    elif not wfc_query and not hrc_query and sbc_query:
        query = sbc_query

    else:
        query = None

    return query
