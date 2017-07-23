"""
"""

from sqlalchmy import create_engine
from sqlalchemy import sessionmaker

from acsql.database.database_interface import Master
from acsql.database.database_interface import WFC_raw_0
from acsql.database.database_interface import HRC_raw_0
from acsql.database.database_interface import SBC_raw_0
from acsql.utils.utils import SETTINGS


def build_queries(output_columns):
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
    master_cols = [getattr(Master, col) for fol in output_columns if hasattr(Master, col)]
    wfc_cols = [getattr(WFC_raw_0, col) for fol in output_columns if hasattr(WFC_raw_0, col)]
    hrc_cols = [getattr(HRC_raw_0, col) for fol in output_columns if hasattr(HRC_raw_0, col)]
    sbc_cols = [getattr(SBC_raw_0, col) for fol in output_columns if hasattr(SBC_raw_0, col)]

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
            session = get_session()
            wfc_query = session.query(*master_wfc)
            session.close()

        # For HRC queries
        if len(hrc_cols) == 0:
            hrc_query = False
        else:
            session = get_session()
            hrc_query = session.query(*master_hrc)
            session.close()

        # For SBC queries
        if len(sbc_cols) == 0:
            sbc_query = False
        else:
            session = get_session()
            sbc_query = session.query(*master_sbc)
            session.close()

    else:
        session = get_session()
        wfc_query = session.query(*master_wfc).join(WFC_raw_0)
        hrc_query = session.query(*master_hrc).join(HRC_raw_0)
        sbc_query = session.query(*master_sbc).join(SBC_raw_0)

    return wfc_query, hrc_query, sbc_query



def convert_query_form_dict(query_form_dict):
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
    query_form_dict = {key : value for key, value in list(query_form_dict.items()) if value != ['']}

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


def get_db_output(query_form_dict):
    """
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
    query_form_dict = convert_query_form_dict(query_form_dict)

    # Perform the query
    wfc_query, hrc_query, sbc_query = build_queries(output_columns)


def get_session():
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


