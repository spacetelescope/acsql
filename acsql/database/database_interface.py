"""This module provides ORMs for the acsql database, as well as engine
and session objects for connecting to the database.

The load_connection() function within this module allows the user to
connect to the acsql database via the session, base, and engine objects
(described below).  The classes within serve as ORMs (Object-relational
mappings) that define the individual tables of the relational database.

The engine object serves as the low-level database API and perhaps most
importantly contains dialects which allows the sqlalchemy module to
communicate with the database.

The base object serves as a base class for class definitions.  It
produces Table objects and constructs ORMs.

The session object manages operations on ORM-mapped objects, as
construced by the base.  These operations include querying, for
example.

Authors
-------
    Matthew Bourque, 2017

Use
---
    This module is intended to be imported from various acsql modules
    and scripts.  The importable objects from this module are as
    follows:

    from acsql.database.database_interface import base
    from acsql.database.database_interface import engine
    from acsql.database.database_interface import session

Dependencies
------------
    External library dependencies include:

    (1) sqlalchemy
"""

import os

from sqlalchemy import Boolean
from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import Date
from sqlalchemy import DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Index
from sqlalchemy import Enum
from sqlalchemy import Float
from sqlalchemy import ForeignKey
from sqlalchemy import ForeignKeyConstraint
from sqlalchemy import Integer
from sqlalchemy.orm import sessionmaker
from sqlalchemy import String
from sqlalchemy import Time

from acsql.utils.utils import SETTINGS, WFC_FILE_EXTS, SBC_FILE_EXTS


def define_columns(data_dict, class_name):
    """Dynamically define the class attributes for the ORM

    Parameters
    ----------
    data_dict : dict
        A dictionary containing the ORM definitions
    class_name : str
        The name of the class/ORM.

    Returns
    -------
    data_dict : dict
        A dictionary containing the ORM definitions, now with header
        definitions added.
    """

    with open(os.path.join(os.path.split(__file__)[0], 'table_definitions',
                           class_name.lower() + '.txt'), 'r') as f:
        data = f.readlines()
    keywords = [item.strip().split(', ') for item in data]
    for keyword in keywords:
        if keyword[1] == 'Integer':
            data_dict[keyword[0].lower()] = Column(Integer())
        elif keyword[1] == 'String':
            data_dict[keyword[0].lower()] = Column(String(100))
        elif keyword[1] == 'Float':
            data_dict[keyword[0].lower()] = Column(Float())
        elif keyword[1] == 'Decimal':
            data_dict[keyword[0].lower()] = Column(Float(precision='13,8'))
        elif keyword[1] == 'Date':
            data_dict[keyword[0].lower()] = Column(Date())
        elif keyword[1] == 'Time':
            data_dict[keyword[0].lower()] = Column(Time())
        elif keyword[1] == 'DateTime':
            data_dict[keyword[0].lower()] = Column(DateTime)
        elif keyword[1] == 'Bool':
            data_dict[keyword[0].lower()] = Column(Boolean)
        else:
            raise ValueError('unrecognized header keyword type: {}:{}'.format(
                keyword[0], keyword[1]))

        if 'aperture' in data_dict:
            data_dict['aperture'] = Column(String(100), index=True)

    return data_dict


def loadConnection(connection_string):
    """Return session, base, and engine objects for connecting to the
    acsql database.

    Create and engine using an given connection_string. Create a Base
    class and Session class from the engine. Create an instance of the
    Session class. Return the session, base, and engine instances.

    Parameters
    ----------
    connection_string : str
        The connection string to connect to the acsql database.  The
        connection string should take the form:
        ``dialect+driver://username:password@host:port/database``

    Returns
    -------
    session : sesson object
        Provides a holding zone for all objects loaded or associated
        with the database.
    base : base object
        Provides a base class for declarative class definitions.
    engine : engine object
        Provides a source of database connectivity and behavior.
    """
    engine = create_engine(connection_string, echo=False, pool_timeout=100000)
    base = declarative_base(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    return session, base, engine


session, base, engine = loadConnection(SETTINGS['connection_string'])


def orm_factory(class_name):
    """Create a SQLAlchemy ORM Classes with the given class_name.

    Parameters
    ----------
    class_name : str
        The name of the class to be created

    Returns
    -------
    class : obj
        The SQLAlchemy ORM
    """

    data_dict = {}
    data_dict['__tablename__'] = class_name.lower()
    data_dict['rootname'] = Column(String(8), ForeignKey('master.rootname'),
                                   primary_key=True, index=True,
                                   nullable=False)
    data_dict['filename'] = Column(String(18), nullable=False, unique=True)
    data_dict = define_columns(data_dict, class_name)

    return type(class_name.upper(), (base,), data_dict)


class Master(base):
    """ORM for the  master table."""
    def __init__(self, data_dict):
        self.__dict__.update(data_dict)

    __tablename__ = 'master'
    rootname = Column(String(8), primary_key=True, index=True, nullable=False)
    path = Column(String(43), unique=True, nullable=False)
    first_ingest_date = Column(Date, nullable=False)
    last_ingest_date = Column(Date, nullable=False)
    detector = Column(Enum('WFC', 'HRC', 'SBC'), nullable=False)
    obstype = Column(Enum('CAL', 'GO'), nullable=True)


class Datasets(base):
    """ORM for the datasets table."""
    def __init__(self, data_dict):
        self.__dict__.update(data_dict)

    __tablename__ = 'datasets'
    rootname = Column(String(8), ForeignKey('master.rootname'),
                      primary_key=True, index=True, nullable=False)
    raw = Column(String(18), nullable=True)
    flt = Column(String(18), nullable=True)
    flc = Column(String(18), nullable=True)
    spt = Column(String(18), nullable=True)
    drz = Column(String(18), nullable=True)
    drc = Column(String(18), nullable=True)
    crj = Column(String(18), nullable=True)
    crc = Column(String(18), nullable=True)
    jif = Column(String(18), nullable=True)
    jit = Column(String(18), nullable=True)
    asn = Column(String(18), nullable=True)

    # foreign_keys = []
    # for filetype in FILE_EXTS:
    #     for ext in [0, 1]:
    #         foreign_keys.append(ForeignKeyConstraint([filetype],
    #             ['wfc_{}_{}.filename'.format(filetype, ext)]))
    # foreign_keys = tuple(foreign_keys)
    # __table_args__ = foreign_keys


# WFC tables
WFC_raw_0 = orm_factory('WFC_raw_0')
WFC_raw_1 = orm_factory('WFC_raw_1')
WFC_raw_2 = orm_factory('WFC_raw_2')
WFC_raw_3 = orm_factory('WFC_raw_3')
WFC_raw_4 = orm_factory('WFC_raw_4')
WFC_raw_5 = orm_factory('WFC_raw_5')
WFC_raw_6 = orm_factory('WFC_raw_6')

WFC_flt_0 = orm_factory('WFC_flt_0')
WFC_flt_1 = orm_factory('WFC_flt_1')
WFC_flt_2 = orm_factory('WFC_flt_2')
WFC_flt_3 = orm_factory('WFC_flt_3')
WFC_flt_4 = orm_factory('WFC_flt_4')
WFC_flt_5 = orm_factory('WFC_flt_5')
WFC_flt_6 = orm_factory('WFC_flt_6')

WFC_flc_0 = orm_factory('WFC_flc_0')
WFC_flc_1 = orm_factory('WFC_flc_1')
WFC_flc_2 = orm_factory('WFC_flc_2')
WFC_flc_3 = orm_factory('WFC_flc_3')
WFC_flc_4 = orm_factory('WFC_flc_4')
WFC_flc_5 = orm_factory('WFC_flc_5')
WFC_flc_6 = orm_factory('WFC_flc_6')

WFC_spt_0 = orm_factory('WFC_spt_0')
WFC_spt_1 = orm_factory('WFC_spt_1')

WFC_drz_0 = orm_factory('WFC_drz_0')
WFC_drz_1 = orm_factory('WFC_drz_1')
WFC_drz_2 = orm_factory('WFC_drz_2')
WFC_drz_3 = orm_factory('WFC_drz_3')

WFC_drc_0 = orm_factory('WFC_drc_0')
WFC_drc_1 = orm_factory('WFC_drc_1')
WFC_drc_2 = orm_factory('WFC_drc_2')
WFC_drc_3 = orm_factory('WFC_drc_3')

WFC_crj_0 = orm_factory('WFC_crj_0')
WFC_crj_1 = orm_factory('WFC_crj_1')
WFC_crj_2 = orm_factory('WFC_crj_2')
WFC_crj_3 = orm_factory('WFC_crj_3')
WFC_crj_4 = orm_factory('WFC_crj_4')
WFC_crj_5 = orm_factory('WFC_crj_5')
WFC_crj_6 = orm_factory('WFC_crj_6')

WFC_crc_0 = orm_factory('WFC_crc_0')
WFC_crc_1 = orm_factory('WFC_crc_1')
WFC_crc_2 = orm_factory('WFC_crc_2')
WFC_crc_3 = orm_factory('WFC_crc_3')
WFC_crc_4 = orm_factory('WFC_crc_4')
WFC_crc_5 = orm_factory('WFC_crc_5')
WFC_crc_6 = orm_factory('WFC_crc_6')

WFC_jif_0 = orm_factory('WFC_jif_0')
WFC_jif_1 = orm_factory('WFC_jif_1')
WFC_jif_2 = orm_factory('WFC_jif_2')
WFC_jif_3 = orm_factory('WFC_jif_3')
WFC_jif_4 = orm_factory('WFC_jif_4')
WFC_jif_5 = orm_factory('WFC_jif_5')
WFC_jif_6 = orm_factory('WFC_jif_6')

WFC_jit_0 = orm_factory('WFC_jit_0')
WFC_jit_1 = orm_factory('WFC_jit_1')
WFC_jit_2 = orm_factory('WFC_jit_2')
WFC_jit_3 = orm_factory('WFC_jit_3')
WFC_jit_4 = orm_factory('WFC_jit_4')
WFC_jit_5 = orm_factory('WFC_jit_5')
WFC_jit_6 = orm_factory('WFC_jit_6')

WFC_asn_0 = orm_factory('WFC_asn_0')
WFC_asn_1 = orm_factory('WFC_asn_1')


# HRC tables
HRC_raw_0 = orm_factory('HRC_raw_0')
HRC_raw_1 = orm_factory('HRC_raw_1')
HRC_raw_2 = orm_factory('HRC_raw_2')
HRC_raw_3 = orm_factory('HRC_raw_3')

HRC_flt_0 = orm_factory('HRC_flt_0')
HRC_flt_1 = orm_factory('HRC_flt_1')
HRC_flt_2 = orm_factory('HRC_flt_2')
HRC_flt_3 = orm_factory('HRC_flt_3')

HRC_spt_0 = orm_factory('HRC_spt_0')
HRC_spt_1 = orm_factory('HRC_spt_1')

HRC_drz_0 = orm_factory('HRC_drz_0')
HRC_drz_1 = orm_factory('HRC_drz_1')
HRC_drz_2 = orm_factory('HRC_drz_2')
HRC_drz_3 = orm_factory('HRC_drz_3')

HRC_crj_0 = orm_factory('HRC_crj_0')
HRC_crj_1 = orm_factory('HRC_crj_1')
HRC_crj_2 = orm_factory('HRC_crj_2')
HRC_crj_3 = orm_factory('HRC_crj_3')

HRC_jif_0 = orm_factory('HRC_jif_0')
HRC_jif_1 = orm_factory('HRC_jif_1')
HRC_jif_2 = orm_factory('HRC_jif_2')

HRC_jit_0 = orm_factory('HRC_jit_0')
HRC_jit_1 = orm_factory('HRC_jit_1')
HRC_jit_2 = orm_factory('HRC_jit_2')

HRC_asn_0 = orm_factory('HRC_asn_0')
HRC_asn_1 = orm_factory('HRC_asn_1')


# SBC tables
SBC_raw_0 = orm_factory('SBC_raw_0')
SBC_raw_1 = orm_factory('SBC_raw_1')
SBC_raw_2 = orm_factory('SBC_raw_2')
SBC_raw_3 = orm_factory('SBC_raw_3')

SBC_flt_0 = orm_factory('SBC_flt_0')
SBC_flt_1 = orm_factory('SBC_flt_1')
SBC_flt_2 = orm_factory('SBC_flt_2')
SBC_flt_3 = orm_factory('SBC_flt_3')

SBC_spt_0 = orm_factory('SBC_spt_0')
SBC_spt_1 = orm_factory('SBC_spt_1')

SBC_drz_0 = orm_factory('SBC_drz_0')
SBC_drz_1 = orm_factory('SBC_drz_1')
SBC_drz_2 = orm_factory('SBC_drz_2')
SBC_drz_3 = orm_factory('SBC_drz_3')

SBC_jif_0 = orm_factory('SBC_jif_0')
SBC_jif_1 = orm_factory('SBC_jif_1')
SBC_jif_2 = orm_factory('SBC_jif_2')

SBC_jit_0 = orm_factory('SBC_jit_0')
SBC_jit_1 = orm_factory('SBC_jit_1')
SBC_jit_2 = orm_factory('SBC_jit_2')

SBC_asn_0 = orm_factory('SBC_asn_0')
SBC_asn_1 = orm_factory('SBC_asn_1')


if __name__ == '__main__':

    # Give user a second chance
    prompt = ('About to reset all table(s) for database instance {}. Do you '
        'wish to proceed? (y/n)\n'.format(SETTINGS['connection_string']))

    response = input(prompt)

    if response.lower() == 'y':
        print('Resetting table(s)')
        base.metadata.drop_all()
        base.metadata.create_all()
