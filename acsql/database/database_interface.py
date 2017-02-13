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

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from acsql.utils.utils import SETTINGS


def loadConnection(connection_string):
    """Return session, base, and engine objects for connecting to the
    acsql database.

    Create and engine using an given connection_string. Create a Base
    class and Session class from the engine. Create an instance of the
    Session class. Return the session, base, and engine instances.

    Parameters
    ----------
    connection_string : str
        The connection string to connect to the acsql database.

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
    engine = create_engine(connection_string, echo=False, pool_timeout=259200)
    base = declarative_base(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    return session, base, engine

session, base, engine = loadConnection(SETTINGS['connection_string'])
