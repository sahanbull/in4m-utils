import logging
from sqlalchemy import create_engine

DATABASE_DRIVERS = {"mysql", "postgresql"}

logger = logging.getLogger(__name__)

def _validate_db_params(driver, host, port, username, pwd):
    """
    Validates the database connection parameters read from the config
    Args:
        driver (str): database type (mysql, postgres etc...)
        host (str): the address of the database server (ip or localhost or url)
        port (int): port the database server is listening to
        username (str): the username of the database
        pwd (str): the password for the database
    """
    # check the database driver
    if driver not in DATABASE_DRIVERS:
        message = "Unexpected database driver value... {0} found !!".format(driver)
        logger.exception(message, exc_info=True)
        raise ValueError(message)

    # validate string values
    for k, v in {"host" : host,
                 "username": username,
                 "password": pwd}.items():
        if type(v) != str:
            message = "Database {0} value should be a string.. {1} found!!".format(k, v)
            logger.exception(message, exc_info=True)
            raise TypeError(message)

    # validate integer values
    if type(port) != int:
        message = "Database port value should be a integer.. {0} found!!".format(port)
        logger.exception(message, exc_info=True)
        raise TypeError(message)


def get_connection_string(driver, host, port, username, pwd, db=None):
    """Takes in the db params and generates the connection string for sqlAlchemy without the database name
    Args:
        driver (str): database type (mysql, postgres etc...)
        host (str): the address of the database server (ip or localhost or url)
        port (int): port the database server is listening to
        username (str): the username of the database
        pwd (str): the password for the database
    Returns:
        connection_string (str) : the connection string with all the parameters except the DB name
    """
    _validate_db_params(driver, host, port, username, pwd)
    connection_string = "{0}://{1}:{2}@{3}:{4}".format(driver, username, pwd, host, port)
    if not db is None:
        connection_string+= "/{}".format(db)
    return connection_string


def _create_db_engine(driver, host, port, username, pwd, db):
    """Takes in the db params and generates the database connection
    Args:
        driver (str): database type (mysql, postgres etc...)
        host (str): the address of the database server (ip or localhost or url)
        port (int): port the database server is listening to
        username (str): the username of the database
        pwd (str): the password for the database
        db (str): the database name
    Returns:
        (db engine object): sqlAlchemy engine object
    """
    return create_engine(get_connection_string(driver, host, port, username, pwd, db=db), echo=True,
                         encoding='utf8')


def get_db_engine(driver, host, port, username, pwd, db):
    """
    Returns the DB engine object if there are no errors
    Returns:
        (db engine object) : sqlAlchemy engine object
    """
    return _create_db_engine(driver, host, port, username, pwd, db)


def is_table_in_database(engine, table_name):
    """
    Checks if a table is in the database
    Args:
        engine (Engine): SQLAlchemy engine object
        table_name (str): Table name
    Returns:
        (bool): if the table exists or not
    """
    return engine.dialect.has_table(engine, table_name)


def create_tables_in_db(metadata):
    """Creates the tables needed in the database
    Args:
        metadata (MetaData): metadata that has the schema that has to be generated/ validated
    """
    logging.info("Creating database tables ... ")
    metadata.create_all()
    logging.info("Tables created successfully!!!")


def remove_tables_in_db(engine, tables):
    """Removes the tables from the  database
    Args:
        engine (Connection): sqlAlchemy connection engine object
        tables ([str]): list of table names to be dropped
    """
    logging.info("Dropping all database tables ... ")
    # create metadata object
    sql_query = "DROP TABLE {0}"

    # TODO: use ORM
    for table in tables:
        try:
            engine.execute(sql_query.format(table))
        except:
            continue

    logging.info("Tables removed successfully!!!")