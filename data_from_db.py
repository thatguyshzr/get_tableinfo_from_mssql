from urllib import parse
import pyodbc
from sqlalchemy import create_engine
import json
import pandas as pd
import argparse

def setup_connection(driver, server_name, db_name, username, password):
    """connect to database

    Args:
        driver (str): SQL driver for pyodbc
        server_name (str): server name
        db_name (str): database name
        username (str): username
        password (str): password

    Returns:
        sqlalchemy.engine.base.Connection: sql connection engine
    """
    print('Connecting to: '+ server_name)
    params= parse.qoute_plus \
    (r'Driver={'+driver+'};Server='+server_name+';Database='+db_name+';Uid='+username+';Pwd={'+password+'};TrustServerCertificate=no;Connection Timeout=30;')
    conn_str= "mssql+pyodbc:///?odbc_connect={}".format(params)
    engine= create_engine(conn_str, echo=False)

    return engine.connect()

def get_metadata(connection, table_name, table_schema):
    """get table information from database

    Args:
        connection (sqlalchemy.engine.base.connection): sql connection engine
        table_name (str): table name to get information of
        table_schema (str): table schema to get info of

    Returns:
        dataframe: dataframe with table info
    """
    query= "SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, \
            CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION FROM INFORMATION_SCHEMA.COLUMNS \
            WHERE TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = '{table_schema}' \
            ORDER BY ORDINAL_POSITION;"
    query1= query.format(table_name= table_name, table_schema= table_schema)
    df= pd.read_sql_query(query1, connection)
    connection.close()
    return df

with open('setup.json') as f:
    setup= json.load(f)

drivers= pyodbc.drivers()

connect= setup_connection(drivers[0], setup['server'], setup['database'], setup['user'], setup['password'])

tablename= input('Table name: ')
get_metadata(connect, setup['tablename'], setup['tableschema'].lower()).to_excel(setup['tablename']+'.xlsx', index= False)

