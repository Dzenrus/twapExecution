import pathlib
import sqlite3
from sqlite3 import Error
import os


def create_connection(db_file):
    """
    Create a database connection to the SQLite database specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql):
    """
    Create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def create_execution(conn, project):
    """
    Create a new project into the execution table
    :param conn:
    :param project:
    :return: project id
    """
    sql = ''' INSERT INTO execution (exchange, market, order_id, time, symbol, price, side, qty, executed_qty, 
                average_price, remaining_qty, complete_flag) VALUES(?,?,?,?,?,?,?,?,?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, project)
    conn.commit()
    return cur.lastrowid


def execute(db_name, exchange, market, order_id, symbol, time, price, side, qty, overall_average, remaining_qty,
            executed_qty, complete_flag):
    """
    Writes into the database file
    :param db_name: named below
    :param exchange: binance, okex, etc
    :param market: spot or futures
    :param order_id: order returned from trade
    :param symbol: btc-usdt
    :param time: time of execution
    :param price: average price of execution
    :param side: side of execution, buy or sell
    :param qty: trade amount
    :param overall_average: average of trade
    :param remaining_qty: remaining amount of trades to be completed
    :param executed_qty: amount executed
    :param complete_flag: when the trade is completed, it will be flagged complete

    :return: null
    """

    # connects to the sqlite file
    database = os.path.join(pathlib.Path().absolute(), f'{db_name}.db')

    sql_create_executions_table = """ CREATE TABLE IF NOT EXISTS execution (
                                        id integer PRIMARY KEY,
                                        exchange varchar,
                                        market varchar,
                                        order_id integer,
                                        time date NOT NULL,
                                        symbol varchar,
                                        price float,
                                        side varchar,
                                        qty float,
                                        executed_qty float,
                                        average_price float,
                                        remaining_qty float,
                                        complete_flag boolean
                                    ); """

    # create a database connection
    conn = create_connection(database)

    if conn is not None:
        # create execution table
        create_table(conn, sql_create_executions_table)

    else:
        print("Error! cannot create the database connection.")

    with conn:
        # create a new project
        project = (
            exchange, market, order_id, time, symbol, price, side, qty, executed_qty, overall_average, remaining_qty,
            complete_flag)
        create_execution(conn, project)
