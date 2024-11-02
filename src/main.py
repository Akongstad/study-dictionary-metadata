"""
Main experiment file. Run experiment for all data systems (Sqlite, Postgresql, duckdb, snowflake)
"""

import random
import sqlite3
from experiment_logger.data_recorder import (
    DataRecorder,
    DatabaseSystem,
    DatabaseObject,
    Granularity,
    DDLCommand,
)
import datetime
import os
import logging
import sys

recorder = DataRecorder()
# Init connections
# SQLite connection
sqlite_conn = sqlite3.connect("sqlite.db")
# PostgreSQL connection


def _execute_timed_query(conn, query: str):
    """Execute query and log the query time"""
    _current_task_loading(query=query)
    with conn:
        start_time = datetime.datetime.now()
        conn.execute(query)
        conn.commit()
        end_time = datetime.datetime.now()

        query_time = end_time - start_time
    return start_time, end_time, query_time


def _current_task_loading(query: str):
    """Simulate progress loading in termimal. Writes the following: "--running: {query}..." to terminal. The dots should blink while the query is running.

    Args:
        query (str): description to be written to terminal.
    """
    sys.stdout.write(f"\r--running: {query}")
    sys.stdout.flush()  # Force output to update in terminal


def create_tables(conn, *, database_system: DatabaseSystem, num_objects: Granularity):
    """Example: Create 1000 tables"""
    print()
    for i in range(num_objects.value):
        query = f"CREATE TABLE t_{i} (id INTEGER PRIMARY KEY, value TEXT);"

        start_time, end_time, query_time = _execute_timed_query(conn=conn, query=query)
        record = (
            database_system,
            DDLCommand.CREATE,
            f"CREATE TABLE t_{i} (id INTEGER PRIMARY KEY, value TEXT);",
            DatabaseObject.TABLE,
            num_objects,
            0,
            query_time.total_seconds(),
            start_time,
            end_time,
        )
        recorder.record(*record)
    print()


def alter_tables(conn, database_system: DatabaseSystem, granularity: Granularity):
    """Example: alter table t_0 add column a. Point query"""
    table_num = random.randint(0, granularity.value - 1)  # In case of prefetching
    print()
    for num_exp in range(3):
        query = f"ALTER TABLE t_{table_num} ADD COLUMN altered_{num_exp} TEXT;"
        start_time, end_time, query_time = _execute_timed_query(conn=conn, query=query)
        record = (
            database_system,
            DDLCommand.ALTER,
            query,
            DatabaseObject.TABLE,
            granularity,
            num_exp,
            query_time.total_seconds(),
            start_time,
            end_time,
        )
        recorder.record(*record)
        print()
        _comment_object(
            conn, database_system, DatabaseObject.TABLE, granularity, num_exp
        )

    print()


def _comment_object(
    conn,
    database_system: DatabaseSystem,
    database_object: DatabaseObject,
    granularity: Granularity,
    num_exp: int,
):
    """Example if comment supported: alter table t1 set comment = 'This table has been altered'\n
    Example if comment not supported: alter table t1 rename to t1_altered"""

    object_num = random.randint(0, granularity.value - 1)
    if (
        database_system == DatabaseSystem.SQLITE
        and database_object.value == "table"
    ):
            # ALTER column name
            query = f"alter table t_{object_num} RENAME COLUMN value TO value_altered;"
    else:
        query = f"alter {database_object.value[0].lower()}_{object_num} set comment 'This table has been altered';"
    _current_task_loading(query)
    start_time = datetime.datetime.now()
    conn.execute(query)
    conn.commit()
    end_time = datetime.datetime.now()

    query_time = end_time - start_time
    record = (
        database_system,
        DDLCommand.COMMENT,
        query,
        DatabaseObject.TABLE,
        granularity,
        num_exp,
        query_time.total_seconds(),
        start_time,
        end_time,
    )
    recorder.record(*record)

    # Clean up. For sqlite
    if (
        database_system == DatabaseSystem.SQLITE
        and database_object.value == "table"
    ):
        query = f"alter table t_{object_num} RENAME COLUMN value_altered TO value;"
        conn.execute(query)
        conn.commit()


def show_objects(database_system: DatabaseSystem):
    """Example: show tables"""
    query = "show tables"


def select_objects():
    """Example: select * from information_schema.tables"""
    pass


def drop_schema(database_system: DatabaseSystem):
    """Example: drop schema/db"""
    # switch case:
    if database_system == DatabaseSystem.SQLITE:
        if os.path.exists("sqlite.db"):
            os.remove("sqlite.db")
            logging.info(f"Dropped: {database_system}")
    else:
        logging.error("Database system not dropped")


def experiment_1(conn, database_system: DatabaseSystem):
    try:
        logging.info("Starting experiment 1!")

        for gran in Granularity:
            logging.info(
                f"Experiment: 1 | Object: {DatabaseObject.TABLE} | Granularity: {gran.value} | Status: started"
            )
            create_tables(conn, database_system=database_system, num_objects=gran)
            alter_tables(conn, database_system, gran)

            drop_schema(database_system)
            if database_system == DatabaseSystem.SQLITE:
                conn = sqlite3.connect("sqlite.db")
    finally:
        logging.info("Experiment 1 finished.")
        drop_schema(database_system)
        recorder.close()


def main():
    logging.basicConfig(
        format="%(levelname)s%(funcName)20s():%(message)s", level=logging.INFO
    )
    experiment_1(sqlite_conn, DatabaseSystem.SQLITE)
    logging.info("Done!")


if __name__ == "__main__":
    main()
