"""
Main experiment file. Run experiment for all data systems (Sqlite, Postgresql, duckdb, snowflake)
"""

import random
import sqlite3
import duckdb
import datetime
import os
import logging
import sys
import psycopg2
import configs
import snowflake.connector
import yaml

from experiment_logger.data_recorder import (
    DataRecorder,
    DatabaseSystem,
    DatabaseObject,
    Granularity,
    DDLCommand,
)

recorder = DataRecorder()


# Init connections
def connect_sqlite() -> sqlite3.Connection:
    return sqlite3.connect("sqlite.db")


def connect_duckdb() -> duckdb.DuckDBPyConnection:
    return duckdb.connect("duckdb.db")


def connect_postgres() -> psycopg2.extensions.connection:
    postgres_conf = yaml.safe_load(open(".config.yaml"))["postgres_conf"]
    return psycopg2.connect(**configs.postgres_conf)


def connect_snowflake() -> snowflake.connector.connection.SnowflakeConnection:
    snowflake_conf = yaml.safe_load(open(".config.yaml"))["snowflake_conf"]
    return snowflake.connector.connect(**snowflake_conf)


def _execute_timed_query(conn, database_system: DatabaseSystem, query: str) -> tuple[datetime.datetime, datetime.datetime, datetime.timedelta]:
    """Execute query and log the query time"""
    _current_task_loading(query=query)
    if database_system == DatabaseSystem.SQLITE:
        with conn:
            start_time = datetime.datetime.now()
            conn.execute(query)
            conn.commit()
            end_time = datetime.datetime.now()

            query_time = end_time - start_time

    elif database_system == DatabaseSystem.DUCKDB:
        start_time = datetime.datetime.now()
        conn.execute(query)
        end_time = datetime.datetime.now()

        query_time = end_time - start_time

    elif database_system == DatabaseSystem.POSTGRES:
        with conn:
            with conn.cursor() as curs:
                start_time = datetime.datetime.now()
                curs.execute(query)
                end_time = datetime.datetime.now()

                query_time = end_time - start_time

    elif database_system == DatabaseSystem.SNOWFLAKE:
        with conn.cursor() as curs:
            start_time = datetime.datetime.now()
            curs.execute(query)
            end_time = datetime.datetime.now()

            query_time = end_time -start_time

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

    if database_system == DatabaseSystem.DUCKDB:
        init_query = """CREATE SCHEMA experiment;
                        use experiment;"""
        _ = _execute_timed_query(
            conn=conn, query=init_query, database_system=database_system
        )
    if database_system == DatabaseSystem.SNOWFLAKE:
        with conn.cursor() as curs:
            curs.execute("CREATE SCHEMA metadata_experiment")
            curs.execute("use schema metadata_experiment;")

    for i in range(num_objects.value):
        query = f"CREATE TABLE t_{i} (id INTEGER PRIMARY KEY, value TEXT);"

        start_time, end_time, query_time = _execute_timed_query(
            conn=conn, query=query, database_system=database_system
        )
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


def alter_tables(
    conn, *, database_system: DatabaseSystem, granularity: Granularity, num_exp
):
    """Example: alter table t_0 add column a. Point query"""
    print()
    table_num = random.randint(0, granularity.value - 1)  # In case of prefetching
    query = f"ALTER TABLE t_{table_num} ADD COLUMN altered_{num_exp} TEXT;"
    start_time, end_time, query_time = _execute_timed_query(
        conn=conn, query=query, database_system=database_system
    )
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
        conn,
        database_system=database_system,
        database_object=DatabaseObject.TABLE,
        granularity=granularity,
        num_exp=num_exp,
    )
    print()


def _comment_object(
    conn,
    *,
    database_system: DatabaseSystem,
    database_object: DatabaseObject,
    granularity: Granularity,
    num_exp: int,
):
    """Example if comment supported: alter table t1 set comment = 'This table has been altered'\n
    Example if comment not supported: alter table t1 rename to t1_altered"""

    object_num = random.randint(0, granularity.value - 1)
    if database_system == DatabaseSystem.SQLITE and database_object.value == "table":
        # ALTER column name
        query = f"alter {database_object.value} t_{object_num} RENAME COLUMN value TO value_altered;"
    else:
        query = f"comment on {database_object.value} t_{object_num} is 'This {database_object.value} has been altered';"

    _current_task_loading(query)
    start_time, end_time, query_time = _execute_timed_query(
        conn=conn, query=query, database_system=database_system
    )
    record = (
        database_system,
        DDLCommand.COMMENT,
        query,
        database_object,
        granularity,
        num_exp,
        query_time.total_seconds(),
        start_time,
        end_time,
    )
    recorder.record(*record)

    # Clean up. For sqlite
    if database_system == DatabaseSystem.SQLITE and database_object.value == "table":
        with conn:
            query = f"alter table t_{object_num} RENAME COLUMN value_altered TO value;"
            conn.execute(query)
            conn.commit()


def show_objects(
    conn,
    *,
    database_system: DatabaseSystem,
    database_object: DatabaseObject,
    granularity: Granularity,
    num_exp,
):
    """Example: show tables"""
    if database_system == DatabaseSystem.SQLITE:
        query = f"""SELECT name FROM sqlite_master WHERE type = '{database_object.value}';"""
    elif database_system == DatabaseSystem.POSTGRES:
        query = f"""
        SELECT n.nspname AS schema_name,
       c.relname AS table_name
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'r'  -- 'r' indicates a regular table
        AND n.nspname NOT IN ('pg_catalog', 'information_schema'); -- Exclude system schemas
        """
    else:
        query = "show tables"
    start_time, end_time, query_time = _execute_timed_query(
        conn=conn, query=query, database_system=database_system
    )
    record = (
        database_system,
        DDLCommand.SHOW,
        query,
        database_object,
        granularity,
        num_exp,
        query_time.total_seconds(),
        start_time,
        end_time,
    )
    recorder.record(*record)
    print()


def select_objects(
    conn,
    *,
    database_system: DatabaseSystem,
    database_object: DatabaseObject,
    granularity: Granularity,
    num_exp,
):
    """Example: select * from information_schema.tables"""
    if database_system == DatabaseSystem.SQLITE:
        query = (
            f"""SELECT * FROM sqlite_master WHERE type = '{database_object.value}';"""
        )
    else:
        query = f"select * from information_schema.{database_object.value}s"

    start_time, end_time, query_time = _execute_timed_query(
        conn=conn, query=query, database_system=database_system
    )
    record = (
        database_system,
        DDLCommand.INFORMATION_SCHEMA,
        query,
        database_object,
        granularity,
        num_exp,
        query_time.total_seconds(),
        start_time,
        end_time,
    )
    recorder.record(*record)
    print()


def drop_schema(conn, database_system: DatabaseSystem):
    """Example: drop schema/db"""
    # switch case:
    try:
        if database_system == DatabaseSystem.SQLITE:
            if os.path.exists("sqlite.db"):
                os.remove("sqlite.db")
                logging.info(f"Dropped: {database_system}")
        if database_system == DatabaseSystem.DUCKDB:
            drop_query = "DROP SCHEMA experiment CASCADE;"
            _ = _execute_timed_query(
                conn=conn, query=drop_query, database_system=database_system
            )
        if database_system == DatabaseSystem.POSTGRES:
            drop_query = """DROP SCHEMA public CASCADE;
                            CREATE SCHEMA public;"""
            _ = _execute_timed_query(
                conn=conn, query=drop_query, database_system=database_system
            )
        if database_system == DatabaseSystem.SNOWFLAKE:
            with conn:
                with conn.cursor() as curs:
                    curs.execute("use schema public")
                    curs.execute("DROP SCHEMA if exists metadata_experiment CASCADE;")

        else:
            logging.error("Database system not dropped")
    except Exception as e:
        logging.error(f"Drop schema failed: {e}")


def experiment_1(conn, database_system: DatabaseSystem):
    try:
        logging.info("Starting experiment 1!")

        for gran in Granularity:
            if database_system == DatabaseSystem.SQLITE:
                conn = sqlite3.connect("sqlite.db")
            # elif database_system == DatabaseSystem.DUCKDB:
            #     conn = duckdb.connect("duckdb.db")
            # elif database_system == DatabaseSystem.POSTGRES:
            #     conn = psycopg2.connect(**configs.postgres_conf)
            # elif database_system == DatabaseSystem.SNOWFLAKE:
            #     conn = connect_snowflake()
            logging.info(
                f"Experiment: 1 | Object: {DatabaseObject.TABLE} | Granularity: {gran.value} | Status: started"
            )

            create_tables(conn, database_system=database_system, num_objects=gran)
            for num_exp in range(3):
                alter_tables(
                    conn,
                    database_system=database_system,
                    granularity=gran,
                    num_exp=num_exp,
                )
                show_objects(
                    conn,
                    database_system=database_system,
                    database_object=DatabaseObject.TABLE,
                    granularity=gran,
                    num_exp=num_exp,
                )
                select_objects(
                    conn,
                    database_system=database_system,
                    database_object=DatabaseObject.TABLE,
                    granularity=gran,
                    num_exp=num_exp,
                )
            logging.info(
                f"Experiment: 1 | Object: {DatabaseObject.TABLE} | Granularity: {gran.value} | Status: SUCCESSFUL"
            )
            drop_schema(conn, database_system)
    except Exception as e:
        logging.error(f"Experiment 1 failed: {e}")
        drop_schema(conn, database_system)
    finally:
        logging.info("Experiment 1 finished.")
        recorder.close()


def main():
    logging.basicConfig(
        format="%(levelname)s%(funcName)20s():%(message)s", level=logging.INFO
    )
    try:
        # sqlit_conn = connect_sqlite()
        # drop_schema(connect_sqlite(), DatabaseSystem.SQLITE)
        # experiment_1(sqlit_conn, DatabaseSystem.SQLITE)

        # duckdb_conn = connect_duckdb()
        # drop_schema(duckdb_conn, DatabaseSystem.DUCKDB)
        # experiment_1(duckdb_conn, DatabaseSystem.DUCKDB)

        psql_conn = connect_postgres()
        drop_schema(psql_conn, DatabaseSystem.POSTGRES)
        experiment_1(psql_conn, DatabaseSystem.POSTGRES)

        # snowflake_conn = connect_snowflake()
        # drop_schema(snowflake_conn, DatabaseSystem.SNOWFLAKE)
        # experiment_1(snowflake_conn, DatabaseSystem.SNOWFLAKE)

        logging.info("Done!")
    finally:
        logging.info("Closing all connections")
        recorder.close()
        # sqlit_conn.close()
        # duckdb_conn.close()
        psql_conn.close()
        # snowflake_conn.close()


if __name__ == "__main__":
    main()
