"""
Main experiment file. Run experiment for all data systems (Sqlite, Postgresql, duckdb, snowflake)
"""

import itertools
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

def _current_task_loading(query: str):
    """Simulate progress loading in termimal. Writes the following: "--running: {query}..." to terminal. The dots should blink while the query is running.

    Args:
        query (str): description to be written to terminal.
    """
    sys.stdout.write(f"\r--running: {query}")
    sys.stdout.flush()  # Force output to update in terminal
    


def create_tables(conn,*, database_system: DatabaseSystem, num_objects: Granularity):
    """Example: Create 1000 tables"""
    with conn:
        for num_exp in range(3):
            print()
            logging.info(f"Repetition {num_exp}")
            for i in range(num_objects.value):
                _current_task_loading(f"CREATE TABLE t_{i} (id INTEGER PRIMARY KEY, value TEXT);")
                
                start_time = datetime.datetime.now()
                conn.execute(f"CREATE TABLE t_{i}_{num_exp} (id INTEGER PRIMARY KEY, value TEXT);")
                conn.commit()
                end_time = datetime.datetime.now()
                
                query_time = end_time - start_time
                record = (
                    database_system,
                    DDLCommand.CREATE,
                    f"CREATE TABLE t_{i} (id INTEGER PRIMARY KEY, value TEXT);",
                    DatabaseObject.TABLE,
                    num_objects,
                    num_exp,
                    query_time.total_seconds(),
                    start_time,
                    end_time,
                )
                recorder.record(*record)
    print()


def alter_table(conn, object_type: DatabaseObject):
    """Example: alter table t_0 add column a"""
    pass


def show_objects():
    """Example: show tables"""
    pass


def select_objects():
    """Example: select * from information_schema.tables"""
    pass


def drop_schema(database_system: DatabaseSystem):
    """Example: drop schema/db"""
    # switch case:
    if database_system == DatabaseSystem.SQLITE:
        if os.path.exists("sqlite.db"):
            os.remove("sqlite.db")
            logging.info(f"Dropped: {database_system}" )
    else:
        logging.error("Database system not dropped")

    
    


def experiment_1(conn, database_system: DatabaseSystem):
    try:
        logging.info("Starting experiment 1!")
        
        for gran in Granularity:
            logging.info(f"Experiment: 1 | Object: {DatabaseObject.TABLE} | Granularity: {gran.value} | Status: started")
            create_tables(conn, database_system=database_system, num_objects=gran)
            logging.info(f"Experiment: 1 | Object: {DatabaseObject.TABLE} | Granularity: {gran.value} | Status: succesful")
            drop_schema(database_system)
            if database_system == DatabaseSystem.SQLITE:
                conn = sqlite3.connect("sqlite.db")
    finally:
        logging.info("Experiment 1 finished.")
        drop_schema(database_system)
        recorder.close()
        
    
    
def main():
    logging.basicConfig(format='%(levelname)s%(funcName)20s():%(message)s', level=logging.INFO)
    experiment_1(sqlite_conn, DatabaseSystem.SQLITE)
    logging.info("Done!")


if __name__ == "__main__":
    main()
