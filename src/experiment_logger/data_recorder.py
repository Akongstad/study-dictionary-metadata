"""
Data recorder. Handle recording of results and experiment metadata.
"""

import sqlite3
from datetime import datetime
from enum import Enum


class DatabaseSystem(Enum):
    """Enum for predefined systems to ensure consistency"""

    SQLITE = "sqlite"
    POSTGRES = "postgres"
    DUCKDB = "duckDB"
    SNOWFLAKE = "snowflake"
    # DATABRICKS = "databricks"


class DatabaseObject(Enum):
    """Enum for predefined target objects in experiments"""

    TABLE = "table"
    INDEX = "index"
    VIEW = "view"
    FUNCTION = "function"
    SEQUENCE = "sequence"
    DATABASE = "database"


class Granularity(Enum):
    """Enum for predefined granularities in experiments"""

    s_1 = 1
    s_10 = 10
    s_100 = 100
    s_1000 = 1000
    s_10000 = 10_000
    s_100000 = 100_000


class DDLCommand(Enum):
    """Enum for predefined DDL commands in experiments"""

    CREATE = "CREATE"
    DROP = "DROP"
    ALTER = "ALTER"
    COMMENT = "COMMENT"
    SHOW = "SHOW"
    INFORMATION_SCHEMA = "INFORMATION_SCHEMA"


class DataRecorder:
    def __init__(self, db_name="experiment_logs.db"):
        self.db_name = db_name
        self.conn = sqlite3.connect(self.db_name)

        # Initialize tables for all systems when we init
        self._initialize_tables()

    def _initialize_tables(self):
        # Create a table for each system if it doesn't exist - no need to check if it exists when we record
        for system in DatabaseSystem:
            table_name = f"{system.value}db_logs"  # So we get sqlite_experiment_logs, postgres_sqlite_experiment_logs_logs, etc.
            create_table_query = f"""CREATE TABLE IF NOT EXISTS {table_name}(id INTEGER PRIMARY KEY AUTOINCREMENT,system_name TEXT,ddl_command TEXT,query_text TEXT,target_object TEXT,granularity INTEGER,repetition_nr INTEGER,query_runtime REAL,start_time DATETIME,end_time DATETIME);"""
            # Seems like the cursor approach is just for batch operations?
            # And the "with" approach is for transactional operations where you want to commit after each operation, otherwise rolled back.
            # it also properly closes the connection after the block.
            with self.conn:
                self.conn.execute(create_table_query)

    def record(
        self,
        system: DatabaseSystem,
        ddl_command: DDLCommand,
        query_text: str,
        target_object: DatabaseObject,
        granularity: Granularity,
        repetition_nr: int,
        query_runtime: float,
        start_time: datetime,
        end_time: datetime,
    ):
        table_name = f"{system.value}db_logs"  # Dyn table name based on system enum
        insert_query = f"""
            INSERT INTO {table_name}(system_name,ddl_command,query_text,target_object,granularity,repetition_nr,query_runtime,start_time,end_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
        """
        # Execute the insert query with transaction management
        record = (
            system.value,
            ddl_command.value,
            query_text,
            target_object.value,
            granularity.value,
            repetition_nr,
            query_runtime,
            start_time,
            end_time,
        )
        with self.conn:
            self.conn.execute(insert_query, record)

    def close(self):
        self.conn.close()


# Example
if __name__ == "__main__":
    # Instantiate DataRecorder to log experiments
    db_recorder = DataRecorder()

    # Mock experiment data for each system using enums
    experiments = [
        (
            DatabaseSystem.SQLITE,
            DDLCommand.CREATE,
            "CREATE TABLE test_table (id INTEGER PRIMARY KEY, value TEXT);",
            DatabaseObject.TABLE,
            Granularity.s_1,
            0,
            0.123,
            datetime.now(),
            datetime.now(),
        ),
    ]

    # Record each experiment in the respective system table
    for experiment in experiments:
        # The * operator unpacks the tuple into each attribute for the record method
        db_recorder.record(*experiment)

    # Close the connection once done
    db_recorder.close()
