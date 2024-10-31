import sqlite3
from datetime import datetime
from enum import Enum

class DatabaseSystem(Enum):
    # Enum for predefined systems to ensure consistency
    SQLITE = "sqlite"
    POSTGRES = "postgres"
    DUCKDB = "duckDB"
    SNOWFLAKE = "snowflake"
    #DATABRICKS = "databricks"

class DatabaseObject(Enum):
    # Enum for predefined target objects in experiments
    TABLE = "table"
    INDEX = "index"
    VIEW = "view"
    FUNCTION = "function"
    SEQUENCE = "sequence"
    DATABVASE = "database"

class Granularity(Enum):
    # Enum for predefined granularities in experiments
    XSMALL = 1
    SMALL = 100
    MEDIUM = 1000
    LARGE = 10000
    XLARGE = 100000

class DDLCommand(Enum):
    # Enum for predefined DDL commands in experiments
    CREATE = "CREATE"
    DROP = "DROP"
    ALTER = "ALTER"
    COMMENT = "COMMENT"

class DataRecorder:
    def __init__(self, db_name='experiment_logs.db'):
        self.db_name = db_name
        self.conn = sqlite3.connect(self.db_name)
        # Initialize tables for all systems when we init 
        self._initialize_tables()

    def _initialize_tables(self):
        # Create a table for each system if it doesn't exist - no need to check if it exists when we record
        for system in DatabaseSystem:
            table_name = f"{system.value}_logs"  # So we get sqlite_logs, postgres_logs, etc.
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ddl_command TEXT,
                target_object TEXT,
                granularity INTEGER,
                query_runtime REAL,
                timestamp TEXT
            );
            """
            
            # Seems like the cursor approach is just for batch operations?
            # And the "with" approach is for transactional operations where you want to commit after each operation, otherwise rolled back.
            # it also properly closes the connection after the block.
            with self.conn:
                self.conn.execute(create_table_query)

    def record(self, system: DatabaseSystem, ddl_command: DDLCommand, target_object: DatabaseObject, granularity: Granularity, query_runtime: float):
        table_name = f"{system.value}_logs"  # Dyn table name based on system enum
        insert_query = f"""
        INSERT INTO {table_name} (ddl_command, target_object, granularity, query_runtime, timestamp)
        VALUES (?, ?, ?, ?, ?)
        """
        # Capture the current timestamp for each record 
        timestamp = datetime.now().isoformat()
        # Execute the insert query with transaction management
        with self.conn:
            self.conn.execute(insert_query, (ddl_command.value, target_object.value, granularity.value, query_runtime, timestamp))

    def close(self):
        self.conn.close()

# Example
if __name__ == "__main__":
    # Instantiate DataRecorder to log experiments
    db_recorder = DataRecorder()

    # Mock experiment data for each system using enums
    experiments = [
        (DatabaseSystem.SQLITE, DDLCommand.CREATE, DatabaseObject.TABLE, Granularity.XSMALL, 0.235),
        (DatabaseSystem.POSTGRES, DDLCommand.DROP, DatabaseObject.TABLE, Granularity.SMALL, 0.450),
        (DatabaseSystem.DUCKDB, DDLCommand.ALTER, DatabaseObject.TABLE, Granularity.MEDIUM, 0.123),
        (DatabaseSystem.SNOWFLAKE, DDLCommand.CREATE, DatabaseObject.INDEX, Granularity.LARGE, 0.678),
        (DatabaseSystem.DATABRICKS, DDLCommand.DROP, DatabaseObject.INDEX, Granularity.XLARGE, 0.542)
    ]

    # Record each experiment in the respective system table
    for experiment in experiments:
        # The * operator unpacks the tuple into each attribute for the record method
        db_recorder.record(*experiment)

    # Close the connection once done
    db_recorder.close()
