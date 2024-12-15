"""Generate sql scripts to run as batch process with sqlite, duckdb, postgresql
Isolates the database’s processing time for view creation, excluding Python’s processing time. Uses the timing functionality in the respective data systems.
"""

import os


def create_script(table_name, datasystem: str, repetitions: int) -> str:
    """create or write to sql file. Create table. path_to_folder/batch"""
    sql_code = f"CREATE TABLE IF NOT EXISTS {table_name} (id INTEGER PRIMARY KEY, value TEXT);\n"

    file_path = (
        f"{os.path.dirname(os.path.realpath(__file__))}/batch/{datasystem}_view_benchmark.sql"
    )
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w") as f:
        f.write("BEGIN TRANSACTION;\n")
        f.write(sql_code)

    generate_views(file_path, repetitions)

    return file_path


def generate_views(path_to_file, num_views: int) -> None:
    """create or write to sql file. Create views. path_to_folder/batch"""
    with open(path_to_file, "a") as f:
        for i in range(num_views):
            view_name = f"view_{i}"
            f.write(f"CREATE VIEW {view_name} AS SELECT * FROM base_table;\n")
        f.write("COMMIT;\n")
    return


def main():
    datasystem = "sqlite"
    base_table_name = "base_table"
    num_views = 100
    create_script(base_table_name, datasystem, num_views)

    datasystem = "postgresql"
    base_table_name = "base_table"
    num_views = 100
    create_script(base_table_name, datasystem, num_views)

    print("Done!")


if __name__ == "__main__":
    main()
