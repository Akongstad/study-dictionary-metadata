# Commands for exporting log data from the sqlite log database to csv files.

```bash
# sqlite
sqlite3 experiment_logs.db
.headers on
.mode csv
.output exports/sqlitedb_logs.csv
SELECT * FROM sqlitedb_logs;
.output stdout
.exit 0;
```

```bash
#duckdb
sqlite3 experiment_logs.db
.headers on
.mode csv
.output exports/duckdb_logs.csv
SELECT * FROM duckDBdb_logs;
.output stdout
.exit 0;
```

```bash
#psql
sqlite3 experiment_logs.db
.headers on
.mode csv
.output exports/postgresdb_logs.csv
SELECT * FROM postgresdb_logs;
.output stdout
.exit 0;
```

```bash
# snowflake
sqlite3 experiment_logs.db
.headers on
.mode csv
.output exports/snowflakedb_logs_new.csv
SELECT * FROM snowflakedb_logs;
.output stdout
.exit 0;
```
