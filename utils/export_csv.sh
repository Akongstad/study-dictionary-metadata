# sqlite
sqlite3 experiment_logs.db
.headers on
.mode csv
.output exports/sqlitedb_logs.csv
SELECT * FROM sqlitedb_logs;
.output stdout
.exit 0;

#duckdb
sqlite3 experiment_logs.db
.headers on
.mode csv
.output exports/duckdb_logs.csv
SELECT * FROM duckDBdb_logs;
.output stdout
.exit 0;

#psql
sqlite3 experiment_logs.db
.headers on
.mode csv
.output exports/postgresdb_logs.csv
SELECT * FROM postgresdb_logs;
.output stdout
.exit 0;


