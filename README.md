### Study on Dictionary Metadata

Repository for the benchmark code used to test performance of functions on metadata in Popular databases and platforms.

#### Postrgres Docker setup

```bash
# Manual
docker pull postgres

docker run --name postgres-container -e POSTGRES_USER=user -e POSTGRES_PASSWORD=research-project -e POSTGRES_DB=postgres -p 5432:5432 -d postgres

docker exec -it d267 psql -U user -d postgres -c "SHOW max_locks_per_transaction;"
docker exec -it d267 psql -U user -d postgres -c "ALTER SYSTEM SET max_locks_per_transaction = 1024;"
docker restart postgres_container

# Script:
python utils/postgres_init.py
```

#### Errors and observations

#### Postgres

```txt
out of shared memory
HINT:  You might need to increase "max_locks_per_transaction". 
```

Link to article describing out of memory issue for postgres <https://help.heroku.com/EW2G2AF7/how-can-i-resolve-the-error-error-out-of-shared-memory-hint-you-might-need-to-increase-max_locks_per_transaction>

```txt
running: select * from information_schema.tablesERROR        experiment_1():Experiment 1 failed: could not resize shared memory segment "/PostgreSQL.1080312552" to 2097152 bytes: No space left on device
CONTEXT:  parallel worker
```

Snapshot

![alt text](<assets/Screenshot 2024-11-03 at 23.30.17.png>)

---

#### Snowflake

```txt
--running: show tablesERROR:root:Experiment 1 failed: 090153 (22000): The result set size exceeded the max number of rows(10000) supported for SHOW statements. Use LIMIT option to limit result set to a smaller number.
(.venv) (base)
```

#### Duckdb

Exporting 100000 tables to parquet hangs and  causes duckdb to use 50 gb ram.
It is very easy to export data, but hard to export objects. This is the same for the aother platforms.

```sql
EXPORT DATABASE 'exports/table_sample.parquet' (FORMAT 'PARQUET');
```
