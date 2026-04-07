# FlexQL (C++ SQL-like Database Driver)

FlexQL is a client-server SQL-like database system built in modern C++. It supports core relational operations, primary indexing, query caching, expiration filtering, and multithreaded request handling.

## Supported Features

- SQL subset: `CREATE TABLE`, `INSERT`, `SELECT`, single-condition `WHERE`, `INNER JOIN`
- Types: `INT`, `DECIMAL`, `VARCHAR`, `DATETIME`
- Expiration handling via `EXPIRES_AT`
- Primary key index for fast equality lookup
- LRU query-result cache for repeated `SELECT`
- Multithreaded TCP server
- C API: `flexql_open`, `flexql_exec`, `flexql_close`, `flexql_free`

## Build

```bash
make clean
make -j4
```

This creates:
- `server`
- `client_demo`
- `benchmark`

## Run

### 1) Start server
```bash
./server
```

### 2) Start client REPL
```bash
./client_demo
```

### 3) Run provided SQL files
Inside client:
```sql
SOURCE fill.sql;
SOURCE query.sql;
```

## Benchmark

Default run:
```bash
./benchmark
```

Large dataset run (10M rows):
```bash
./benchmark 10000000
```

Unit-test only mode:
```bash
./benchmark --unit-test
```

## Quick SQL Example

```sql
CREATE TABLE USERS (ID DECIMAL, NAME VARCHAR(64), EXPIRES_AT DECIMAL);
INSERT INTO USERS VALUES (1, 'Alice', 1893456000), (2, 'Bob', 1893456000);
SELECT * FROM USERS WHERE ID = 1;
```

## Notes

- `WHERE` supports one condition (no `AND`/`OR` chaining).
- Only `INNER JOIN` is implemented in this submission.
- Design document source is available in `FlexQL_Design_Report.tex`.
