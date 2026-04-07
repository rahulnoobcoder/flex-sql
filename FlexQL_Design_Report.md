# FlexQL Design Document

Github URL : https://github.com/rahulnoobcoder/flex-sql


## 1) System Overview
FlexQL is designed as a lightweight client-server database driver with a limited SQL subset focused on correctness, clarity, and high throughput for large datasets.

The system is split into:
- A client-facing API and REPL-style interface
- A multithreaded server for query processing
- A storage and execution core responsible for parsing, indexing, caching, and result generation

## 2) Data Storage Design
### Chosen model
- **Column-oriented storage** was selected to improve scan efficiency for analytical-style reads and to reduce unnecessary data movement when only a subset of columns is selected.

### Data representation
- Fixed-width numeric values are stored compactly for predictable access.
- Variable-length text is separated into a string area and referenced by metadata in table cells.
- Schema metadata is maintained per table for type enforcement and query-time validation.

### Key storage numbers
- Cell segment size: **65,536 cells** per segment.
- String segment size: **65,536 bytes (64 KB)** per segment.
- WAL max single-row payload guard: **8 MB**.

### Durability strategy
- Data files represent persistent table state.
- A write-ahead logging approach is used so changes can be recovered safely after interruption.

## 3) Indexing Method
### Primary index
- A dedicated primary-key index maps key values to row locations.
- This supports fast equality lookups and avoids full-table scans for primary-key queries.

### Rationale
- The workload includes repeated key-based lookups and selective filtering.
- Prioritizing primary-key acceleration gives the highest practical gain with moderate complexity.

## 4) Caching Strategy
### Query-result caching
- An **LRU cache** stores recent read query results.
- Cache entries are reused for repeated identical reads.
- Cache is invalidated when writes modify relevant data to preserve correctness.
- Cache capacity: **100 query results**.

### Rationale
- Assignment-style workloads and benchmarks often repeat query shapes.
- LRU offers predictable memory bounds and effective hit rates for temporal locality.

## 5) Expiration Timestamp Handling
### Policy
- Expiration is modeled through an `EXPIRES_AT` field.
- Rows with expiration time less than or equal to current system time are treated as expired.

### Query behavior
- Expired rows are filtered out during read operations.
- This filtering is consistently applied in both single-table reads and join processing.

## 6) Multithreading Design
### Connection model
- The server processes multiple client connections concurrently.
- Each request is handled in a thread-safe execution context.

### Runtime/network limits used
- Server listen backlog: **128** pending connections.
- Per-connection read buffer: **8 MB**.
- Maximum buffered pending SQL per connection: **16 MB**.
- Socket send/receive buffers (client and server): **4 MB** each.

### Concurrency control
- Read/write synchronization is used around shared table state.
- Read operations are allowed to proceed concurrently where safe.
- Write operations use exclusive access to protect consistency.
- Shared subsystems (for example cache and log-related paths) are protected with dedicated synchronization.

### Rationale
- This model keeps concurrency high for read-heavy loads while preserving correctness for writes.

## 7) Query Handling Design
## SELECT
- Projection resolution is performed first (all columns or selected columns).
- Optional filtering is applied next.
- Optional ordering and limiting are applied before final output.
- Expiration filtering is integrated into the read path.

## WHERE (single condition)
- The design intentionally supports one condition as required.
- Comparison operators are type-aware and validated against schema information.

## INNER JOIN
- Join keys are resolved against both table schemas.
- Matching rows are combined using key-based lookup strategy.
- Optional post-join filtering and projection are then applied.
- Expiration filtering is respected on both sides of the join.

## 8) Optimization Decisions
### Ingestion path
- Insert-heavy workloads are optimized through batch-oriented handling and reduced per-row overhead.
- Network and request handling are tuned for high-throughput transfer.
- Benchmark insert batch size: **20,000 rows per INSERT statement**.
- Default benchmark row target: **1,000,000 rows** (extended run used **10,000,000 rows**).
- WAL flush threshold: **4 MB** buffered payload.
- WAL 4 MB row-equivalent (benchmark row shape): about **52,000 rows**
  - Based on ~80 bytes/row SQL payload in the benchmark builder: $4,194,304 / 80 \approx 52,428$ rows.
  - Practical value varies with row text length (ID digits, string lengths, and SQL formatting).

### Read path
- Primary indexing minimizes lookup cost for key filters.
- LRU caching reduces repeated compute for identical reads.
- Early expiration checks reduce unnecessary downstream work.
- SQL parser safety limits: max token length **4096** chars, max tokens **200,000** per statement.

### Stability/performance balance
- Optimizations were chosen to improve throughput without changing query correctness semantics.
- Consistency and deterministic behavior are prioritized over aggressive unsafe shortcuts.

## 9) Compilation and Execution Instructions
### Build
- Run `make clean`
- Run `make -j4`

### Start server
- Run `./server`

### Start client
- Run `./client_demo`

### Execute provided SQL scripts
- In the client interface, run:
  - `SOURCE fill.sql;`
  - `SOURCE query.sql;`

### Run benchmark
- Standard: `./benchmark`
- Large dataset: `./benchmark 10000000`

## 10) Performance Report Location
Detailed benchmark and unit-test performance results are documented separately in `performance.md`.

## 11) Final Design Summary
The final design emphasizes:
- Simple and clear SQL subset behavior
- Strong practical performance on large inserts
- Efficient read handling via indexing + caching
- Correct concurrent behavior through explicit synchronization
- Predictable expiration semantics integrated into query execution
