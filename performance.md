# FlexQL Performance RepoSummary: **21/21 passed, 0 failed**

## Insert Benchmark Results
rt

## Test Environment
- Build mode: `-O3 -std=c++17 -pthread`
- Server/client port: `9000`
- Benchmark tool: `./benchmark`
- Dataset scales tested: `1,000,000` and `10,000,000` inserted rows

## Commands Used
```bash
make clean && make -j4
./server
./benchmark --unit-test
./benchmark 1000000
./benchmark 10000000
```

Read-latency checks (after 10M load):
```bash
printf 'SELECT NAME, EMAIL FROM BIG_USERS WHERE ID = 9000000;\n.exit\n' | ./client_demo
printf 'SELECT ID FROM BIG_USERS WHERE BALANCE > 10998;\n.exit\n' | ./client_demo
```

## Unit Test Results
From `./benchmark --unit-test`:
- Unit Test 
| Workload | Elapsed | Throughput |
|---|---:|---:|
| 1,000,000 rows | 903 ms | 1,107,419 rows/sec |
| 10,000,000 rows | 7,369 ms | 1,357,036 rows/sec |

### Notes
- 10M run includes SQL subset validation + unit tests after insert phase.
- Throughput remains above **1.1M rows/sec** across both scales, peaking at **~1.36M rows/sec** in this run.

## SELECT Latency (after 10M load)

| Query | Cold | Warm |
|---|---:|---:|
| `SELECT NAME, EMAIL FROM BIG_USERS WHERE ID = 9000000;` | 0.00 s | 0.00 s |
| `SELECT ID FROM BIG_USERS WHERE BALANCE > 10998;` | 0.23 s | 0.17 s |

## Derived Indicators
- Effective per-row insert time (10M run):
  - $7369\,\text{ms} / 10{,}000{,}000 \approx 0.7369\,\mu s/row$
- At ~1.357M rows/sec and 20k-row batches:
  - $1{,}357{,}036 / 20{,}000 \approx 67.9$ batches/sec

## Configuration Numbers Relevant to Performance
- Insert batch size: **20,000 rows/batch**
- WAL flush threshold: **4 MB**
- WAL single-row payload guard: **8 MB**
- LRU cache capacity: **100** query results
- Per-connection server read buffer: **8 MB**
- Per-connection pending SQL cap: **16 MB**
- Socket send/receive buffers: **4 MB** (client and server)
