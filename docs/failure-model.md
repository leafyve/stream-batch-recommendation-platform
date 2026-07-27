# Failure model

| Failure | Expected behavior | Verified? |
|---|---|---|
| Malformed JSON / invalid rating | Persist invalid payload and errors; emit DLQ; valid queries continue | Docker contract test |
| Duplicate within watermark | Feature count once; raw merge remains one logical row | Spark and recovery tests |
| Out-of-order within 24 hours | Included in original event-time windows | Recovery/e2e parity |
| Event older than watermark | Stored as valid raw; discarded from both feature states | Docker contract test |
| Stream process SIGKILL | Restart from four preserved checkpoints under new query run IDs | Recovery test |
| Reinvoked Iceberg batch | Merge same deterministic logical keys | Recovery and batch rerun |
| Catalog/MinIO process restart | Named volumes retain catalog and objects | Recovery test |
| Concurrent SQLite writes | Wait/serialize local catalog commits | Reproduced regression contract |
| Broker loss | Processing unavailable; no replica | Not tested |
| Checkpoint-volume loss | Kafka replay possible; feature state continuity lost | Not tested |
| MinIO-volume loss/corruption | Warehouse loss or unreadable metadata | Not tested |
| Network partition | Component-specific timeout/retry behavior | Not tested |
| Host/WSL/Windows crash | Depends on Docker named-volume durability | Not tested |

The recovery timer begins immediately before `docker kill --signal=SIGKILL`
and ends after the restarted container is running and all four new query-start
events are observed. It is specifically a process/container restart measure.

The storage test restarts MinIO and the REST catalog, waits on health checks,
and compares all eight table counts before and after. It is not a storage
durability, replication, backup, or disaster-recovery test.

SQLite errors can make the Iceberg client report commit state unknown. Before
retrying the reproduced failure, the table was read to establish whether the
commit had succeeded. Deterministic merge keys protect logical rows, but
orphan-file cleanup remains an operational concern after failed unknown-state
commits.

