# Delivery semantics

## Producer

The producer uses `acks=all`, idempotence, five bounded retries, exponential
backoff, checked delivery callbacks, and a checked final flush. The local
single broker still provides no replica failover. Event identity excludes
replay-run and ingestion timestamps, so an acknowledged event replay retains
the same ID.

## Spark source and checkpoints

Each query durably records source offsets and state under its own checkpoint.
After a process kill Spark resumes from that checkpoint. The project does not
depend on consumer-group committed offsets for recovery.

`foreachBatch` can reinvoke a micro-batch. Each sink deduplicates its source on
the logical key and executes Iceberg `MERGE INTO`. A retry therefore updates
the same logical row. Iceberg itself does not enforce uniqueness, so probe and
integration tests explicitly check duplicate keys.

## Invalid events and DLQ

Invalid Iceberg rows use deterministic identity `SHA-256(topic:partition:offset)`.
Their merge is idempotent. The subsequent Kafka DLQ write is a separate action:
failure after the Iceberg commit can cause the batch to retry and produce a
second DLQ message. Downstream consumers must deduplicate by invalid ID.

## Event-time duplicates and lateness

The user/movie queries apply a 24-hour watermark before
`dropDuplicatesWithinWatermark(["event_id"])`. Duplicates retained in state
count once. Delayed events within the watermark participate in their original
event-time windows. Events older than the watermark are dropped from feature
state; the independent raw-valid query still stores a valid raw row.

Watermark advancement depends on observed maximum event time and trigger
progress. State retention trades bounded memory for the possibility that a
duplicate beyond the retained horizon can be processed again.

## Exact claim boundary

The platform demonstrates:

- checked producer acknowledgements;
- durable query checkpoint resume;
- idempotent logical Iceberg rows under tested retries;
- exact batch-stream parity for finalised tested windows.

It does not provide one atomic transaction across Kafka input, four Iceberg
tables, and Kafka DLQ output. It therefore does not claim universal
end-to-end exactly-once delivery or cross-table atomicity.

