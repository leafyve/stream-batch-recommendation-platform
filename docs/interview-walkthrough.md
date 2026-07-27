# Interview walkthrough

## Seven-minute path

**0:00–0:45 — Problem.** Explain that recommendation systems need consistent
online-like and offline features before they need a model. This project tests
that data contract locally with MovieLens.

**0:45–1:45 — Architecture.** Walk from the typed producer through a
three-partition Redpanda topic into four named Spark queries, Iceberg REST,
SQLite catalog metadata, and MinIO objects. Point out the independent batch
path and parity validator.

**1:45–2:45 — Event time.** Describe seven-day daily-slide UTC windows, the
24-hour watermark, event-ID deduplication, out-of-order inclusion, and the
deliberately late-event contract.

**2:45–3:45 — Writes.** Explain `foreachBatch` at-least-once invocation and
deterministic `MERGE INTO` keys. Be explicit that Iceberg has no relational
primary-key enforcement and that DLQ output is a separate at-least-once action.

**3:45–4:45 — Batch parity.** Show that batch rereads the source independently,
uses the shared expressions, and compares only finalised windows with exact
count/min/max and `1e-9` average tolerance.

**4:45–5:45 — Recovery.** Describe the observed-offset gate, checkpoint
inventory, SIGKILL, new query run IDs, uniqueness checks, and separate
MinIO/catalog restart. Qualify it as process restart recovery.

**5:45–6:30 — Benchmark.** Show candidate rate discovery, derived low/medium
rates, three trials per rate, full MovieLens 1M accounting, and local resource
limits. Quote only values rendered from evidence.

**6:30–7:00 — Limits.** Single broker, SQLite single writer, local MinIO,
one Spark worker, no auth/TLS, no production deployment, no model, and no HA.

## Strong discussion points

The strongest architecture decision is separating four query checkpoints and
an independent batch source, then paying the explicit repeated-read cost. The
strongest correctness result is zero-mismatch parity on only finalised windows,
not an unqualified table comparison. The strongest recovery evidence is
observable checkpoint resume after an entire stream-container SIGKILL plus
unchanged table counts across storage/catalog restart.

The hardest diagnosed failure was concurrent Iceberg REST commits against
SQLite. The catalog returned `SQLITE_BUSY` as an unknown commit state.
Investigation first read the target table before retry; the fix serializes
driver commits and lets the fixture wait for its one writer. This is a useful
example of respecting an unknown-state failure rather than blindly retrying.

The claim requiring the most qualification is “exactly once.” What is actually
demonstrated is deterministic logical idempotency under the tested retry and
restart scenarios. There is no atomic Kafka-to-Iceberg-to-DLQ transaction.

