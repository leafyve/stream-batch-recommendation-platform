# Testing and evidence

## Test layers

- Unit tests cover configuration precedence/validation, schema
  canonicalisation, producer sorting/injection/delivery behavior, source
  download checks, and the reference feature implementation.
- Spark tests run against Spark 3.5.8 and verify valid/invalid routing,
  unsupported ratings/schema versions, watermark deduplication, UTC windows,
  and out-of-order inclusion.
- Contract/e2e evidence asserts live malformed JSON, invalid rating, DLQ, late
  event, parity, restart, persistence, and benchmark outcomes.
- Docker integration tests validate Compose, health, topic configuration, all
  eight readable tables, and logical uniqueness.

Core package coverage is enforced at 85% branch coverage. Thin Spark job entry
points and external-runtime orchestration are excluded from the unit coverage
denominator and covered through Spark/Docker evidence instead.

## Commands

```powershell
py -3.11 scripts/task.py test
py -3.11 scripts/task.py spark-test
py -3.11 scripts/task.py integration-test
py -3.11 scripts/task.py recovery-test
py -3.11 scripts/task.py benchmark
py -3.11 scripts/task.py report
py -3.11 scripts/secret_scan.py
py -3.11 scripts/check_committed_files.py
docker compose config --quiet
```

Normal CI uses Python 3.11 and Java 17 and runs locked installation, lock hash
verification, Ruff, strict Mypy, unit/coverage tests, practical Spark tests,
Compose validation, secret scanning, and large-file scanning. It intentionally
does not run the MovieLens 1M benchmark or destructive recovery test.

## Evidence layout

Ignored verbose runtime evidence is under `reports/runtime`. Compact committed
evidence is under `reports/evidence`:

- `recovery.json`: 100K checkpoint kill/restart, rerun, parity, and storage
  restart;
- `contracts.json`: malformed/invalid/DLQ/late-event behavior;
- `catalog.json`: SQLite driver, read-after-restart, and repeated commit proof;
- `build.json`: immutable registry pins and host-local image identities;
- `benchmark.json`: rate discovery, nine measured trials, full 1M outcome,
  resources, and parity;
- `verification.json`: final clean-clone command results.

Each evidence file records command, environment, dataset/checksum,
configuration, run count, UTC time, result, and limitations. README result
rows are generated from these files by `scripts/generate_report.py`.
