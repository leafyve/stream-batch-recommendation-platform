# Repository instructions

These rules apply to the entire `tiktok_SE_intern` repository.

- Work only inside this repository. Do not inspect, edit, clean, reset, commit, or otherwise interact with sibling repositories.
- Do not push commits or deploy the project externally.
- Do not fabricate commands, tests, service health, evidence, metrics, benchmark results, deployment status, or production usage.
- Do not publish unverified resume metrics. Distinguish local completion, automated verification, Docker integration, local benchmarks, external deployment, production operation, and real-user validation.
- Do not commit secrets, `.env`, raw MovieLens records, downloaded archives, expanded datasets, runtime checkpoints, transient logs, or large runtime artifacts.
- Use only official GroupLens sources for MovieLens data. Preserve local provenance and license material without committing the raw records.
- Run the checks relevant to a phase before committing it. Inspect the diff and stage only the intended files.
- Make atomic commits. Never use `git add .` or `git add -A`.
- Do not weaken, skip, or rewrite tests merely to make a failing result pass.
- Do not silently change locked technologies or versions. Stop and report a reproduced incompatibility before proposing a material architecture change.
- Preserve compact generated evidence used by README metrics. Keep raw runtime evidence ignored under `reports/runtime`.
- Qualify delivery semantics precisely. Do not make a general end-to-end exactly-once or high-availability claim.
- Destructive cleanup must be explicit and guarded. Persistent Docker volumes require a clear option and confirmation.

Locked core stack:

- Apache Spark/PySpark 3.5.8, Scala 2.12, Java 17
- Apache Iceberg 1.10.1
- Redpanda v26.1.13 and Redpanda Console v3.8.0
- MinIO server source release `RELEASE.2025-10-15T17-29-55Z`

Public project title:

> Stream-Batch Recommendation Feature Platform

Initial status wording:

> A locally completed distributed-data infrastructure project using containerised services.
