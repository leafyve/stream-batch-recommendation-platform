"""Fetch and verify the exact Spark connector and Iceberg runtime jars."""

from __future__ import annotations

import hashlib
import sys
import urllib.request
from pathlib import Path

ARTIFACTS = {
    "spark-sql-kafka-0-10_2.12-3.5.8.jar": (
        "https://repo1.maven.org/maven2/org/apache/spark/"
        "spark-sql-kafka-0-10_2.12/3.5.8/spark-sql-kafka-0-10_2.12-3.5.8.jar",
        "d2b7068eabce2a1267103625e58efae32f7ee384db1cbc22424532ad2ed95fb4",
    ),
    "spark-token-provider-kafka-0-10_2.12-3.5.8.jar": (
        "https://repo1.maven.org/maven2/org/apache/spark/"
        "spark-token-provider-kafka-0-10_2.12/3.5.8/"
        "spark-token-provider-kafka-0-10_2.12-3.5.8.jar",
        "85f44359b60943d262f37d1327ebb7931fe47e0392286fe3023299e25b84b0d8",
    ),
    "kafka-clients-3.4.1.jar": (
        "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/"
        "kafka-clients-3.4.1.jar",
        "9d0a9058c8ede79db6e54ae378ed16fe8d1bead2894635a4aa6aa0b7981668c6",
    ),
    "commons-pool2-2.11.1.jar": (
        "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/"
        "commons-pool2-2.11.1.jar",
        "ea0505ee7515e58b1ac0e686e4d1a5d9f7d808e251a61bc371aa0595b9963f83",
    ),
    "iceberg-spark-runtime-3.5_2.12-1.10.1.jar": (
        "https://repo1.maven.org/maven2/org/apache/iceberg/"
        "iceberg-spark-runtime-3.5_2.12/1.10.1/"
        "iceberg-spark-runtime-3.5_2.12-1.10.1.jar",
        "39ea09e6c03550a300b9d9ab498949836d2d434e441da4c12a943a086e396940",
    ),
    "iceberg-aws-bundle-1.10.1.jar": (
        "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.10.1/"
        "iceberg-aws-bundle-1.10.1.jar",
        "86bf20892ea5b4c17688f19b075399885f6aa5303f6b2dc9f491e76ceef9633b",
    ),
}


def fetch(destination: Path) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    for filename, (url, expected) in ARTIFACTS.items():
        target = destination / filename
        with urllib.request.urlopen(url, timeout=120) as response:
            payload = response.read()
        actual = hashlib.sha256(payload).hexdigest()
        if actual != expected:
            raise RuntimeError(f"{filename}: expected {expected}, received {actual}")
        target.write_bytes(payload)
        print(f"{filename} sha256={actual} bytes={len(payload)}", flush=True)


if __name__ == "__main__":
    fetch(Path(sys.argv[1]))
