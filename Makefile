.PHONY: setup up download-data create-topics init-tables test spark-test integration-test run-stream produce-dev run-batch parity recovery-test benchmark report down clean

PYTHON ?= python3
TASK = $(PYTHON) scripts/task.py

setup:
	$(TASK) setup
up:
	$(TASK) up
download-data:
	$(TASK) download-data
create-topics:
	$(TASK) create-topics
init-tables:
	$(TASK) init-tables
test:
	$(TASK) test
spark-test:
	$(TASK) spark-test
integration-test:
	$(TASK) integration-test
run-stream:
	$(TASK) run-stream
produce-dev:
	$(TASK) produce-dev
run-batch:
	$(TASK) run-batch
parity:
	$(TASK) parity
recovery-test:
	$(TASK) recovery-test
benchmark:
	$(TASK) benchmark
report:
	$(TASK) report
down:
	$(TASK) down
clean:
	$(TASK) clean
