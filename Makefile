SHELL := /bin/bash
-include .env
export

DB2_USER     ?= db2inst1
DB2_DATABASE ?= TESTDB
DB2_TOPIC    ?= db2-orders
ENRICHED_TOPIC ?= orders-enriched

.PHONY: help venv discover up seed topics connector flink spine aws-setup tableflow \
        read quick-read produce-more status down clean

help:
	@echo "Setup:"
	@echo "  make venv        — create .venv and install Python deps"
	@echo "  make discover    — resolve SR + mint CC API keys into .env (confluent login first)"
	@echo "Local + ingest:"
	@echo "  make up          — build & start DB2 + self-managed Connect (Docker daemon required)"
	@echo "  make seed        — create ORDERS table + rows in DB2 (after DB2 is healthy)"
	@echo "  make topics      — create the db2-orders topic on CC"
	@echo "  make connector   — deploy the JDBC source (DB2 -> db2-orders)"
	@echo "Process + materialize:"
	@echo "  make flink       — create the Flink statement (db2-orders -> orders-enriched)"
	@echo "  make aws-setup   — create S3 + Glue + IAM (needs AWS creds); paste AWS_* into .env"
	@echo "  make tableflow   — enable Tableflow (Iceberg) on orders-enriched"
	@echo "Consume (Spark, the end):"
	@echo "  make read        — Spark reads the Iceberg table + aggregates"
	@echo "  make quick-read  — pyiceberg no-JVM sanity read"
	@echo "Ops:  make produce-more | make status | make down | make clean"

venv:
	python3 -m venv .venv && .venv/bin/pip install -q -r requirements.txt && echo "✓ venv ready"

discover:
	bash scripts/00_discover_env.sh

up:
	docker compose up -d --build
	@echo "DB2 takes a few minutes on first boot. Watch: docker compose logs -f db2  (wait for 'Setup has completed')"

seed:
	@echo "→ waiting for DB2 to be healthy ..."
	@until [ "$$(docker inspect -f '{{.State.Health.Status}}' olpipe-db2 2>/dev/null)" = "healthy" ]; do sleep 5; printf '.'; done; echo
	@echo "→ seeding ORDERS ..."
	docker exec olpipe-db2 bash -lc "su - $(DB2_USER) -c 'db2 connect to $(DB2_DATABASE) && db2 -tvf /tmp/init.sql'"
	@echo "✓ seeded"

topics:
	bash scripts/01_create_topics.sh

connector:
	bash scripts/02_deploy_connector.sh

flink:
	bash scripts/03_create_flink.sh

# the Confluent-Cloud half end to end (topics -> connector -> flink), assumes `make up && make seed` done
spine: topics connector flink

aws-setup:
	.venv/bin/python infra/01_create_aws_resources.py

tableflow:
	.venv/bin/python infra/02_setup_tableflow.py

read:
	bash run_spark.sh reader/spark_read.py

quick-read:
	.venv/bin/python reader/quick_read.py

# insert a fresh batch into DB2 so the JDBC connector produces more rows (helps trigger a Tableflow S3 flush)
produce-more:
	docker exec olpipe-db2 bash -lc "su - $(DB2_USER) -c 'db2 connect to $(DB2_DATABASE) && db2 \"insert into ORDERS (ORDER_ID,CUSTOMER,ITEM,QUANTITY,AMOUNT_USD) select ORDER_ID||'\''-r'\'', CUSTOMER, ITEM, QUANTITY, AMOUNT_USD from ORDERS\"'"
	@echo "✓ doubled the ORDERS rows"

status:
	@echo "== connectors =="; curl -s $(CONNECT_REST)/connectors/db2-orders-source/status 2>/dev/null | python3 -m json.tool 2>/dev/null || echo "(connect worker not reachable)"
	@echo "== flink =="; confluent flink statement list --environment $(ENV_ID) --cloud $(CLOUD_PROVIDER) --region $(CLOUD_REGION) 2>/dev/null | grep -i olpipe || true
	@echo "== tableflow =="; confluent tableflow topic list --environment $(ENV_ID) --cluster $(CLUSTER_ID) 2>/dev/null || true

down:
	docker compose down

clean: down
	-confluent flink statement delete olpipe-enrich-orders --environment $(ENV_ID) --cloud $(CLOUD_PROVIDER) --region $(CLOUD_REGION) --force 2>/dev/null
	-curl -s -X DELETE $(CONNECT_REST)/connectors/db2-orders-source 2>/dev/null
	docker compose down -v
	@echo "✓ torn down (CC topics + Tableflow + AWS resources are left for you to remove explicitly)"
