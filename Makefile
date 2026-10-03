# Berlin transit delay pipeline.
# Everything an operator or a reviewer needs is a target here.

SHELL := /bin/bash
COMPOSE := docker compose
PSQL := $(COMPOSE) exec -T timescaledb psql -U $${POSTGRES_USER:-delays} -d $${POSTGRES_DB:-delays}
TOOLS := $(COMPOSE) run --rm --no-deps \
	-e PG_DSN=postgresql://$${POSTGRES_USER:-delays}:$${POSTGRES_PASSWORD}@timescaledb:5432/$${POSTGRES_DB:-delays} \
	producer

.DEFAULT_GOAL := help
.PHONY: help up down restart logs ps seed-gtfs compat proto test test-python test-java lint \
        build psql proof-replay proof-checkpoint proof-load clean nuke

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

up: ## Start the pipeline (builds on first run)
	@test -f .env || (echo "copy .env.example to .env first" && exit 1)
	$(COMPOSE) up -d --build
	@echo "Flink UI   http://localhost:8083"
	@echo "Grafana    http://localhost:3000"

down: ## Stop everything, keep the data
	$(COMPOSE) down

restart: ## Restart the stream processing job only
	$(COMPOSE) restart taskmanager

logs: ## Follow logs of the main services
	$(COMPOSE) logs -f producer jobmanager taskmanager

ps: ## Show service state and memory use
	$(COMPOSE) ps
	@docker stats --no-stream --format 'table {{.Name}}\t{{.MemUsage}}\t{{.CPUPerc}}' \
		$$($(COMPOSE) ps -q) 2>/dev/null || true

seed-gtfs: ## Reload the GTFS stop and route dimension tables
	$(COMPOSE) run --rm gtfs-loader

compat: ## Pin the schema subject to BACKWARD compatibility
	$(COMPOSE) run --rm --no-deps -e SCHEMA_REGISTRY_URL=http://redpanda:8081 \
		producer python /app/tools/set_compatibility.py

build: ## Build the job jar and the producer image
	$(COMPOSE) build

proto: ## Generate the Python protobuf bindings (the Java ones come from Maven)
	mkdir -p producer/src/bvg_producer/generated/delay/v1
	python -m grpc_tools.protoc -Iproto \
		--python_out=producer/src/bvg_producer/generated \
		--pyi_out=producer/src/bvg_producer/generated \
		proto/delay/v1/stop_event.proto
	@for dir in producer/src/bvg_producer/generated \
	            producer/src/bvg_producer/generated/delay \
	            producer/src/bvg_producer/generated/delay/v1; do \
		echo '"""Generated protobuf modules."""' > "$$dir/__init__.py"; \
	done

test: test-python test-java ## Run every test

test-python: proto ## Run the producer test suite
	cd producer && PYTHONPATH=src python -m pytest -q

test-java: ## Run the Flink job test suite
	cd flink-job && mvn -B -q test

lint: proto ## Lint and type check the Python sources
	cd producer && ruff check src tests ../tools && mypy src

psql: ## Open a psql shell on the sink
	$(COMPOSE) exec timescaledb psql -U $${POSTGRES_USER:-delays} -d $${POSTGRES_DB:-delays}

# ---------------------------------------------------------------------------
# Proofs. Each one is scripted so a reviewer can rerun it, and each writes
# its numbers into docs/proofs.md by hand afterwards.
# ---------------------------------------------------------------------------

proof-replay: ## Proof 1: replay from offset 0 reproduces the same output
	@echo "== capturing the checksum of the current output =="
	$(TOOLS) python /app/tools/verify_replay.py capture first-run
	@echo "== stopping the job and clearing its output =="
	$(COMPOSE) stop jobmanager taskmanager
	$(PSQL) -c "TRUNCATE delay_tumbling_1h, delay_sliding_1h_5m, late_events;"
	@echo "== restarting from offset 0 under a fresh consumer group =="
	CONSUMER_GROUP=replay-$$(date +%s) STARTING_OFFSETS=earliest \
		$(COMPOSE) up -d --force-recreate jobmanager taskmanager
	@echo "Wait until the job has caught up (watch the Flink UI), then run:"
	@echo "  make proof-replay-verify"

proof-replay-verify: ## Proof 1, second half: compare the two runs
	$(TOOLS) python /app/tools/verify_replay.py capture replay
	$(TOOLS) python /app/tools/verify_replay.py compare first-run replay

proof-checkpoint: ## Proof 2: kill the job mid-stream, recover from checkpoint
	@echo "== rows before the kill =="
	$(PSQL) -c "SELECT count(*) AS tumbling_rows, sum(event_count) AS events FROM delay_tumbling_1h;"
	@echo "== killing the task manager =="
	$(COMPOSE) kill -s SIGKILL taskmanager
	sleep 5
	$(COMPOSE) up -d taskmanager
	@echo "== the job manager restarts the job from the last checkpoint =="
	@echo "Watch recovery:  docker compose logs -f jobmanager | grep -i restor"
	@echo "Then compare row counts and look for gaps:"
	@echo "  make proof-checkpoint-verify"

proof-checkpoint-verify: ## Proof 2, second half: look for gaps and duplicates
	$(PSQL) -f - < sql/checks/recovery_checks.sql

proof-load: ## Proof 3: replay recorded traffic at 10x with lag sampling
	$(COMPOSE) --profile probe up -d lag-probe
	SPEEDUP=$${SPEEDUP:-10} LOOPS=$${LOOPS:-1} $(COMPOSE) --profile load run --rm loadgen
	@echo "== lag during the test =="
	$(PSQL) -c "SELECT time_bucket('30 seconds', sampled_at) AS t, sum(lag) AS total_lag \
		FROM consumer_lag_samples WHERE sampled_at > now() - interval '30 minutes' \
		GROUP BY 1 ORDER BY 1;"
	$(COMPOSE) stop lag-probe

clean: ## Stop and remove containers, keep the volumes
	$(COMPOSE) down --remove-orphans

nuke: ## Remove containers and every volume, including recorded snapshots
	$(COMPOSE) down -v --remove-orphans
