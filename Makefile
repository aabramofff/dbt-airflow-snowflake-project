DC = docker compose
CONTAINER = airflow-scheduler

DBT_ROOT = /opt/airflow/dbt_core
DBT_BIN = /home/airflow/.local/bin/dbt
DBT_FLAGS = --project-dir $(DBT_ROOT) --profiles-dir $(DBT_ROOT)

EXEC = $(DC) exec $(CONTAINER)
DBT_EXEC = $(EXEC) $(DBT_BIN)

.PHONY: help up down restart logs bash dbt-deps dbt-seed dbt-run dbt-test dbt-full clean

help:
	@echo Available commands:
	@echo up       - Start containers
	@echo down     - Stop containers
	@echo restart  - Restart containers
	@echo deps     - Install dbt dependencies
	@echo seed     - Run dbt seeds
	@echo run      - Run dbt models
	@echo build    - Full dbt build
	@echo clean    - Clear local cache


up:
	$(DC) up -d

down:
	$(DC) down

restart:
	$(DC) restart

logs:
	$(DC) logs -f airflow-scheduler

bash:
	$(EXEC) bash


deps:
	$(DBT_EXEC) deps $(DBT_FLAGS)

seed:
	$(DBT_EXEC) seed $(DBT_FLAGS)

run:
	$(DBT_EXEC) run $(DBT_FLAGS)

test:
	$(DBT_EXEC) test $(DBT_FLAGS)

build:
	$(DBT_EXEC) build $(DBT_FLAGS)


clean:
	rm -rf dbt_core/target dbt_core/logs dbt_core/dbt_packages
	@echo "Local cache has been cleared. To clear in container run 'make dbt-clean'"

dbt-clean:
	$(DBT_EXEC) clean $(DBT_FLAGS)
