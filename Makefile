SHELL := /bin/bash

COMPOSE := docker compose
AIRFLOW_CLI := $(COMPOSE) run --rm airflow-cli

MAIN_DAG := SP500_stock_data_download
UPLOAD_DAG := SP500_stock_data_upload_from_local_csv
VALIDATION_DAG := validation_variables_and_connections

.PHONY: help build up down restart ps logs init vars-import dags-list trigger trigger-upload trigger-validation unpause-main unpause-upload clear-main clear-upload

help:
	@echo "Available targets:"
	@echo "  make build               Build docker images"
	@echo "  make up                  Start services in background"
	@echo "  make down                Stop services"
	@echo "  make restart             Restart services"
	@echo "  make ps                  Show service status"
	@echo "  make logs                Show logs (follow)"
	@echo "  make dags-list           List Airflow DAGs"
	@echo "  make vars-import         Import local airflow/airflow_variables.json"
	@echo "  make unpause-main        Unpause main DAG"
	@echo "  make unpause-upload      Unpause upload DAG"
	@echo "  make trigger             Trigger main DAG"
	@echo "  make trigger-upload      Trigger upload DAG"
	@echo "  make trigger-validation  Trigger validation DAG"
	@echo "  make clear-main          Clear task state for latest run of main DAG"
	@echo "  make clear-upload        Clear task state for latest run of upload DAG"

build:
	$(COMPOSE) up -d --build

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

restart: down up

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f

dags-list:
	$(AIRFLOW_CLI) airflow dags list

vars-import:
	python -c 'import json; d=json.load(open("airflow/airflow_variables.json")); [print(f"{k}={v}") for k, v in d.items()]' | while IFS='=' read -r k v; do $(AIRFLOW_CLI) airflow variables set "$$k" "$$v"; done

unpause-main:
	$(AIRFLOW_CLI) airflow dags unpause $(MAIN_DAG)

unpause-upload:
	$(AIRFLOW_CLI) airflow dags unpause $(UPLOAD_DAG)

trigger:
	$(AIRFLOW_CLI) airflow dags trigger $(MAIN_DAG)

trigger-upload:
	$(AIRFLOW_CLI) airflow dags trigger $(UPLOAD_DAG)

trigger-validation:
	$(AIRFLOW_CLI) airflow dags trigger $(VALIDATION_DAG)

clear-main:
	$(AIRFLOW_CLI) airflow tasks clear $(MAIN_DAG) -y

clear-upload:
	$(AIRFLOW_CLI) airflow tasks clear $(UPLOAD_DAG) -y
