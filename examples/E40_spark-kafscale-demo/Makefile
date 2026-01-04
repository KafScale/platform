SHELL := /bin/bash

APP_NAME := spark-kafscale-demo
JAR := target/$(APP_NAME).jar
MAIN_CLASS := com.example.kafscale.spark.WordCountSparkJob

SPARK_SUBMIT ?= $(if $(SPARK_HOME),$(SPARK_HOME)/bin/spark-submit,spark-submit)
SPARK_MASTER ?= local[*]
KAFSCALE_SETUP_PROFILE ?= default

.PHONY: help build-jar clean run-jar-standalone

help:
	@echo "Targets:"
	@echo "  build-jar            Build the Spark demo jar"
	@echo "  run-jar-standalone   Run the Spark job locally via spark-submit"
	@echo "  clean                Remove build artifacts"

build-jar:
	@mvn -DskipTests package

run-jar-standalone: build-jar
	@if ! command -v $(SPARK_SUBMIT) >/dev/null 2>&1; then \
		echo "spark-submit not found. Set SPARK_HOME or add spark-submit to PATH." >&2; \
		exit 1; \
	fi
	@echo "Running Spark job with profile $(KAFSCALE_SETUP_PROFILE) ..."
	@KAFSCALE_SETUP_PROFILE=$(KAFSCALE_SETUP_PROFILE) \
		$(SPARK_SUBMIT) \
		--class $(MAIN_CLASS) \
		--master $(SPARK_MASTER) \
		$(JAR)

clean:
	@rm -rf target
