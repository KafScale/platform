SHELL := /bin/bash

FLINK_IMAGE ?= flink:1.18.1-scala_2.12
FLINK_NET ?= flink-net
FLINK_JOBMANAGER ?= flink-jobmanager
FLINK_TASKMANAGER ?= flink-taskmanager
FLINK_REST_HOST ?= $(FLINK_JOBMANAGER)
FLINK_REST_PORT ?= 8081

JAR_PATH := target/flink-kafscale-demo.jar
MAIN_CLASS := com.example.kafscale.flink.WordCountJob

# Build with Java 11 to match the Flink runtime image.
MVN_CMD ?= docker run --rm -v "$(PWD)":/app -w /app maven:3.9-eclipse-temurin-11 mvn

# Use host.docker.internal for local broker access from Docker
KAFSCALE_BOOTSTRAP_SERVERS ?= host.docker.internal:39092
KAFSCALE_SETUP_PROFILE ?= default

.PHONY: help build-jar build-jar-standalone docker-net up down submit submit-local logs clean status

help:
	@echo "Targets:"
	@echo "  build-jar  Build the Flink job jar (dependencies shaded)"
	@echo "  build-jar-standalone  Build jar with standalone profile (resources on classpath)"
	@echo "  up         Start Flink standalone (JobManager + TaskManager) in Docker"
	@echo "  down       Stop and remove Flink containers + network"
	@echo "  submit     Submit the job jar to the Flink JobManager"
	@echo "  submit-local Submit the job jar to local KafScale from Docker (host.docker.internal)"
	@echo "  status     Check JobManager REST endpoint"
	@echo "  logs       Tail Flink JobManager logs"

build-jar:
	@if [[ "$(BUILD_JAR)" == "1" ]]; then \
		$(MVN_CMD) -DskipTests package; \
	elif [[ -f $(JAR_PATH) ]]; then \
		echo "Jar exists. Set BUILD_JAR=1 to rebuild."; \
	else \
		$(MVN_CMD) -DskipTests package; \
	fi

build-jar-standalone:
	$(MVN_CMD) -Pstandalone -DskipTests package

# Create a dedicated network so the Flink containers can resolve each other

docker-net:
	@docker network inspect $(FLINK_NET) >/dev/null 2>&1 || docker network create $(FLINK_NET)

up: docker-net
	@docker ps -a --format '{{.Names}}' | grep -q '^$(FLINK_JOBMANAGER)$$' \
		&& echo "JobManager container $(FLINK_JOBMANAGER) already exists; reusing it." \
		|| docker run -d --name $(FLINK_JOBMANAGER) --network $(FLINK_NET) -p 8081:8081 \
			--add-host=host.docker.internal:host-gateway \
			-e JOB_MANAGER_RPC_ADDRESS=$(FLINK_JOBMANAGER) $(FLINK_IMAGE) jobmanager
	@docker ps -a --format '{{.Names}}' | grep -q '^$(FLINK_TASKMANAGER)$$' \
		&& echo "TaskManager container $(FLINK_TASKMANAGER) already exists; reusing it." \
		|| docker run -d --name $(FLINK_TASKMANAGER) --network $(FLINK_NET) \
			--add-host=host.docker.internal:host-gateway \
			-e JOB_MANAGER_RPC_ADDRESS=$(FLINK_JOBMANAGER) \
			-e FLINK_PROPERTIES="taskmanager.numberOfTaskSlots: 2" \
			$(FLINK_IMAGE) taskmanager

submit: build-jar
	@docker run --rm --network $(FLINK_NET) -v "$(PWD)/target:/jars" \
		--add-host=host.docker.internal:host-gateway $(FLINK_IMAGE) \
		flink run -m $(FLINK_REST_HOST):$(FLINK_REST_PORT) -c $(MAIN_CLASS) /jars/$(notdir $(JAR_PATH)) --profile=$(KAFSCALE_SETUP_PROFILE) \
		-Dkafscale.bootstrap.servers=$(KAFSCALE_BOOTSTRAP_SERVERS)

submit-local: build-jar
	@docker run --rm --network $(FLINK_NET) -v "$(PWD)/target:/jars" \
		--add-host=host.docker.internal:host-gateway $(FLINK_IMAGE) \
		flink run -m $(FLINK_REST_HOST):$(FLINK_REST_PORT) -c $(MAIN_CLASS) /jars/$(notdir $(JAR_PATH)) --profile=$(KAFSCALE_SETUP_PROFILE) \
		-Dkafscale.bootstrap.servers=$(KAFSCALE_BOOTSTRAP_SERVERS)

logs:
	@docker logs -f $(FLINK_JOBMANAGER)

status:
	@docker run --rm --network $(FLINK_NET) curlimages/curl:8.10.1 -fsS http://$(FLINK_REST_HOST):$(FLINK_REST_PORT)/overview >/dev/null \
		&& echo "Flink JobManager REST is up at http://$(FLINK_REST_HOST):$(FLINK_REST_PORT)" \
		|| echo "Flink JobManager REST is not reachable at http://$(FLINK_REST_HOST):$(FLINK_REST_PORT)"

down:
	-@docker rm -f $(FLINK_JOBMANAGER) $(FLINK_TASKMANAGER) >/dev/null 2>&1 || true
	-@docker network rm $(FLINK_NET) >/dev/null 2>&1 || true

clean:
	$(MVN_CMD) -DskipTests clean
