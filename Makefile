PROJECT_NAME=JobPulse
COMPOSE=docker compose
DOCKERFILE_PATH=./Dockerfile

.PHONY: help build up pull down logs ps clean restart

help:
	@echo "Available commands:"
	@echo "make build              Build local image"
	@echo "make up                 Build & start (smart pull)"
	@echo "make pull               Run this before make up if make up fails to pull images (mainly due to timeout and resource limits)"
	@echo "make down               Stop containers"
	@echo "make clean              Remove containers, images, volumes"
	@echo "make logs               Follow container logs"
	@echo "make ps                 List running containers"
	@echo "make restart            Restart the application (still keeps volumes)"

build:
	docker build -f $(DOCKERFILE_PATH) . -t job_warehouse_discord:latest

pull:
	@echo "🔄 Trying PARALLEL pull..."
	$(COMPOSE) pull || ( \
		echo "Parallel pull failed (timeout/resource). Falling back to SEQUENTIAL pull..." && \
		set COMPOSE_PARALLEL_LIMIT=1 && $(COMPOSE) pull \
	)

up: build pull
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f

ps:
	$(COMPOSE) ps

restart:
	$(COMPOSE) down
	$(COMPOSE) up -d

clean:
	$(COMPOSE) down --rmi all --volumes --remove-orphans
	docker volume prune -f
	docker builder prune -f
