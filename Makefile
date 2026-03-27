PROJECT_NAME=JobPulse
COMPOSE=docker compose

.PHONY: help build up pull down logs ps clean restart

help:
	@echo "Available commands:"
	@echo "make up                 Build & start (smart pull)"
	@echo "make down               Stop containers"
	@echo "make clean              Remove containers, images, volumes"
	@echo "make logs               Follow container logs"
	@echo "make ps                 List running containers"
	@echo "make restart            Restart the application (still keeps volumes)"


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
