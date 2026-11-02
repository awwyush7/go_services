# Override to use Docker: make up COMPOSE="docker compose"
COMPOSE ?= podman compose

.PHONY: proto up down run-item run-order test

proto:
	protoc --go_out=. --go_opt=module=go_services \
		--go-grpc_out=. --go-grpc_opt=module=go_services \
		proto/item.proto

up:
	$(COMPOSE) up -d

# -v also deletes the MySQL data, so MySQL and the in-memory DynamoDB start empty together.
down:
	$(COMPOSE) down -v

run-item:
	go run ./cmd/item-service

run-order:
	go run ./cmd/order-service

test:
	go test ./...
