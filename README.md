# Item Service + Order Service

A small onboarding project with two Go services for a food-ordering flow.

- **Item Service** (gRPC, MySQL, Redis): `GetItem`, `UpdateStock`
- **Order Service** (HTTP, Kafka, DynamoDB): `POST /orders`, `PATCH /orders/{id}/status`

```
POST /orders ──> Kafka ──> consumer ──> DynamoDB: save as PENDING
                              │
                              ├──> Item Service GetItem      (price)
                              ├──> Item Service UpdateStock  (take stock)
                              │
                              └──> DynamoDB: RECEIVED or FAILED
```

## Run it

```sh
export COMPOSE="docker compose"   # skip this if you use podman
make up                           # wait about 30s for MySQL and Kafka
make run-item                     # terminal 1
make run-order                    # terminal 2
```

`make test` runs the unit tests. `make down` stops everything and deletes all data.

## Try it

The seed data has 18 dishes. Dish 5 has no stock.

```sh
# Install once: brew install grpcurl
grpcurl -plaintext -proto proto/item.proto -d '{"id": 7}' \
  localhost:50061 item.ItemService/GetItem

grpcurl -plaintext -proto proto/item.proto -d '{"item_id": 7, "delta": 10}' \
  localhost:50061 item.ItemService/UpdateStock

# Create an order, then watch the order-service terminal for the result
curl -X POST localhost:8080/orders -H 'Idempotency-Key: demo-1' \
  -d '{"item_id": 7, "qty": 2}'

# Use the order_id from the response
curl -X PATCH localhost:8080/orders/<order_id>/status -d '{"status": "PROCESSED"}'
```

The status path is `RECEIVED -> PROCESSED -> SHIPPED -> DELIVERED`. Skipping a step returns 409.

## Left out to keep it simple

- Orders with several dishes, and all-or-nothing reservations across them
- `GET /orders/{id}`, `ListItems`, `CreateItem`
- Retries, manual Kafka offset commits, and a sweeper for stuck orders
- An `orders.delivered` event
- Graceful shutdown, config from environment variables
- Fake repositories for unit tests
