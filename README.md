# Item Service + Order Service

A small onboarding project with two Go services for a food-ordering flow.

- **Item Service** (gRPC, MySQL, Redis): `GetItem`, `UpdateStock`, `ReserveStock`
- **Order Service** (HTTP, Kafka, DynamoDB): `POST /orders`, `PATCH /orders/{id}/status`

```
POST /orders ──> Kafka ──> consumer ──> DynamoDB: save as PENDING
                              │
                              ├──> Item Service GetItem       (price)
                              ├──> Item Service ReserveStock  (take stock, once per order)
                              │
                              ├──> DynamoDB: RECEIVED or FAILED
                              │
                              └──> commit the Kafka offset
```

If anything fails or the service crashes, the order is processed again. `ReserveStock` runs in one MySQL transaction and records each order in `stock_reservations`, so a retry never takes stock twice.

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

# Takes stock once for this order_id. Calling it again changes nothing.
grpcurl -plaintext -proto proto/item.proto -d '{"order_id": "test-1", "item_id": 7, "qty": 1}' \
  localhost:50061 item.ItemService/ReserveStock

# Create an order, then watch the order-service terminal for the result
curl -X POST localhost:8080/orders -H 'Idempotency-Key: demo-1' \
  -d '{"item_id": 7, "qty": 2}'

# Use the order_id from the response
curl -X PATCH localhost:8080/orders/<order_id>/status -d '{"status": "PROCESSED"}'
```

The status path is `RECEIVED -> PROCESSED -> ON_THE_WAY -> DELIVERED`. Skipping a step returns 409.

Reaching `DELIVERED` publishes an `OrderDeliveredEvent` to the Kafka topic `orders.delivered`, which the Java billing service reads. To read it:

```sh
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:19092 --topic orders.delivered --from-beginning
```

## Left out to keep it simple

- `GET /orders/{id}`, `ListItems`, `CreateItem`
- A sweeper for stuck orders, and expiry or release of reservations
- Graceful shutdown, config from environment variables
- Fake repositories for unit tests
