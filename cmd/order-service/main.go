// Command order-service runs the HTTP API and the Kafka consumer.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"go_services/internal/itempb"
	"go_services/internal/order"
)

const (
	httpAddr        = ":8080"
	itemServiceAddr = "localhost:50061"
	kafkaBroker     = "localhost:9092"
	dynamoEndpoint  = "http://localhost:8000"
)

func main() {
	ctx := context.Background()

	// DynamoDB Local accepts any credentials.
	dynamo := dynamodb.New(dynamodb.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(dynamoEndpoint),
		Credentials:  credentials.NewStaticCredentialsProvider("local", "local", ""),
	})

	// This connects on first use, so Item Service can start after this one.
	conn, err := grpc.NewClient(itemServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fatal("create item service client", err)
	}

	server := &order.Server{
		Dynamo: dynamo,
		Items:  itempb.NewItemServiceClient(conn),
		Kafka: &kafka.Writer{
			Addr: kafka.TCP(kafkaBroker),
			// The default is 1s, which would make every POST wait up to a second.
			BatchTimeout: 10 * time.Millisecond,
		},
	}

	if err := server.CreateTable(ctx); err != nil {
		fatal("create table", err)
	}

	// Run the consumer in the background, alongside the HTTP server.
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBroker},
		GroupID: "order-service",
		Topic:   order.TopicCreate,
	})
	go server.Consume(ctx, reader)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /orders", server.CreateOrder)
	mux.HandleFunc("PATCH /orders/{id}/status", server.UpdateOrderStatus)

	slog.Info("order-service listening", "addr", httpAddr)
	if err := http.ListenAndServe(httpAddr, mux); err != nil {
		fatal("serve", err)
	}
}

func fatal(message string, err error) {
	slog.Error(message, "error", err)
	os.Exit(1)
}
