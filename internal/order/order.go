// Package order is the Order Service: an HTTP API, a Kafka consumer and DynamoDB storage.
package order

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"go_services/internal/itempb"
)

const (
	TopicCreate    = "orders.create"
	TopicDelivered = "orders.delivered"

	StatusPending   = "PENDING"
	StatusReceived  = "RECEIVED"
	StatusProcessed = "PROCESSED"
	StatusShipped   = "SHIPPED"
	StatusDelivered = "DELIVERED"
	StatusFailed    = "FAILED"
)

// Hard-coded on every order, to keep this project simple.
const (
	merchantID       = "M-1"
	serviceID        = "S-1"
	deliveryFeePaise = 3000
)

// previousStatus maps each status a client may set to the only status it may come from.
var previousStatus = map[string]string{
	StatusProcessed: StatusReceived,
	StatusShipped:   StatusProcessed,
	StatusDelivered: StatusShipped,
}

// orderNamespace is fixed, so the same key always gives the same order ID.
var orderNamespace = uuid.MustParse("6f3a1c52-8d47-4e0b-9a26-1b7e5d90c3f4")

// Order is stored in DynamoDB, sent on Kafka and returned by the API.
type Order struct {
	OrderID          string `json:"order_id" dynamodbav:"order_id"`
	ItemID           int64  `json:"item_id" dynamodbav:"item_id"`
	Qty              int64  `json:"qty" dynamodbav:"qty"`
	Status           string `json:"status" dynamodbav:"status"`
	TotalPaise       int64  `json:"total_paise" dynamodbav:"total_paise"`
	MerchantID       string `json:"merchant_id" dynamodbav:"merchant_id"`
	ServiceID        string `json:"service_id" dynamodbav:"service_id"`
	DeliveryFeePaise int64  `json:"delivery_fee_paise" dynamodbav:"delivery_fee_paise"`
}

// Server holds the connections the Order Service uses.
type Server struct {
	Dynamo *dynamodb.Client
	Kafka  *kafka.Writer
	Items  itempb.ItemServiceClient
}

// OrderID derives the order ID from an Idempotency-Key. UUID v5 is a hash of
// its input, so a repeated request gets the same ID.
func OrderID(idempotencyKey string) string {
	return uuid.NewSHA1(orderNamespace, []byte(idempotencyKey)).String()
}
