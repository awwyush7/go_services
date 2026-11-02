package order

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const tableName = "orders"

// "status" is a reserved word in DynamoDB, so expressions must say #status instead.
var statusName = map[string]string{"#status": "status"}

// CreateTable creates the orders table. DynamoDB Local starts empty each time.
func (s *Server) CreateTable(ctx context.Context) error {
	_, err := s.Dynamo.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String(tableName),
		BillingMode: types.BillingModePayPerRequest,
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("order_id"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("order_id"), KeyType: types.KeyTypeHash},
		},
	})
	var alreadyExists *types.ResourceInUseException
	if errors.As(err, &alreadyExists) {
		return nil
	}
	return err
}

// saveIfNew saves the order as PENDING and returns false if it already exists.
func (s *Server) saveIfNew(ctx context.Context, order Order) (bool, error) {
	order.Status = StatusPending
	item, err := attributevalue.MarshalMap(order)
	if err != nil {
		return false, err
	}

	_, err = s.Dynamo.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           aws.String(tableName),
		Item:                item,
		ConditionExpression: aws.String("attribute_not_exists(order_id)"),
	})
	if isConditionFailed(err) {
		return false, nil
	}
	return err == nil, err
}

// setResult records the consumer's result on the order.
func (s *Server) setResult(ctx context.Context, orderID, status string, totalPaise int64) error {
	_, err := s.Dynamo.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                aws.String(tableName),
		Key:                      orderKey(orderID),
		UpdateExpression:         aws.String("SET #status = :status, total_paise = :total"),
		ExpressionAttributeNames: statusName,
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":status": &types.AttributeValueMemberS{Value: status},
			":total":  &types.AttributeValueMemberN{Value: fmt.Sprint(totalPaise)},
		},
	})
	return err
}

// moveStatus changes the status only if the order is currently in status from.
// DynamoDB checks the condition, so two requests at once cannot both succeed.
func (s *Server) moveStatus(ctx context.Context, orderID, from, to string) (Order, error) {
	output, err := s.Dynamo.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                aws.String(tableName),
		Key:                      orderKey(orderID),
		UpdateExpression:         aws.String("SET #status = :to"),
		ConditionExpression:      aws.String("#status = :from"),
		ExpressionAttributeNames: statusName,
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":to":   &types.AttributeValueMemberS{Value: to},
			":from": &types.AttributeValueMemberS{Value: from},
		},
		ReturnValues: types.ReturnValueAllNew,
	})
	if err != nil {
		return Order{}, err
	}

	var order Order
	err = attributevalue.UnmarshalMap(output.Attributes, &order)
	return order, err
}

func orderKey(orderID string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"order_id": &types.AttributeValueMemberS{Value: orderID},
	}
}

func isConditionFailed(err error) bool {
	var conditionFailed *types.ConditionalCheckFailedException
	return errors.As(err, &conditionFailed)
}
