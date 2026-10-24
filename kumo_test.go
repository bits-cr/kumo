package kumo_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/sivchari/kumo"
)

const (
	deliveryBus   = "kumo-newserver-bus"
	deliveryRule  = "kumo-newserver-rule"
	deliveryQueue = "kumo-newserver-queue"
)

// TestNewServerEventBridgeToSQS checks that an EventBridge rule delivers to
// its SQS target on NewServer's random port, not only on the default 4566.
//
// Not parallel: every NewServer shares the global service registry, so
// concurrent servers see each other's resources.
func TestNewServerEventBridgeToSQS(t *testing.T) {
	srv := kumo.NewServer()
	t.Cleanup(srv.Close)

	cfg := aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""),
	}
	events := eventbridge.NewFromConfig(cfg, func(o *eventbridge.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
	queues := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
	ctx := t.Context()

	queue := createDeliveryTarget(ctx, t, events, queues)

	if _, err := events.PutEvents(ctx, &eventbridge.PutEventsInput{
		Entries: []ebtypes.PutEventsRequestEntry{{
			EventBusName: aws.String(deliveryBus),
			Source:       aws.String("my.app"),
			DetailType:   aws.String("OrderCreated"),
			Detail:       aws.String(`{"orderId":"o-1"}`),
		}},
	}); err != nil {
		t.Fatalf("put events: %v", err)
	}

	// Delivery is asynchronous, so poll the queue.
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		out, err := queues.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:        queue,
			WaitTimeSeconds: 1,
		})
		if err != nil {
			t.Fatalf("receive message: %v", err)
		}

		if len(out.Messages) > 0 {
			return
		}
	}

	t.Fatal("the event never reached the SQS target")
}

// createDeliveryTarget creates a queue, and a bus whose rule targets it, and
// deletes them when the test ends so reruns in the same process
// (-count>1) start clean. It returns the queue URL.
//
//nolint:funlen // Each resource is created and its cleanup registered in turn.
func createDeliveryTarget(ctx context.Context, t *testing.T, events *eventbridge.Client, queues *sqs.Client) *string {
	t.Helper()

	queue, err := queues.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(deliveryQueue)})
	if err != nil {
		t.Fatalf("create queue: %v", err)
	}

	t.Cleanup(func() {
		if _, err := queues.DeleteQueue(context.Background(), &sqs.DeleteQueueInput{QueueUrl: queue.QueueUrl}); err != nil {
			t.Errorf("delete queue: %v", err)
		}
	})

	attrs, err := queues.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       queue.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	if err != nil {
		t.Fatalf("get queue attributes: %v", err)
	}

	if _, err := events.CreateEventBus(ctx, &eventbridge.CreateEventBusInput{Name: aws.String(deliveryBus)}); err != nil {
		t.Fatalf("create event bus: %v", err)
	}

	t.Cleanup(func() {
		if _, err := events.DeleteEventBus(context.Background(), &eventbridge.DeleteEventBusInput{
			Name: aws.String(deliveryBus),
		}); err != nil {
			t.Errorf("delete event bus: %v", err)
		}
	})

	if _, err := events.PutRule(ctx, &eventbridge.PutRuleInput{
		Name:         aws.String(deliveryRule),
		EventBusName: aws.String(deliveryBus),
		EventPattern: aws.String(`{"source":["my.app"]}`),
	}); err != nil {
		t.Fatalf("put rule: %v", err)
	}

	t.Cleanup(func() {
		if _, err := events.DeleteRule(context.Background(), &eventbridge.DeleteRuleInput{
			Name:         aws.String(deliveryRule),
			EventBusName: aws.String(deliveryBus),
			Force:        true,
		}); err != nil {
			t.Errorf("delete rule: %v", err)
		}
	})

	if _, err := events.PutTargets(ctx, &eventbridge.PutTargetsInput{
		Rule:         aws.String(deliveryRule),
		EventBusName: aws.String(deliveryBus),
		Targets: []ebtypes.Target{{
			Id:  aws.String("queue"),
			Arn: aws.String(attrs.Attributes[string(sqstypes.QueueAttributeNameQueueArn)]),
		}},
	}); err != nil {
		t.Fatalf("put targets: %v", err)
	}

	return queue.QueueUrl
}
