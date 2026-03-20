package scheduler

import (
	"context"
	"encoding/json"
	"log"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// SQSWorkerConfig holds all dependencies for the SQS long-poll worker.
// All fields degrade gracefully — empty QueueURL disables the worker entirely,
// missing HealthcheckURLs entries mean no HC pings for that adapter.
type SQSWorkerConfig struct {
	// QueueURL is the SQS queue URL (env: SQS_INGEST_QUEUE_URL).
	// If empty the worker is disabled — useful in local development.
	QueueURL string

	// Dispatchers maps adapter name to the ingestion function to run.
	// e.g. "cal-access" -> calAccessIngestAllFn closure
	Dispatchers map[string]func() error

	// HealthcheckURLs maps adapter name to its Healthchecks.io check URL.
	// e.g. "indiana" -> "https://hc-ping.com/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
	HealthcheckURLs map[string]string
}

// sqsIngestMessage is the JSON body sent by EventBridge to SQS.
// Example: {"adapter": "cal-access"}
type sqsIngestMessage struct {
	Adapter string `json:"adapter"`
}

// StartSQSWorker launches a background goroutine that long-polls the SQS queue
// and dispatches adapter runs in response to EventBridge messages.
//
// If cfg.QueueURL is empty the function logs a notice and returns nil immediately,
// allowing the server to start without SQS in local/dev environments.
//
// Messages are ALWAYS deleted from the queue after processing, regardless of
// whether the adapter run succeeded or failed, to prevent retry floods.
//
// The goroutine exits cleanly when ctx is cancelled (graceful shutdown).
func StartSQSWorker(ctx context.Context, cfg SQSWorkerConfig) error {
	if cfg.QueueURL == "" {
		log.Printf("sqs worker: disabled (SQS_INGEST_QUEUE_URL not set)")
		return nil
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return err
	}

	sqsClient := sqs.NewFromConfig(awsCfg)

	go func() {
		log.Printf("sqs worker: started, polling %s", cfg.QueueURL)
		for {
			// Check for context cancellation before each poll.
			select {
			case <-ctx.Done():
				log.Printf("sqs worker: context cancelled, stopping")
				return
			default:
			}

			// Long-poll up to 20 seconds per call.
			out, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
				QueueUrl:            &cfg.QueueURL,
				MaxNumberOfMessages: 1,
				WaitTimeSeconds:     20,
			})
			if err != nil {
				if ctx.Err() != nil {
					// Context was cancelled during the poll — exit cleanly.
					log.Printf("sqs worker: shutting down after context cancel")
					return
				}
				log.Printf("sqs worker: receive error: %v", err)
				time.Sleep(5 * time.Second)
				continue
			}

			for _, msg := range out.Messages {
				processMessage(ctx, sqsClient, cfg, msg)
			}
		}
	}()

	return nil
}

// processMessage parses one SQS message, dispatches the adapter run, and
// deletes the message unconditionally when done.
func processMessage(ctx context.Context, client *sqs.Client, cfg SQSWorkerConfig, msg sqstypes.Message) {
	body := ""
	if msg.Body != nil {
		body = *msg.Body
	}

	var parsed sqsIngestMessage
	if err := json.Unmarshal([]byte(body), &parsed); err != nil {
		log.Printf("sqs worker: failed to parse message body %q: %v", body, err)
		deleteMessage(ctx, client, cfg.QueueURL, msg)
		return
	}

	adapterName := parsed.Adapter
	if adapterName == "" {
		log.Printf("sqs worker: message missing adapter field, body: %q", body)
		deleteMessage(ctx, client, cfg.QueueURL, msg)
		return
	}

	dispatchFn, ok := cfg.Dispatchers[adapterName]
	if !ok {
		log.Printf("sqs worker: unknown adapter %q, discarding message", adapterName)
		deleteMessage(ctx, client, cfg.QueueURL, msg)
		return
	}

	hcURL := ""
	if cfg.HealthcheckURLs != nil {
		hcURL = cfg.HealthcheckURLs[adapterName]
	}

	log.Printf("sqs worker: dispatching adapter %q", adapterName)
	if err := RunWithHealthcheck(ctx, hcURL, dispatchFn); err != nil {
		log.Printf("sqs worker: adapter %q failed: %v", adapterName, err)
	} else {
		log.Printf("sqs worker: adapter %q completed successfully", adapterName)
	}

	// Always delete — even on error — so the message does not re-deliver.
	deleteMessage(ctx, client, cfg.QueueURL, msg)
}

// deleteMessage deletes a single SQS message. Errors are logged but not returned
// because the worker must continue processing subsequent messages.
func deleteMessage(ctx context.Context, client *sqs.Client, queueURL string, msg sqstypes.Message) {
	_, err := client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      &queueURL,
		ReceiptHandle: msg.ReceiptHandle,
	})
	if err != nil {
		log.Printf("sqs worker: failed to delete message (receipt handle %v): %v", msg.ReceiptHandle, err)
	}
}
