package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"api-gateway/cmd/internal/config"
	"api-gateway/cmd/internal/kafka"
	"api-gateway/cmd/internal/validator"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/logger"
)

// Connection Worker Payload wrapper
type TelemetryJob struct {
	Ctx     context.Context
	Payload []validator.TelemetryPayload
	IsValid bool
}

func main() {
	cfg := config.LoadConfig()

	// Thread-Safe Client Registry
	var activeSessionRegistry sync.Map

	// Backpressure Handling & Buffered Channel
	maxConcurrentConnections := 50000
	jobQueue := make(chan TelemetryJob, maxConcurrentConnections)

	// Boot Confluent producer loop
	producer, err := kafka.NewProducer(cfg, 50000)
	if err != nil {
		log.Fatalf("Fatal system crash tracking configuration bootstrap parameters: %v", err)
	}

	// Connection Lifecycle Management Workers
	workerPoolSize := 10
	var workerWg sync.WaitGroup
	ctx, cancelWorkers := context.WithCancel(context.Background())

	for i := 0; i < workerPoolSize; i++ {
		workerWg.Add(1)
		go func(workerID int) {
			defer workerWg.Done()
			for {
				select {
				case job, ok := <-jobQueue:
					if !ok {
						return
					}

					// Verify connection lifecycle status before processing
					select {
					case <-job.Ctx.Done():
						log.Printf("[API-Gateway] Request canceled by client before processing. Skipping job.")
						continue
					default:
					}

					// Hand data to Confluent Kafka
					success := producer.RoutePacket(job.Payload, job.IsValid)

					// If successfully written and valid, track it in the Thread-Safe Registry
					if success && job.IsValid && len(job.Payload) > 0 {
						activeSessionRegistry.Store(job.Payload[0].Sessid, time.Now())
					}

				case <-ctx.Done():
					return
				}
			}
		}(i)
	}

	// Initialize Fiber
	app := fiber.New(fiber.Config{
		DisableStartupMessage: false,
		ReadTimeout:           5 * time.Second,
		WriteTimeout:          5 * time.Second,
	})

	app.Use(logger.New())

	// Security Middleware Layer
	app.Use(func(c *fiber.Ctx) error {
		apiKeyHeader := c.Get("X-API-Key")
		if apiKeyHeader == "" || apiKeyHeader != cfg.APIKey {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"status":  "error",
				"message": "Access Denied: Invalid Security Identifier Provided",
			})
		}
		return c.Next()
	})

	// The Telemetry Ingestion API
	app.Post("/api/v1/telemetry", func(c *fiber.Ctx) error {
		body := c.Body()

		// Validate raw JSON structural layout boundaries
		payloads, err := validator.ValidatePayload(body)

		// Prepare job properties
		var job TelemetryJob
		job.Ctx = c.UserContext()

		if err != nil {
			// Malformed data setup -> Route to DLQ
			job.IsValid = false
			job.Payload = []validator.TelemetryPayload{{
				TsClient: time.Now().UnixMilli(),
				Sessid:   "MALFORMED_JSON_ERROR",
				Event:    "MALFORMED",
				Value:    string(body),
			}}
		} else {
			job.IsValid = true
			job.Payload = payloads
		}

		// Backpressure Valve Select Enginer
		select {
		case jobQueue <- job:
			return c.Status(fiber.StatusAccepted).JSON(fiber.Map{"status": "queued"})
		default:
			log.Printf("[API-GATEWAY] API Gateway queue full! Rejecting incoming request.")
			return c.Status(fiber.StatusTooManyRequests).JSON(fiber.Map{
				"status":  "rejected",
				"message": "Gateway saturation spike, please backoff transmission metrics.",
			})
		}
	})

	// Gracefull Shutdown
	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		if err := app.Listen(":" + cfg.APIPort); err != nil {
			log.Printf("Server processing pipeline stopped: %v", err)
		}
	}()
	log.Printf("API-Gateway is operational, listening on Port: %s", cfg.APIPort)

	<-shutdownChan
	log.Println("Intercepted shutdown sequence, starting graceful termination...")

	// Stop accepting new requests
	if err := app.Shutdown(); err != nil {
		log.Printf("Error halting HTTP server application bounds: %v", err)
	}

	// Drain queue
	log.Println("Draining buffered job queues safely...")
	close(jobQueue)

	// Stop workers
	cancelWorkers()
	workerWg.Wait()

	// Close Kafka producer
	if err := producer.Close(); err != nil {
		log.Printf("Error closing kafka producer: %v", err)
	}

	log.Println("API-Gateway completely and safely stopped.")
}
