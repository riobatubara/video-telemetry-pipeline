package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"api-gateway/internal/config"
	"api-gateway/internal/validator"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Producer struct {
	producer *kafka.Producer
	cfg      *config.Config
	dataChan chan *kafka.Message
	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc

	// Thread-Safe Client Session Registry
	mu             sync.RWMutex
	activeSessions map[string]time.Time
}

func NewProducer(cfg *config.Config, bufferSize int) (*Producer, error) {
	// Build Confluent Configuration Map
	configMap := &kafka.ConfigMap{
		"bootstrap.servers":   strings.Join(cfg.KafkaBrokers, ","), // Connects using env array values
		"acks":                "1",                                 // Confirms local broker receipt
		"compression.type":    "zstd",                              // Text performance optimization
		"go.delivery.reports": true,                                // Enables event loop status feedback
	}

	client, err := kafka.NewProducer(configMap)
	if err != nil {
		return nil, fmt.Errorf("failed to bootstrap confluent kafka client: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	p := &Producer{
		producer:       client,
		cfg:            cfg,
		dataChan:       make(chan *kafka.Message, bufferSize), // Enforces user backpressure bounds
		ctx:            ctx,
		cancel:         cancel,
		activeSessions: make(map[string]time.Time),
	}

	p.wg.Add(2)
	go p.processQueue()
	go p.handleDeliveryReports()

	return p, nil
}

// TrackSession tracks your active sessions in memory using thread-safe structures
func (p *Producer) TrackSession(sessid string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.activeSessions[sessid] = time.Now()
}

// RoutePayload serializes data maps and streams them into your backpressure queues
func (p *Producer) RoutePayload(payloads []validator.Payload, isValid bool) bool {
	topic := p.cfg.KafkaTopicRaw
	if !isValid {
		topic = p.cfg.KafkaTopicDLQ
	}

	payloadBytes, err := json.Marshal(payloads)
	if err != nil {
		return false
	}

	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          payloadBytes,
	}

	// Backpressure Bound Checks
	select {
	case p.dataChan <- msg:
		if isValid && len(payloads) > 0 {
			// Safely track session using the first payloads's ID
			p.TrackSession(payloads[0].Sessid)
		}
		return true
	default:
		log.Printf("[KAFKA ERROR] BackPressure Critical, internal channel is full. Dropping payload block for system safety.")
		return false // API will intercepts this return to emit HTTP 429 backpressure status code
	}
}

func (p *Producer) processQueue() {
	defer p.wg.Done()
	for {
		select {
		case msg, ok := <-p.dataChan:
			if !ok {
				// Channel closed during shutdown
				return
			}

			// Produce updates asynchronously straight into confluent client channels
			err := p.producer.Produce(msg, nil)
			if err != nil {
				log.Printf("[KAFKA ERROR]: Direct buffer drop failed: %v", err)
			}

		case <-p.ctx.Done():
			return
		}
	}
}

func (p *Producer) handleDeliveryReports() {
	defer p.wg.Done()
	for {
		select {
		case ev, ok := <-p.producer.Events():
			if !ok {
				// Producer closed, exit goroutine
				return
			}

			switch m := ev.(type) {
			case *kafka.Message:
				if m.TopicPartition.Error != nil {
					log.Printf("[KAFKA ERRROR]: Broker processing error on message drop: %v", m.TopicPartition.Error)
				}
			}

		case <-p.ctx.Done():
			return
		}
	}
}

// Close safely drains out the remaining payload structures before shutting down
func (p *Producer) Close() error {
	p.cancel()

	// Closing channel after cancel ensures select unblocks cleanly
	close(p.dataChan)

	p.wg.Wait()

	// Flush remaining in-flight confluent messages for up to 5 seconds
	p.producer.Flush(5000)

	p.producer.Close()
	return nil
}
