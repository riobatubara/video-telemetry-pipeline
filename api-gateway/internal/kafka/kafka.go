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

	mu             sync.RWMutex
	activeSessions map[string]time.Time
}

func NewProducer(cfg *config.Config, bufferSize int) (*Producer, error) {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers":   strings.Join(cfg.KafkaBrokers, ","),
		"acks":                "all", // FIXED (was 1)
		"compression.type":    "zstd",
		"go.delivery.reports": true,
	}

	client, err := kafka.NewProducer(configMap)
	if err != nil {
		return nil, fmt.Errorf("failed to bootstrap kafka client: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	p := &Producer{
		producer:       client,
		cfg:            cfg,
		dataChan:       make(chan *kafka.Message, bufferSize),
		ctx:            ctx,
		cancel:         cancel,
		activeSessions: make(map[string]time.Time),
	}

	p.wg.Add(2)
	go p.processQueue()
	go p.handleDeliveryReports()

	return p, nil
}

func (p *Producer) TrackSession(sessid string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.activeSessions[sessid] = time.Now()
}

func (p *Producer) RoutePayload(payloads []validator.Payload, isValid bool) bool {
	topic := p.cfg.KafkaTopicRaw
	if !isValid {
		topic = p.cfg.KafkaTopicDLQ
	}

	var payloadBytes []byte
	var err error

	if len(payloads) == 1 {
		payloadBytes, err = json.Marshal(payloads[0])
	} else {
		payloadBytes, err = json.Marshal(payloads)
	}

	if err != nil {
		return false
	}

	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: payloadBytes,
	}

	// FIXED: no silent drop, use timeout backpressure
	select {
	case p.dataChan <- msg:
		if isValid && len(payloads) > 0 {
			p.TrackSession(payloads[0].Sessid)
		}
		return true
	case <-time.After(500 * time.Millisecond):
		log.Printf("[KAFKA ERROR] Backpressure timeout, dropping payload")
		return false
	}
}

func (p *Producer) processQueue() {
	defer p.wg.Done()

	for {
		select {
		case msg, ok := <-p.dataChan:
			if !ok {
				return
			}

			// FIXED: retry produce
			for i := 0; i < 3; i++ {
				err := p.producer.Produce(msg, nil)
				if err == nil {
					break
				}
				log.Printf("[KAFKA ERROR] Produce retry %d: %v", i+1, err)
				time.Sleep(100 * time.Millisecond)
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
				return
			}

			switch m := ev.(type) {
			case *kafka.Message:
				if m.TopicPartition.Error != nil {
					log.Printf("[KAFKA ERROR] Delivery failed: %v", m.TopicPartition.Error)
				}
			}

		case <-p.ctx.Done():
			return
		}
	}
}

func (p *Producer) Close() error {
	// stop intake
	p.cancel()

	// stop queue
	close(p.dataChan)

	// FIXED: flush BEFORE stopping goroutines
	p.producer.Flush(5000)

	// now wait for goroutines to finish
	p.wg.Wait()

	p.producer.Close()
	return nil
}
