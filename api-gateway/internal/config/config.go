package config

import (
	"os"
	"strings"
)

type Config struct {
	APIPort       string   // Port number for the API server to listen on.
	APIKey        string   // Secret key used for API authentication.
	KafkaBrokers  []string // List of Kafka broker addresses.
	KafkaTopicRaw string   // Kafka topic for raw video telemetry data.
	KafkaTopicDLQ string   // Kafka topic for dead-letter queue (DLQ) messages.
}

func LoadConfig() *Config {
	// Fall back to a default broker address if the environment variable is empty.
	brokersEnv := os.Getenv("KAFKA_BROKERS")
	if brokersEnv == "" {
		brokersEnv = "kafka:9092"
	}

	// Fall back to a default secret key if the environment variable is empty.
	apiKey := os.Getenv("API_KEY")
	if apiKey == "" {
		apiKey = "apikeysecret"
	}

	return &Config{
		APIPort:       getEnv("API_PORT", "8080"),
		APIKey:        apiKey,
		KafkaBrokers:  strings.Split(brokersEnv, ","), // Convert comma-separated string to a slice.
		KafkaTopicRaw: getEnv("KAFKA_TOPIC_RAW", "video.telemetry.raw"),
		KafkaTopicDLQ: getEnv("KAFKA_TOPIC_DLQ", "video.telemetry.dlq"),
	}
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}
