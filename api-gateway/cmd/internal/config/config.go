package config

import (
	"os"
	"strings"
)

type Config struct {
	APIPort       string
	APIKey        string
	KafkaBrokers  []string
	KafkaTopicRaw string
	KafkaTopicDLQ string
}

func LoadConfig() *Config {
	brokersEnv := os.Getenv("KAFKA_BROKERS")
	if brokersEnv == "" {
		brokersEnv = "kafka:9092"
	}

	apiKey := os.Getenv("API_KEY")
	if apiKey == "" {
		apiKey = "5vRSCBOv15e2Gpaaf1EAjD4yE9opWffR"
	}

	return &Config{
		APIPort:       getEnv("API_PORT", "8080"),
		APIKey:        apiKey,
		KafkaBrokers:  strings.Split(brokersEnv, ","),
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
