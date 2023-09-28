package main

import (
	"fmt"
	"github.com/segmentio/kafka-go"
	log2 "log"
	"party-manager/internal/config"
	"time"
)

func main() {
	cfg, err := config.LoadGlobalConfig()
	if err != nil {
		panic(err)
	}

	kafkaCfg := cfg.Kafka

	w := &kafka.Writer{
		Addr:         kafka.TCP(fmt.Sprintf("%s:%d", kafkaCfg.Host, kafkaCfg.Port)),
		Topic:        "mc-connections",
		Balancer:     &kafka.LeastBytes{},
		Async:        true,
		BatchTimeout: 100 * time.Millisecond,
		ErrorLogger:  kafka.LoggerFunc(log2.Printf),
	}

	w.WriteMessages()

}
