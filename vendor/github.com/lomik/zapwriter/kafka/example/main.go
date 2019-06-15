package main

import (
	"fmt"
	"log"
	"time"

	"github.com/lomik/zapwriter"
	_ "github.com/lomik/zapwriter/kafka"
	"go.uber.org/zap"
)

func main() {
	config := []zapwriter.Config{
		{Logger: "", File: "none", Level: "warn", Encoding: "mixed", EncodingTime: "iso8601", EncodingDuration: "string"},
		{Logger: "", File: "kafka://127.0.0.1:9092/?topic=zapwriter&sync=0&error_logger=kafka_error", Level: "info", Encoding: "json", EncodingTime: "epoch", EncodingDuration: "nanos"},
		{Logger: "kafka_error", File: "stderr", Level: "error", Encoding: "mixed", EncodingTime: "iso8601", EncodingDuration: "string"},
	}

	if err := zapwriter.ApplyConfig(config); err != nil {
		log.Fatal(err)
	}

	for {
		zapwriter.Logger("access").Error("error message",
			zap.Error(fmt.Errorf("error object")),
			zap.Duration("time", time.Second),
		)
		time.Sleep(100 * time.Millisecond)
	}
}
