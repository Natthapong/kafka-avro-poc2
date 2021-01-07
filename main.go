package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Natthapong/kafka-avro-poc/consumer"
	"github.com/Natthapong/kafka-avro-poc/producer"
	"github.com/Shopify/sarama"
)

const (
	produceMode = "produce"
	consumeMode = "consume"
	topic       = "ucenter"
)

var mode string
var broker string
var verbose bool = false

func init() {
	flag.StringVar(&mode, "m", "", "cmd mode, 'produce', 'consume'")
	flag.StringVar(&broker, "h", "pkc-ew3qg.asia-southeast2.gcp.confluent.cloud:9092", "kafka broker host:port")
	flag.BoolVar(&verbose, "verbose", false, "Sarama logging")
}

func main() {
	if verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}
	flag.Parse()
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
	}
	if mode == "" {
		flag.Usage()
		return
	}

	var done = make(chan struct{})
	defer close(done)

	switch mode {
	case produceMode:
		producer, err := producer.NewProducer(broker, topic)
		if err != nil {
			panic(err)
		}
		defer producer.Close()
		go producer.StartProduce(done, topic)
	case consumeMode:
		consumer, err := consumer.StartSyncConsumer(broker, topic)
		if err != nil {
			panic(err)
		}
		defer consumer.Close()
	default:
		flag.Usage()
		return
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	fmt.Println("received signal", <-c)
}
