package consumer

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"

	"github.com/Natthapong/kafka-avro-poc/producer"
	"github.com/Shopify/sarama"
	"github.com/linkedin/goavro"
)

var schema string

type ConsumerGroupHandler interface {
	sarama.ConsumerGroupHandler
	WaitReady()
	Reset()
}

type ConsumerGroup struct {
	cg sarama.ConsumerGroup
}

func init() {
	schemaBytes, err := ioutil.ReadFile("cardspending.avsc")
	if err != nil {
		panic(fmt.Sprintf("Error read schema %s", err))
	}
	schema = string(schemaBytes)
}

func NewConsumerGroup(broker string, topics []string, group string, handler ConsumerGroupHandler) (*ConsumerGroup, error) {
	ctx := context.Background()
	conf := sarama.NewConfig()
	conf.Version = sarama.V2_6_0_0
	conf.ClientID = "sasl_scram_client"
	conf.Net.SASL.Enable = true
	conf.Net.SASL.User = os.Getenv("SASL_USER")
	conf.Net.SASL.Password = os.Getenv("SASL_PASSWORD")
	conf.Net.SASL.Handshake = true
	conf.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}
	conf.Net.TLS.Enable = true
	conf.Net.TLS.Config = tlsConfig
	conf.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	conf.Consumer.Group.Heartbeat.Interval = 5 * time.Second
	conf.Consumer.Offsets.Initial = sarama.OffsetOldest

	brokers := strings.Split(broker, ",")
	client, err := sarama.NewConsumerGroup(brokers, group, conf)
	if err != nil {
		panic(err)
	}
	go func() {
		for {
			err := client.Consume(ctx, topics, handler)
			if err != nil {
				if err == sarama.ErrClosedConsumerGroup {
					break
				} else {
					panic(err)
				}
			}
			if ctx.Err() != nil {
				return
			}
			handler.Reset()
		}
	}()

	handler.WaitReady() // Await till the consumer has been set up
	log.Println("Sarama consumer up and running!...")

	return &ConsumerGroup{
		cg: client,
	}, nil
}

func (c *ConsumerGroup) Close() error {
	return c.cg.Close()
}

type ConsumerSessionMessage struct {
	Session sarama.ConsumerGroupSession
	Message *sarama.ConsumerMessage
}

func decodeMessage(data []byte) error {
	var msg producer.CardSpending

	avroCodec, err := goavro.NewCodec(schema)
	if err != nil {
		log.Fatal("goavro new codec error: ", err)
	}

	// Convert binary Avro data back to native Go form
	native, _, err := avroCodec.NativeFromBinary(data)
	if err != nil {
		log.Fatal("goavro native from binary error: ", err)
		return err
	}

	// Convert native Go form to textual Avro data
	textual, err := avroCodec.TextualFromNative(nil, native)
	if err != nil {
		log.Fatal("goavro textual from native error: ", err)
		return err
	}

	// NOTE: Textual encoding will show all fields, even those with values that
	// match their default values
	fmt.Println(string(textual))
	// Output: {"next":{"LongList":{"next":null}}}

	if json.Unmarshal(textual, &msg); err != nil {
		return err
	}

	return nil
}

func StartSyncConsumer(broker, topic string) (*ConsumerGroup, error) {
	handler := NewSyncConsumerGroupHandler(func(data []byte) error {
		if err := decodeMessage(data); err != nil {
			return err
		}
		return nil
	})
	consumer, err := NewConsumerGroup(broker, []string{topic}, "uct", handler)
	if err != nil {
		return nil, err
	}
	return consumer, nil
}
