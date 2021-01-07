package producer

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/linkedin/goavro"
)

var schema string

type CardSpending struct {
	ID      int    `json:"id"`
	Message string `json:"message"`
}

type Producer struct {
	p sarama.AsyncProducer
	s string
}

func init() {
	schemaBytes, err := ioutil.ReadFile("cardspending.avsc")
	if err != nil {
		panic(fmt.Sprintf("Error read schema %s", err))
	}
	schema = string(schemaBytes)
}

func NewProducer(broker, topic string) (*Producer, error) {
	conf := sarama.NewConfig()
	conf.Metadata.Full = true
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
	conf.Producer.Retry.Max = 5
	conf.Producer.RequiredAcks = sarama.WaitForAll
	conf.Producer.Return.Successes = true
	conf.Producer.Return.Errors = true

	brokers := strings.Split(broker, ",")
	producer, err := sarama.NewAsyncProducer(brokers, conf)
	if err != nil {
		return nil, err
	}

	return &Producer{
		p: producer,
		s: schema,
	}, nil
}

func (p *Producer) StartProduce(done chan struct{}, topic string) {
	for i := 0; i < 10; i++ {
		avroCodec, err := goavro.NewCodec(schema)
		if err != nil {
			log.Fatal("goavro new codec error: ", err)
		}

		in := CardSpending{ID: i, Message: fmt.Sprintf("MsgNo. %d", i)}
		msgBytes, err := json.Marshal(in)
		if err != nil {
			log.Println("json marshal error: ", err)
			continue
		}
		native, _, err := avroCodec.NativeFromTextual(msgBytes)
		if err != nil {
			log.Fatal("goavro native from textual error: ", err)
		}
		binaryValue, err := avroCodec.BinaryFromNative(nil, native)
		if err != nil {
			log.Fatal("goavro convert to binary error: ", err)
		}

		strTime := strconv.Itoa(int(time.Now().Unix()))
		select {
		case p.p.Input() <- &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(strTime),
			Value: sarama.ByteEncoder(binaryValue),
		}:
			fmt.Printf("produced %d messages\n", i)
		case err := <-p.p.Errors():
			fmt.Printf("Failed to send message to kafka, err: %s, msg: %s\n", err, in)
		}
	}
	<-done
}

func (p *Producer) Close() error {
	if p != nil {
		return p.p.Close()
	}
	return nil
}
