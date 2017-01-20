package gxkafka

import (
	"log"
	"os"
	"strconv"
	"testing"
	"time"
)

import (
	"github.com/Shopify/sarama"
)

func init() {
	sarama.Logger = log.New(os.Stdout, "[Sarama] ", log.LstdFlags)
}

type Message struct {
	Content string "json:`content`"
}

func TestKafkaProducer(t *testing.T) {
	var (
		id        = "producer-client-id"
		zk        = "127.0.0.1:2181/kafka"
		topic     = "test1"
		err       error
		partition int32
		offset    int64
		producer  Producer
		message   Message
	)

	if producer, err = NewProducerWithZk(id, zk, HASH, true); err != nil {
		t.Errorf("NewProducerWithZk(id:%s, zk:%s) = err{%v}", id, zk, err)
	}
	defer producer.Stop()

	for i := 0; i < 10; i++ {
		message.Content = "hello:" + strconv.Itoa(i)
		partition, offset, err = producer.SendMessage(topic, message.Content, message)
		if err != nil {
			t.Fatalf("FAILED to produce message:%v, err:%v", message, err)
		}
		t.Logf("send telemetry message:%#v, kafka partition:%d, kafka offset:%d",
			message, partition, offset)
		time.Sleep(1e9)
	}
}
