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
	EventName string `json:"event_name,omitempty"`
	Timestamp int64  `json:"timestamp,omitempty"`
	IP        string `json:"ip,omitempty"`
	PlanID    int64  `json:"plan_id,omitempty"`
	UUID      string `json:"uuid,omitempty"`
	OS        int    `json:"os,omitempty"`
	AppID     int    `json:"app_id,omitempty"`
	Token     string `json:"token,omitempty"`
	Status    string `json:"status,omitempty"`
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

	message.EventName = "event"
	message.Timestamp = time.Now().Unix()
	message.IP = "127.0.0.1"
	message.PlanID = 123
	message.UUID = "a-b-c-d-e"
	message.OS = 2
	message.AppID = 1
	message.Token = "1234"
	message.Status = "good"

	for i := 0; i < 10; i++ {
		message.UUID = "hello:" + strconv.Itoa(i)
		partition, offset, err = producer.SendMessage(topic, message.UUID, message)
		if err != nil {
			t.Fatalf("FAILED to produce message:%v, err:%v", message, err)
		}
		t.Logf("send telemetry message:%#v, kafka partition:%d, kafka offset:%d",
			message, partition, offset)
		time.Sleep(1e9)
	}
}
