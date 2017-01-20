// Copyright 2016 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by a BSD-style license.

// Package gxkafka encapsulates some kafka functions based on github.com/Shopify/sarama.
// MOD: 2016-06-01 05:57
package gxkafka

import (
	"encoding/json"
	"fmt"

	"strings"
)

import (
	"github.com/Shopify/sarama"
)

const (
	HASH = iota + 1
	RANDOM
)

// Producer is interface for sending messages to Kafka.
type Producer interface {
	SendMessage(topic string, key interface{}, message interface{}) (int32, int64, error)
	SendBytes(topic string, key []byte, message []byte) (int32, int64, error)
	Stop()
}

type producer struct {
	// topic    string  // 此处不存储topic，不能把producer和某个topic绑定，否则就不能给其他topic发送消息。
	Producer sarama.SyncProducer
}

// NewProducer constructs a new SyncProducer for give brokers addresses.
// @clientID should applied for sarama.validID [sarama config.go:var validID = regexp.MustCompile(`\A[A-Za-z0-9._-]+\z`)]
func NewProducer(clientID string, brokers string, partitionMethod int, waitForAllAck bool) (Producer, error) {
	if clientID == "" || brokers == "" {
		return producer{}, fmt.Errorf("@clientID:%s, @brokers:%s", clientID, brokers)
	}

	var partitionerConstructor sarama.PartitionerConstructor
	switch partitionMethod {
	case HASH:
		partitionerConstructor = sarama.NewHashPartitioner
	case RANDOM:
		partitionerConstructor = sarama.NewRandomPartitioner
	default:
		return producer{}, fmt.Errorf("Partition method %d not supported.", partitionMethod)
	}

	var kafkaConfig = sarama.NewConfig()
	kafkaConfig.ClientID = clientID
	kafkaConfig.Producer.Return.Successes = true
	kafkaConfig.Producer.Return.Errors = true
	kafkaConfig.Producer.Partitioner = partitionerConstructor
	if waitForAllAck {
		kafkaConfig.Producer.RequiredAcks = sarama.WaitForAll
	} else {
		kafkaConfig.Producer.RequiredAcks = sarama.WaitForLocal
	}

	var brokerList []string = strings.Split(brokers, ",")
	kafkaProducer, err := sarama.NewSyncProducer(brokerList, kafkaConfig)
	if err != nil {
		return nil, err
	}

	return producer{Producer: kafkaProducer}, nil
}

// NewProducerWithZk returns a new SyncProducer for give brokers addresses.
// @clientID should applied for sarama.validID [sarama config.go:var validID = regexp.MustCompile(`\A[A-Za-z0-9._-]+\z`)]
func NewProducerWithZk(clientID string, zookeeper string, partitionMethod int, waitForAllAck bool) (Producer, error) {
	var (
		err     error
		brokers []string
	)

	if brokers, err = GetBrokerList(zookeeper); err != nil {
		return producer{}, nil
	}

	return NewProducer(clientID, strings.Join(brokers, ","), partitionMethod, waitForAllAck)
}

func (p producer) SendMessage(topic string, key interface{}, message interface{}) (partition int32, offset int64, err error) {
	msg, err := json.Marshal(message)
	if err != nil {
		return -1, -1, fmt.Errorf("cannot marshal message %v: %v", message, err)
	}

	var keyEncoder, valueEncoder sarama.Encoder
	valueEncoder = sarama.ByteEncoder(msg)
	var producerMessage = sarama.ProducerMessage{
		Topic: topic,
		Value: valueEncoder,
	}
	if key != nil {
		keyByte, err := json.Marshal(key)
		if err != nil {
			return -1, -1, fmt.Errorf("cannot marshal key%v: %v", key, err)
		}

		keyEncoder = sarama.ByteEncoder(keyByte)
		// keyEncoder = sarama.StringEncoder(key)
		producerMessage.Key = keyEncoder
	}
	partition, offset, err = p.Producer.SendMessage(&producerMessage)
	if err != nil {
		return -1, -1, fmt.Errorf("cannot send message %v: %v", message, err)
	}

	return partition, offset, nil
}

func (p producer) SendBytes(topic string, key []byte, message []byte) (partition int32, offset int64, err error) {
	var keyEncoder, valueEncoder sarama.Encoder
	valueEncoder = sarama.ByteEncoder(message)
	var producerMessage = sarama.ProducerMessage{
		Topic: topic,
		Value: valueEncoder,
	}
	if key != nil {
		keyEncoder = sarama.ByteEncoder(key)
		producerMessage.Key = keyEncoder
	}
	partition, offset, err = p.Producer.SendMessage(&producerMessage)
	if err != nil {
		return -1, -1, fmt.Errorf("cannot send message %v: %v", message, err)
	}

	return partition, offset, nil
}

func (p producer) Stop() {
	p.Producer.Close()
}
