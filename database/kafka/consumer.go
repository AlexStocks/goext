// Copyright 2016 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by a BSD-style license.

// Package gxkafka encapsulates some kafka functions based on github.com/Shopify/sarama.
// MOD : 2016-06-01 05:57
package gxkafka

import (
	"fmt"
	"os"
	"os/signal"

	"log"
	"strings"
	"syscall"
)

import (
	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kafka/consumergroup"
	"github.com/wvanbergen/kazoo-go"
)

// MessageCallback is a short notation of a callback function for incoming Kafka message.
type MessageCallback func(msg *sarama.ConsumerMessage) error

type Consumer struct {
	CGName         string
	ZkAddressNodes string
	Topics         string
}

type topic struct {
	Name            string
	MessageCallback MessageCallback
	Consuming       bool
}

const (
	OFFSETS_PROCESSING_TIMEOUT_SECONDS = 10e9
	OFFSETS_COMMIT_INTERVAL            = 10e9
)

func NewConsumer(cgName, topicList, zkAddressList string) (Consumer, error) {
	c := Consumer{}

	if cgName == "" {
		return c, fmt.Errorf(" consumer group name cannot be empty")
	}

	c = Consumer{
		CGName:         cgName,
		ZkAddressNodes: zkAddressList,
		Topics:         topicList,
	}

	return c, nil
}

// Start runs the process of consuming. It is blocking.
func (c *Consumer) Start() error {
	var kafkaTopics []string
	var zkNodes []string

	var config = consumergroup.NewConfig()
	config.ClientID = "kafka-consumer1"
	// 这个属性仅仅影响第一次启动时候获取value的offset，
	// 后面再次启动的时候如果其他conf不变则会根据上次commit的offset开始获取value
	config.Offsets.Initial = sarama.OffsetNewest
	config.Offsets.ProcessingTimeout = OFFSETS_PROCESSING_TIMEOUT_SECONDS
	config.Offsets.CommitInterval = OFFSETS_COMMIT_INTERVAL
	zkNodes, config.Zookeeper.Chroot = kazoo.ParseConnectionString(c.ZkAddressNodes)
	kafkaTopics = strings.Split(c.Topics, ",")
	cg, err := consumergroup.JoinConsumerGroup(c.CGName, kafkaTopics, zkNodes, config)
	if err != nil {
		return err
	}
	// defer cg.Close()

	runConsumer(cg)
	return nil
}

func runConsumer(cg *consumergroup.ConsumerGroup) {
	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, os.Kill, syscall.SIGHUP, syscall.SIGTERM)

	go func() {
		for err := range cg.Errors() {
			log.Println(err)
		}
	}()

	var eventCount int32 = 0
	var offsets = make(map[string]map[int32]int64)
	var messageKey string
	for {
		select {
		case message := <-cg.Messages():
			if offsets[message.Topic] == nil {
				offsets[message.Topic] = make(map[int32]int64)
			}

			eventCount += 1
			if offsets[message.Topic][message.Partition] != 0 && offsets[message.Topic][message.Partition] != message.Offset-1 {
				log.Printf("Unexpected offset on %s:%d. Expected %d, found %d, diff %d.\n",
					message.Topic, message.Partition, offsets[message.Topic][message.Partition]+1,
					message.Offset, message.Offset-offsets[message.Topic][message.Partition]+1)
			}

			// Simulate processing time
			messageKey = string(message.Key)
			if len(messageKey) != 0 && messageKey != "\"\"" {
				log.Printf("idx:%d, messge{key:%v, topic:%v, partition:%v, offset:%v, value:%v}\n",
					eventCount, messageKey, message.Topic, message.Partition, message.Offset, string(message.Value))
			} else {
				log.Printf("idx:%d, messge{topic:%v, partition:%v, offset:%v, value:%v}\n",
					eventCount, message.Topic, message.Partition, message.Offset, string(message.Value))
			}

			offsets[message.Topic][message.Partition] = message.Offset
			if err := cg.CommitUpto(message); err != nil {
				log.Printf("[%s] Consuming message: %v", message.Topic, err)
			}

		case <-signals:
			if err := cg.Close(); err != nil {
				sarama.Logger.Println("Error closing the consumer", err)
				// log.Warnf("Error closing the consumer", err)
			}

			return
		}
	}
}
