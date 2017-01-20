// Copyright 2016 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by a BSD-style license.

// Package gxkafka encapsulates some kafka functions based on github.com/Shopify/sarama.
// MOD : 2016-06-01 05:57
package gxkafka

import (
	"fmt"
	"strings"
	"sync"
)

import (
	"github.com/AlexStocks/goext/strings"
	"github.com/Shopify/sarama"
	Log "github.com/alecthomas/log4go"
	"github.com/wvanbergen/kafka/consumergroup"
	"github.com/wvanbergen/kazoo-go"
)

const (
	OFFSETS_PROCESSING_TIMEOUT_SECONDS = 10e9
	OFFSETS_COMMIT_INTERVAL            = 10e9
)

// MessageCallback is a short notation of a callback function for incoming Kafka message.
type (
	MessageCallback func(msg *sarama.ConsumerMessage) error

	empty struct{}

	Consumer struct {
		consumerGroup string
		zookeeper     string
		topics        string
		clientID      string
		cb            MessageCallback

		cg   *consumergroup.ConsumerGroup
		done chan empty
		wg   sync.WaitGroup
	}
)

// NewConsumer constructs a Consumer.
// @clientID should applied for sarama.validID [sarama config.go:var validID = regexp.MustCompile(`\A[A-Za-z0-9._-]+\z`)]
// NewConsumer 之所以不能直接以brokers当做参数，是因为用到了consumer group，各个消费者的信息要存到zk中
func NewConsumer(clientID string, zookeeper string, topicList string, consumerGroup string, cb MessageCallback) (*Consumer, error) {
	c := &Consumer{
		consumerGroup: clientID,
		zookeeper:     zookeeper,
		topics:        topicList,
		clientID:      clientID,
		cb:            cb,
		done:          make(chan empty),
	}

	if consumerGroup == "" || topicList == "" || zookeeper == "" || cb == nil {
		return c, fmt.Errorf("@consumerGroup:%s, @topicList:%s, @zookeeper:%s, @cb:%v",
			consumerGroup, topicList, zookeeper, cb)
	}

	return c, nil
}

// Start runs the process of consuming. It is blocking.
func (c *Consumer) Start() error {
	var (
		err         error
		kafkatopics []string
		zkNodes     []string
		config      *consumergroup.Config
	)

	config = consumergroup.NewConfig()

	// 这个属性仅仅影响第一次启动时候获取value的offset，
	// 后面再次启动的时候如果其他conf不变则会根据上次commit的offset开始获取value
	config.ClientID = c.clientID
	config.Offsets.Initial = sarama.OffsetNewest
	config.Offsets.ProcessingTimeout = OFFSETS_PROCESSING_TIMEOUT_SECONDS
	config.Offsets.CommitInterval = OFFSETS_COMMIT_INTERVAL
	zkNodes, config.Zookeeper.Chroot = kazoo.ParseConnectionString(c.zookeeper)
	kafkatopics = strings.Split(c.topics, ",")
	c.cg, err = consumergroup.JoinConsumerGroup(c.consumerGroup, kafkatopics, zkNodes, config)
	if err != nil {
		return err
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.run()
	}()

	return nil
}

func (c *Consumer) run() {
	var (
		err        error
		eventCount int32
		offsets    map[string]map[int32]int64
		messageKey string
	)

	offsets = make(map[string]map[int32]int64)
	for {
		select {
		case message := <-c.cg.Messages():
			if offsets[message.Topic] == nil {
				offsets[message.Topic] = make(map[int32]int64)
			}

			eventCount += 1
			if offsets[message.Topic][message.Partition] != 0 && offsets[message.Topic][message.Partition] != message.Offset-1 {
				Log.Warn("Unexpected offset on %s:%d. Expected %d, found %d, diff %d.\n",
					message.Topic, message.Partition, offsets[message.Topic][message.Partition]+1,
					message.Offset, message.Offset-offsets[message.Topic][message.Partition]+1)
			}

			// Simulate processing time
			messageKey = string(message.Key)
			if len(messageKey) != 0 && messageKey != "\"\"" {
				Log.Info("idx:%d, messge{key:%v, topic:%v, partition:%v, offset:%v, value:%v}\n",
					eventCount, messageKey, message.Topic, message.Partition, message.Offset, gxstrings.String(message.Value))
			} else {
				Log.Info("idx:%d, messge{topic:%v, partition:%v, offset:%v, value:%v}\n",
					eventCount, message.Topic, message.Partition, message.Offset, gxstrings.String(message.Value))
			}

			// message -> PushNotification
			if err = c.cb(message); err != nil {
				Log.Warn("Consumer.callback(kafka message{topic:%s, key:%s, value:%s}) = error(%s)",
					message.Topic, gxstrings.String(message.Key), gxstrings.String(message.Value), err)

				// 出错则直接提交offset，记录下log
				offsets[message.Topic][message.Partition] = message.Offset
				c.Commit(message)

				break
			}

			offsets[message.Topic][message.Partition] = message.Offset
		case errMsg := <-c.cg.Errors():
			Log.Error("kafka consumer error:%s", errMsg)

		case <-c.done:
			if err := c.cg.Close(); err != nil {
				Log.Warn("Error closing the consumer:%v", err)
			}

			return
		}
	}
}

func (c *Consumer) Commit(message *sarama.ConsumerMessage) {
	if err := c.cg.CommitUpto(message); err != nil {
		Log.Error("Consuming message {%v-%v-%v}, commit error:%v",
			message.Topic, gxstrings.String(message.Key), gxstrings.String(message.Value), err)
	} else {
		Log.Info("Consuming message {%v-%v-%v}, commit over!",
			message.Topic, gxstrings.String(message.Key), gxstrings.String(message.Value))
	}
}

func (c *Consumer) Stop() {
	close(c.done)
	c.wg.Wait()
}
