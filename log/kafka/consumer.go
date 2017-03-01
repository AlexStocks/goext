// Copyright 2016 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by a BSD-style license.

// Package gxkafka encapsulates some kafka functions based on github.com/Shopify/sarama.
// MOD : 2016-06-01 05:57
package gxkafka

import (
	"fmt"
	"sync"
)

import (
	"github.com/AlexStocks/goext/strings"
	"github.com/AlexStocks/goext/sync"
	"github.com/Shopify/sarama"
	Log "github.com/alecthomas/log4go"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/wvanbergen/kazoo-go"
)

const (
	OFFSETS_PROCESSING_TIMEOUT_SECONDS = 10e9
	OFFSETS_COMMIT_INTERVAL            = 10e9
)

// MessageCallback is a short notation of a callback function for incoming Kafka message.
type (
	Consumer interface {
		Start() error
		Commit(*sarama.ConsumerMessage)
		Stop()
	}

	consumer struct {
		consumerGroup string
		brokers       []string
		topics        []string
		clientID      string
		cb            ConsumerMessageCallback

		// cg   *consumergroup.ConsumerGroup
		cg   *cluster.Consumer
		done chan gxsync.Empty
		wg   sync.WaitGroup
	}
)

// NewConsumer constructs a consumer.
// @clientID should applied for sarama.validID [sarama config.go:var validID = regexp.MustCompile(`\A[A-Za-z0-9._-]+\z`)]
// NewConsumer 之所以不能直接以brokers当做参数，是因为/wvanderbergen/kafka/consumer用到了consumer group，
// 各个消费者的信息要存到zk中
func NewConsumer(
	clientID string,
	brokers []string,
	topicList []string,
	consumerGroup string,
	cb ConsumerMessageCallback,
) (Consumer, error) {

	if consumerGroup == "" || len(brokers) == 0 || len(topicList) == 0 || cb == nil {
		return nil, fmt.Errorf("@consumerGroup:%s, @brokers:%s, @topicList:%s, cb:%v",
			consumerGroup, brokers, topicList, cb)
	}

	return &consumer{
		consumerGroup: consumerGroup,
		brokers:       brokers,
		topics:        topicList,
		clientID:      clientID,
		cb:            cb,
		done:          make(chan gxsync.Empty),
	}, nil
}

func NewConsumerWithZk(
	clientID string,
	zookeeper string,
	topicList []string,
	consumerGroup string,
	cb ConsumerMessageCallback,
) (Consumer, error) {

	if consumerGroup == "" || len(topicList) == 0 || zookeeper == "" || cb == nil {
		return nil, fmt.Errorf("@consumerGroup:%s, @topicList:%s, @zookeeper:%s, @cb:%v",
			consumerGroup, topicList, zookeeper, cb)
	}

	var (
		err     error
		kz      *kazoo.Kazoo
		zkNodes []string
		brokers []string
		config  *kazoo.Config
	)

	config = kazoo.NewConfig()
	zkNodes, config.Chroot = kazoo.ParseConnectionString(zookeeper)
	if kz, err = kazoo.NewKazoo(zkNodes, config); err != nil {
		return nil, err
	}
	if brokers, err = kz.BrokerList(); err != nil {
		return nil, err
	}
	kz.Close()

	return &consumer{
		consumerGroup: consumerGroup,
		brokers:       brokers,
		topics:        topicList,
		clientID:      clientID,
		cb:            cb,
		done:          make(chan gxsync.Empty),
	}, nil
}

// Start runs the process of consuming. It is blocking.
func (c *consumer) Start() error {
	var (
		err    error
		config *cluster.Config
	)

	config = cluster.NewConfig()
	config.ClientID = c.clientID
	config.Group.Return.Notifications = true
	config.Consumer.Return.Errors = true
	// 这个属性仅仅影响第一次启动时候获取value的offset，
	// 后面再次启动的时候如果其他config不变则会根据上次commit的offset开始获取value
	// config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.Consumer.MaxProcessingTime = OFFSETS_PROCESSING_TIMEOUT_SECONDS
	// config.Consumer.Offsets.CommitInterval = OFFSETS_COMMIT_INTERVAL
	// The retention duration for committed offsets. If zero, disabled
	// (in which case the `offsets.retention.minutes` option on the
	// broker will be used).
	config.Consumer.Offsets.Retention = 0
	if c.cg, err = cluster.NewConsumer(c.brokers, c.consumerGroup, c.topics, config); err != nil {
		return err
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.run()
		Log.Info("consumer message processing goroutine done!")
	}()

	return nil
}

func (c *consumer) run() {
	var (
		err        error
		eventCount int32
		offsets    map[string]map[int32]int64
		// messageKey string
		note *cluster.Notification
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
			// messageKey = string(message.Key)
			// if len(messageKey) != 0 && messageKey != "\"\"" {
			// 	Log.Debug("idx:%d, messge{key:%v, topic:%v, partition:%v, offset:%v, value:%v}\n",
			// 		eventCount, messageKey, message.Topic, message.Partition, message.Offset, gxstrings.String(message.Value))
			// } else {
			// 	Log.Debug("idx:%d, messge{topic:%v, partition:%v, offset:%v, value:%v}\n",
			// 		eventCount, message.Topic, message.Partition, message.Offset, gxstrings.String(message.Value))
			// }

			// message -> PushNotification
			if err = c.cb(message); err != nil {
				Log.Warn("consumer.callback(kafka message{topic:%s, key:%s, value:%s}) = error(%s)",
					message.Topic, gxstrings.String(message.Key), gxstrings.String(message.Value), err)

				// 出错则直接提交offset，记录下log
				offsets[message.Topic][message.Partition] = message.Offset
				c.Commit(message)

				break
			}

			offsets[message.Topic][message.Partition] = message.Offset

		case err = <-c.cg.Errors():
			Log.Error("kafka consumer error:%+v", err)

		case note = <-c.cg.Notifications():
			Log.Info("kafka consumer Rebalanced: %+v", note)

		case <-c.done:
			if err := c.cg.Close(); err != nil {
				Log.Warn("Error closing the consumer:%v", err)
			}

			return
		}
	}
}

func (c *consumer) Commit(message *sarama.ConsumerMessage) {
	c.cg.MarkOffset(message, "")
	// 	Log.Debug("Consuming message {%v-%v-%v}, commit over!",
	// 		message.Topic, gxstrings.String(message.Key), gxstrings.String(message.Value))
}

func (c *consumer) Stop() {
	close(c.done)
	c.wg.Wait()
}
