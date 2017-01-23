// Copyright 2016 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by a BSD-style license.

// Package gxkafka encapsulates some kafka functions based on github.com/Shopify/sarama.
// MOD : 2016-06-01 18:00
package gxkafka

import (
	"log"
	// "fmt"
)

import (
	// "github.com/Shopify/sarama"
	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kazoo-go"
)

// type ProducerError struct {
// 	Msg *ProducerMessage
// 	Err error
// }
type (
	// Consumer will invoke @ProduceMessageCallback when got message
	ConsumerMessageCallback func(*sarama.ConsumerMessage) error
	// AsyncProducer will invoke @ProduceMessageCallback when got sucess message response.
	ProducerMessageCallback func(*sarama.ProducerMessage)
	// AsyncProducer will invoke @ProduceErrorCallback when got error message response
	ProducerErrorCallback func(*sarama.ProducerError)

	empty struct{}
)

func GetBrokerList(zkHosts string) ([]string, error) {
	var (
		config  = kazoo.NewConfig()
		zkNodes []string
	)

	// fmt.Println("zkHosts:", zkHosts)
	zkNodes, config.Chroot = kazoo.ParseConnectionString(zkHosts)
	kz, err := kazoo.NewKazoo(zkNodes, config)
	if err != nil {
		log.Fatal("[ERROR] Failed to connect to the zookeeper cluster:", err)
		return nil, err
	}
	defer kz.Close()

	brokerList, err := kz.BrokerList()
	// fmt.Printf("broker list:%#v\n", brokerList)
	if err != nil {
		log.Fatal("[ERROR] Failed to retrieve Kafka broker list from zookeeper:", err)
		return nil, err
	}

	return brokerList, nil
}
