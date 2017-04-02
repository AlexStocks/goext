// Copyright 2017 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by a BSD-style license.

// 2017-04-02 02:04
// package gxelasticsearch provides a Elasticsearch driver

package gxelasticsearch

import (
	"context"
	"fmt"
)

import (
	"github.com/pkg/errors"
	es "gopkg.in/olivere/elastic.v3"
)

type EsClient struct {
	*es.Client
}

func CreateEsClient(hosts []string) (EsClient, error) {
	var esClient EsClient
	// Create a client
	client, err := es.NewClient(es.SetURL(hosts[:]...))
	if err == nil {
		esClient.Client = client
	}

	return esClient, err
}

// https://github.com/olivere/elastic/issues/457
func buildEsIndexSettings(shardNum int32, replicaNum int32, refreshInterval int32) string {
	return fmt.Sprintf(`{
		"settings" : {
			"number_of_shards": %d,
			"number_of_replicas": %d,
			"refresh_interval": "%ds"
		}
	}`, shardNum, replicaNum, refreshInterval)
}

func (ec EsClient) CreateEsIndex(index string, shardNum int32, replicaNum int32, refreshInterval int32) error {
	var (
		err    error
		exists bool
		body   string
		ctx    context.Context
	)

	ctx = context.Background()
	exists, err = ec.IndexExists(index).DoC(ctx)
	if err != nil {
		return errors.Wrapf(err, "CreateRcIndex(index:%s, shardNum:%s, replicaNum:%d, refreshInterval:%d)",
			index, shardNum, replicaNum, refreshInterval)
	}
	if exists {
		return nil
	}

	body = buildEsIndexSettings(shardNum, replicaNum, refreshInterval)
	_, err = ec.CreateIndex(index).BodyString(body).DoC(ctx)
	if err != nil {
		return errors.Wrapf(err, "CreateEsIndex(body:%s)", body)
	}

	return nil
}

func (ec EsClient) DeleteEsIndex(index string) error {
	var (
		err error
		ctx context.Context
	)

	ctx = context.Background()
	_, err = ec.DeleteIndex(index).DoC(ctx)
	if err != nil {
		return errors.Wrapf(err, "DeleteEsIndex(index:%s)", index)
	}

	return nil
}

// InsertWithDocId 插入@msg
// !!! 如果@msg的类型是string 或者 []byte，则被当做Json String类型直接存进去
func (ec EsClient) Insert(index string, typ string, msg interface{}) error {
	var (
		err      error
		ok       bool
		msgBytes []byte
		ctx      context.Context
	)

	// https://github.com/olivere/elastic/issues/127
	// Elasticsearch can create an identifier for you, automatically.
	// _, err = ec.Index().Index(index).Type(typ).Id(1).BodyJson(msg).Do()
	switch msg.(type) {
	case string:
		_, err = ec.Index().Index(index).Type(typ).BodyString(msg.(string)).DoC(ctx)
		if err != nil {
			return errors.Wrapf(err, "Insert(index:%s, type:%s, msg:%s)", index, typ, msg)
		}

	default:
		if msgBytes, ok = msg.([]byte); ok {
			_, err = ec.Index().Index(index).Type(typ).BodyString(string(msgBytes)).DoC(ctx)
			if err != nil {
				return errors.Wrapf(err, "Insert(index:%s, type:%s, msg:%s)", index, typ, (string)(msgBytes))
			}
		} else {
			_, err = ec.Index().Index(index).Type(typ).BodyJson(msg).DoC(ctx)
			if err != nil {
				return errors.Wrapf(err, "Insert(index:%s, type:%s, msg:%#v)", index, typ, msg)
			}
		}
	}

	return nil
}

// Search
// Waiting for es5.x's future sql feature

// InsertWithDocId 插入@msg时候指定@docID
// !!! 如果@msg的类型是string 或者 []byte，则被当做Json String类型直接存进去
func (ec EsClient) InsertWithDocId(index string, typ string, docID string, msg interface{}) error {
	var (
		err      error
		ok       bool
		msgBytes []byte
		ctx      context.Context
	)

	switch msg.(type) {
	case string:
		_, err = ec.Index().Index(index).Type(typ).Id(docID).BodyString(msg.(string)).DoC(ctx)
		if err != nil {
			return errors.Wrapf(err, "InsertWithDocId(index:%s, type:%s, docID:%s, msg:%s)", index, typ, docID, msg)
		}

	default:
		if msgBytes, ok = msg.([]byte); ok {
			_, err = ec.Index().Index(index).Type(typ).Id(docID).BodyString(string(msgBytes)).DoC(ctx)
			if err != nil {
				return errors.Wrapf(err, "InsertWithDocId(index:%s, type:%s, docID:%s, msg:%s)", index, typ, docID, (string)(msgBytes))
			}
		} else {
			_, err = ec.Index().Index(index).Type(typ).Id(docID).BodyJson(msg).DoC(ctx)
			if err != nil {
				return errors.Wrapf(err, "InsertWithDocId(index:%s, type:%s, docID:%s, msg:%#v)", index, typ, docID, msg)
			}
		}
	}

	return nil
}
