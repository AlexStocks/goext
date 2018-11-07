// Copyright 2016 ~ 2018 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by Apache License 2.0.

// 2018-10-23 21:46
// package gxinfluxdb provides a InfluxDB driver
package gxinfluxdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httputil"
	"net/url"
)

import (
	"github.com/influxdata/influxdb/client/v2"
	jerrors "github.com/juju/errors"
)

type InfluxDBClient struct {
	host string
	client.Client
}

func NewInfluxDBClient(host, user, password string) (InfluxDBClient, error) {
	// Create a new HTTPClient
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     host,
		Username: user,
		Password: password,
	})

	return InfluxDBClient{host: host, Client: c}, jerrors.Trace(err)
}

func (c InfluxDBClient) Close() error {
	return jerrors.Trace(c.Client.Close())
}

// queryDB convenience function to query the database
func (c InfluxDBClient) queryDB(cmd string, db string) (res []client.Result, err error) {
	q := client.Query{
		Command:  cmd,
		Database: db,
	}
	if response, err := c.Query(q); err == nil {
		if response.Error() != nil {
			return res, response.Error()
		}
		res = response.Results
	}

	return res, jerrors.Trace(err)
}

func (c InfluxDBClient) CreateDB(db string) error {
	_, err := c.queryDB(fmt.Sprintf("CREATE DATABASE %s", db), "")
	return jerrors.Trace(err)
}

func (c InfluxDBClient) DropDB(db string) error {
	_, err := c.queryDB(fmt.Sprintf("DROP DATABASE %s", db), "")
	return jerrors.Trace(err)
}

func (c InfluxDBClient) GetDBList() ([]string, error) {
	res, err := c.queryDB("SHOW DATABASES", "")
	if err != nil {
		return nil, jerrors.Trace(err)
	}

	vals := res[0].Series[0].Values
	databases := make([]string, 0, len(vals)+1)
	for _, val := range vals {
		databases = append(databases, val[0].(string))
	}

	return databases, nil
}

func (c InfluxDBClient) CreateAdmin(user, password string) error {
	_, err := c.queryDB(fmt.Sprintf("create user \"%s\" "+
		"with password '%s' with all privileges", user, password), "")
	return jerrors.Trace(err)
}

func (c InfluxDBClient) DropAdmin(user string) error {
	_, err := c.queryDB(fmt.Sprintf("DROP USER %s", user), "")
	return jerrors.Trace(err)
}

func (c InfluxDBClient) GetUserList() ([]string, error) {
	res, err := c.queryDB("SHOW USERS", "")
	if err != nil {
		return nil, jerrors.Trace(err)
	}
	vals := res[0].Series[0].Values
	users := make([]string, 0, len(vals)+1)
	for _, val := range vals {
		users = append(users, val[0].(string))
	}

	return users, nil
}

func (c InfluxDBClient) GetTableList(db string) ([]string, error) {
	res, err := c.queryDB("SHOW MEASUREMENTS", db)
	if err != nil {
		return nil, jerrors.Trace(err)
	}

	vals := res[0].Series[0].Values
	tables := make([]string, 0, len(vals)+1)
	for _, val := range vals {
		tables = append(tables, val[0].(string))
	}

	return tables, nil
}

func (c InfluxDBClient) TableSize(db, table string) (int, error) {
	count := int64(0)
	q := fmt.Sprintf("SELECT count(*) FROM %s", table)
	res, err := c.queryDB(q, db)
	if err == nil {
		count, err = res[0].Series[0].Values[0][1].(json.Number).Int64()
	}

	return int(count), jerrors.Trace(err)
}

func (c InfluxDBClient) Ping() error {
	_, _, err := c.Client.Ping(0)
	return jerrors.Trace(err)
}

// from https://github.com/opera/logpeck/blob/master/sender_influxdb.go
func (c InfluxDBClient) SendLines(database string, raw_data []byte) ([]byte, error) {
	// uri := "http://" + Host + "/write?db=" + database
	// http://127.0.0.1:8080/write?db=xxx
	uri := (&url.URL{
		Scheme:   "http",
		Host:     c.host,
		Path:     "write",
		RawQuery: "db=" + database,
	}).String()

	body := ioutil.NopCloser(bytes.NewBuffer(raw_data))
	resp, err := http.Post(uri, "application/json", body)
	if err != nil {
		return nil, jerrors.Trace(err)
	}

	rsp, _ := httputil.DumpResponse(resp, true)
	return rsp, nil
}
