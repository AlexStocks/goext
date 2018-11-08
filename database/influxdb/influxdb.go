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
	"time"
)

import (
	log "github.com/AlexStocks/log4go"
	"github.com/influxdata/influxdb/client/v2"
	jerrors "github.com/juju/errors"
)

/////////////////////////////////////////////////
// BatchPoints
/////////////////////////////////////////////////

type BatchPoints struct {
	points           []*client.Point
	database         string
	precision        string
	retentionPolicy  string
	writeConsistency string
}

func NewBatchPoints(db, precision string) *BatchPoints {
	return &BatchPoints{
		points:    make([]*client.Point, 0, 4096),
		database:  db,
		precision: precision,
	}
}

func (bp *BatchPoints) Size() int {
	return len(bp.points)
}

func (bp *BatchPoints) Clear() {
	bp.points = bp.points[:0]
}

func (bp *BatchPoints) AddPoint(p *client.Point) {
	bp.points = append(bp.points, p)
}

func (bp *BatchPoints) AddPoints(ps []*client.Point) {
	bp.points = append(bp.points, ps...)
}

func (bp *BatchPoints) Points() []*client.Point {
	return bp.points
}

func (bp *BatchPoints) Precision() string {
	return bp.precision
}

func (bp *BatchPoints) Database() string {
	return bp.database
}

func (bp *BatchPoints) WriteConsistency() string {
	return bp.writeConsistency
}

func (bp *BatchPoints) RetentionPolicy() string {
	return bp.retentionPolicy
}

func (bp *BatchPoints) SetPrecision(p string) error {
	if _, err := time.ParseDuration("1" + p); err != nil {
		return err
	}
	bp.precision = p
	return nil
}

func (bp *BatchPoints) SetDatabase(db string) {
	bp.database = db
}

func (bp *BatchPoints) SetWriteConsistency(wc string) {
	bp.writeConsistency = wc
}

func (bp *BatchPoints) SetRetentionPolicy(rp string) {
	bp.retentionPolicy = rp
}

/////////////////////////////////////////////////
// InfxluxDB Client
/////////////////////////////////////////////////

type InfluxDBClient struct {
	client.Client
	bp *BatchPoints
}

func NewInfluxDBClient(host, user, password, db, precision string) (InfluxDBClient, error) {
	// Create a new HTTPClient
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     host,
		Username: user,
		Password: password,
	})
	bp := NewBatchPoints(db, precision)

	return InfluxDBClient{Client: c, bp: bp}, jerrors.Trace(err)
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
func (c InfluxDBClient) SendLines(host, database string, raw_data []byte) ([]byte, error) {
	// uri := "http://" + Host + "/write?db=" + database
	// http://127.0.0.1:8080/write?db=xxx
	uri := (&url.URL{
		Scheme:   "http",
		Host:     host,
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

func (c InfluxDBClient) AddPoint(pt *client.Point) {
	c.bp.AddPoint(pt)
}

func (c InfluxDBClient) AddPointData(table string, tags map[string]string, fields map[string]interface{}, t time.Time) error {
	pt, err := client.NewPoint(table, tags, fields, t)
	if err != nil {
		return jerrors.Trace(err)
	}

	c.bp.AddPoint(pt)

	return nil
}

// return current point number
func (c InfluxDBClient) Size() int {
	return c.bp.Size()
}

func (c InfluxDBClient) Flush() (int, error) {
	size := c.bp.Size()
	err := c.Client.Write(c.bp)
	if err != nil {
		log.Error("failed to write BatchPoints[0]:%#v to database %s", c.bp.points[0], c.bp.database)
	}
	c.bp.Clear()

	return size, jerrors.Trace(err)
}
