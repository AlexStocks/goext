package gxinfluxdb

import (
	"testing"
	"time"
)

import (
	client "github.com/influxdata/influxdb/client/v2"
)

func TestGxInfluxDBClient(t *testing.T) {
	// Create a new HTTPClient
	c, err := NewInfluxDBClient("http://localhost:18086", "", "")
	if err != nil {
		t.Fatal(err)
	}

	// Close client resources
	defer func() {
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	// drop db
	db := "test_db"
	err = c.DropDB(db)
	if err != nil {
		t.Fatal(err)
	}
	// drop user
	username := "alex"
	err = c.DropAdmin(username)
	if err != nil {
		t.Fatal(err)
	}

	// ping server
	err = c.Ping()
	if err != nil {
		t.Fatal(err)
	}

	// create database
	err = c.CreateDB(db)
	if err != nil {
		t.Fatal(err)
	}
	// get dbs
	dbs, err := c.GetDBList()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("databases:%#v", dbs)

	// get tables
	tables, err := c.GetTableList("_internal")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("tables:%#v", tables)

	// create admin
	err = c.CreateAdmin(username, "stocks")
	if err != nil {
		t.Fatal(err)
	}
	// get users
	users, err := c.GetUserList()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("users:%#v", users)

	// Create a new point batch
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  db,
		Precision: "ns",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Create a point and add to batch
	tags := map[string]string{"cpu": "cpu-total"}
	fields := map[string]interface{}{
		"idle":   10.1,
		"system": 53.3,
		"user":   1,
	}

	table := "test_table"
	pt, err := client.NewPoint(table, tags, fields, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	bp.AddPoint(pt)

	// Write the batch
	if err := c.Write(bp); err != nil {
		t.Fatal(err)
	}

	// Write single record
	record := map[string]interface{}{
		"idle":      20.1,
		"system":    43.3,
		"user":      6,
		"timestamp": time.Now().UnixNano(),
	}
	rspData, err := c.Send("test_db", record)
	t.Logf("rspData:%s, err:%#v", string(rspData), err)

	tableSize, err := c.TableSize(db, table)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(table, " size:", tableSize)
}
