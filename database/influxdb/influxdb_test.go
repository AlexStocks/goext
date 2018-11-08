package gxinfluxdb

import (
	"testing"
	"time"
)

import (
	client "github.com/influxdata/influxdb/client/v2"
	jerrors "github.com/juju/errors"
)

func TestGxInfluxDBClient(t *testing.T) {
	// Create a new HTTPClient
	db := "test_db"
	c, err := NewInfluxDBClient("http://localhost:18086", "", "", db, "s")
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
	tableSize, err := c.TableSize(db, table)
	if err != nil {
		t.Fatal(jerrors.ErrorStack(err))
	}
	t.Log(table, " size:", tableSize)

	// Use inner batchpoints
	tags = map[string]string{"cpu": "cpu-free"}
	fields = map[string]interface{}{
		"idle":   90.0,
		"system": 33.0,
		"user":   2,
	}
	err = c.AddPoint(table, tags, fields, time.Now())
	if err != nil {
		t.Fatal(jerrors.ErrorStack(err))
	}
	size, err := c.Flush()
	if err != nil {
		t.Fatal(jerrors.ErrorStack(err))
	}
	if size != 1 {
		t.Errorf("size:%d != 1", size)
	}
	if len(c.bp.points) != 0 {
		t.Errorf("len(c.bp.points):%d != 0", c.bp.points)
	}
}
