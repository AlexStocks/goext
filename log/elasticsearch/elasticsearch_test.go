package gxelasticsearch

import (
	"testing"
)

type EsConf struct {
	ShardNum        int32
	ReplicaNum      int32
	RefreshInterval int32
	EsHosts         []string

	PushIndex string
	PushType  string
}

type Doc struct {
	Subject string `json:"subject"`
	Message string `json:"message"`
}

func TestEsClient(t *testing.T) {
	var (
		err    error
		esConf EsConf
		client EsClient
		doc    Doc
		key    string
	)

	esConf = EsConf{
		ShardNum:        5,
		ReplicaNum:      0,
		RefreshInterval: 1,
		EsHosts: []string{
			// "http://10.116.27.4:5858",
			// "http://10.66.130.221:5858",
			// "http://10.66.130.228:5858",
			"http://119.81.218.90:5858",
		},
		PushIndex: "dokidoki-push-test",
		PushType:  "gopush",
	}

	// Create a client
	client, err = CreateEsClient(esConf.EsHosts)
	if err != nil {
		t.Fatal(err)
	}

	// Create an index
	err = client.CreateEsIndex(esConf.PushIndex, esConf.ShardNum, esConf.ReplicaNum, esConf.RefreshInterval)
	if err != nil {
		t.Fatal(err)
	}

	// Add some documents
	doc = Doc{
		Subject: "Invitation",
		Message: "Would you like to visit me in Berlin in October?",
	}
	err = client.Insert(esConf.PushIndex, esConf.PushType, doc)
	if err != nil {
		t.Fatal(err)
	}

	doc = Doc{
		Subject: "doc-subject1",
		Message: "doc-msg1",
	}
	key = doc.Subject
	err = client.InsertWithDocId(esConf.PushIndex, esConf.PushType, key, doc)
	if err != nil {
		t.Fatal(err)
	}

	// 此处由于key不变，这个doc值会覆盖上面的doc值
	doc = Doc{
		Subject: "doc-subject2",
		Message: "doc-msg2",
	}
	err = client.InsertWithDocId(esConf.PushIndex, esConf.PushType, key, doc)
	if err != nil {
		t.Fatal(err)
	}

	// Delete the index again
	// time.Sleep(30e9)
	// err = client.DeleteEsIndex(esConf.PushIndex)
	// if err != nil {
	//     t.Fatal(err)
	// }
}
