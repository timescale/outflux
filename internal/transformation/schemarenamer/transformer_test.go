package schemarenamer

import (
	"testing"

	"github.com/timescale/outflux/internal/idrf"
)

func TestNewTransformer(t *testing.T) {
	trans := NewTransformer("id", "output")
	if trans == nil {
		t.Fatal("could not create transformer")
	} else if trans.id != "id" || trans.outputSchema != "output" || trans.prepareCalled {
		t.Fatalf("transformer should have id: id, outputSchema: output and prepareCalled: false\ngot: %v", trans)
	}
}

func TestPrepare(t *testing.T) {
	testCases := []struct {
		inName       string
		newSchema    string
		expectedName string
	}{
		{inName: "table", newSchema: "schema", expectedName: "schema.table"},
		{inName: "schema.table", newSchema: "schema", expectedName: "schema.table"},
		{inName: "schema1.table", newSchema: "schema", expectedName: "schema.table"},
		{inName: "one.two.three", newSchema: "four", expectedName: "four.two.three"},
		{inName: "schema.table+smt", newSchema: "schema2", expectedName: "schema2.table+smt"},
	}

	timeCol := "time"
	columns := []*idrf.Column{
		&idrf.Column{Name: timeCol, DataType: idrf.IDRFTimestamp},
		&idrf.Column{Name: "col", DataType: idrf.IDRFBoolean},
	}
	for _, testCase := range testCases {
		ds, _ := idrf.NewDataSet(testCase.inName, columns, timeCol)
		trans := NewTransformer("id", testCase.newSchema)
		inDataChan := make(chan idrf.Row, 1)
		resBundle, err := trans.Prepare(&idrf.Bundle{DataChan: inDataChan, DataDef: ds})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !trans.prepareCalled {
			t.Fatalf("expected prepareCalled to be true, wasn't")
		}

		if resBundle.DataDef.DataSetName != testCase.expectedName {
			t.Fatalf("expected: %s, got: %s", testCase.expectedName, resBundle.DataDef.DataSetName)
		}

		// channel should be the same, msg sent to inDataChan
		// should come out of resBundle.DataChan
		inDataChan <- []interface{}{"1"}
		got := <-resBundle.DataChan
		if got[0].(string) != "1" {
			t.Fatalf("unexpected data received. expected '1', got: %v", got[0])
		}
	}
}

func TestStart(t *testing.T) {
	trans := NewTransformer("id", "schema")
	if trans.Start(nil) == nil {
		t.Fatal("prepare not called. expected error, none got")
	}

	timeCol := "time"
	columns := []*idrf.Column{
		&idrf.Column{Name: timeCol, DataType: idrf.IDRFTimestamp},
		&idrf.Column{Name: "col", DataType: idrf.IDRFBoolean},
	}
	ds, _ := idrf.NewDataSet("ds", columns, timeCol)
	inBundle := &idrf.Bundle{
		DataChan: make(chan idrf.Row, 1),
		DataDef:  ds,
	}
	outBundle, err := trans.Prepare(inBundle)
	if err != nil {
		t.Fatalf("unexpected error on prepare: %v", err)
	}
	err = trans.Start(make(chan error))
	if err != nil {
		t.Fatalf("unexpected error on Start: %v", err)
	}

	// will close output bundle channel
	// no data should have been sent to it
	close(outBundle.DataChan)
	for range outBundle.DataChan {
		t.Fatalf("unexpected data received on channel")
	}
}
