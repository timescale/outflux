package jsoncombiner

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/timescale/outflux/internal/idrf"
)

func TestCacheItems(t *testing.T) {
	cols := []*idrf.Column{&idrf.Column{Name: "col1", DataType: idrf.IDRFTimestamp},
		&idrf.Column{Name: "col2", DataType: idrf.IDRFBoolean},
		&idrf.Column{Name: "col3", DataType: idrf.IDRFBoolean},
	}
	columnsToCombine := map[string]bool{"col2": true, "col3": true}
	inputDs, _ := idrf.NewDataSet("ds", cols, cols[0].Name)
	outputDs := &idrf.DataSet{DataSetName: "outds"}
	inputBundle := &idrf.Bundle{DataDef: inputDs}

	transformer := &Transformer{columnsToCombine: columnsToCombine}

	transformer.cacheItems(inputBundle, outputDs)

	if transformer.cachedInputBundle == nil || transformer.cachedInputBundle.DataDef.DataSetName != inputDs.DataSetName {
		t.Error("input bundle not cached")
	} else if transformer.cachedOutputBundle == nil || transformer.cachedOutputBundle.DataDef.DataSetName != outputDs.DataSetName {
		t.Error("ouptut data set not cached")
	} else if transformer.cachedOutputBundle.DataChan == nil {
		t.Error("output data channel not created")
	} else if transformer.jsonCreator == nil {
		t.Error("jsonCreator not created")
	} else if transformer.combinedIndexes == nil || len(transformer.combinedIndexes) != 2 {
		t.Error("combined column indexes not cached")
	} else if transformer.combinedIndexes[1] != "col2" || transformer.combinedIndexes[2] != "col3" {
		t.Errorf("combined column indexes not properly cached")
	}
}
func TestPrepare(t *testing.T) {
	colsBefore := []*idrf.Column{
		&idrf.Column{Name: "col1", DataType: idrf.IDRFTimestamp},
		&idrf.Column{Name: "col2", DataType: idrf.IDRFBoolean},
		&idrf.Column{Name: "col3", DataType: idrf.IDRFInteger32},
	}
	originDs, _ := idrf.NewDataSet("ds", colsBefore, colsBefore[0].Name)
	cols := []*idrf.Column{colsBefore[0], &idrf.Column{Name: "col2", DataType: idrf.IDRFJson}}
	testCases := []struct {
		desc      string
		ds        *idrf.DataSet
		toCombine map[string]bool
		res       string
		expectErr bool
		val       validator
		comb      columnCombiner
	}{
		{
			desc:      "invalid transformation",
			expectErr: true,
			val:       &mock{valErr: fmt.Errorf("error")},
		}, {
			desc:      "bad origin data set = error from NewDataSet",
			expectErr: true,
			ds:        &idrf.DataSet{},
			val:       &mock{},
			comb:      &mock{combRes: cols},
		}, {
			desc:      "all good",
			res:       "col2",
			toCombine: map[string]bool{},
			ds:        originDs,
			val:       &mock{},
			comb:      &mock{combRes: cols},
		},
	}

	for _, tc := range testCases {
		trans := &Transformer{
			columnsToCombine: tc.toCombine,
			resultColumn:     tc.res,
			validator:        tc.val,
			colColmbiner:     tc.comb,
		}

		in := &idrf.Bundle{
			DataDef: tc.ds,
		}

		bund, err := trans.Prepare(in)

		if tc.expectErr && err == nil {
			t.Errorf("test: %s\nexpected error, none got", tc.desc)
		} else if !tc.expectErr && err != nil {
			t.Errorf("test: %s\nunexpected error: %v", tc.desc, err)
		}

		if tc.expectErr {
			continue
		}

		if trans.cachedInputBundle == nil || trans.cachedOutputBundle == nil || trans.combinedIndexes == nil {
			t.Errorf("test: %s\required data wasn't cached", tc.desc)
		} else if bund.DataChan == nil {
			t.Errorf("test: %s\noutput data channel not created", tc.desc)
		} else if bund.DataDef.DataSetName != in.DataDef.DataSetName || bund.DataDef.TimeColumn != in.DataDef.TimeColumn {
			t.Errorf("test: %s\noutput data set, did not match expectations", tc.desc)
		}
	}
}

func TestId(t *testing.T) {
	trans := &Transformer{id: "id"}
	if trans.ID() != "id" {
		t.Error("unexpected value")
	}
}

func TestNewTransformer(t *testing.T) {
	testCases := []struct {
		desc      string
		cols      []string
		res       string
		expectErr bool
	}{
		{desc: "nil cols not allowed", res: "res", expectErr: true},
		{desc: "empty cols not allowed", res: "res", cols: []string{}, expectErr: true},
		{desc: "empty res col not allowed", cols: []string{"col1"}, expectErr: true},
		{desc: "all good", res: "res", cols: []string{"col1"}},
	}

	for _, tc := range testCases {
		trans, err := NewTransformer("id", tc.cols, tc.res)
		if err != nil && !tc.expectErr {
			t.Errorf("test:%s\nunexpected err: %v", tc.desc, err)
		} else if err == nil && tc.expectErr {
			t.Errorf("test: %s\nexpected error, none received", tc.desc)
		}

		if tc.expectErr {
			continue
		}

		if trans.id != "id" || len(trans.columnsToCombine) != 1 || trans.resultColumn != tc.res {
			t.Errorf("test: %s\ntransformer not properly initialized", tc.desc)
		} else if trans.columnsToCombine["col1"] != true {
			t.Errorf("test: %s\ntransformer not properly initialized", tc.desc)
		}
	}
}

func TestTransformerStart(t *testing.T) {
	trans := &Transformer{}
	if trans.Start(nil) == nil {
		t.Error("transformer should fail if bundles aren't cached")
	}

	// exit early because of external error
	errChan := make(chan error, 1)
	errChan <- fmt.Errorf("enqueue one external error")
	outData := make(chan idrf.Row)
	trans = &Transformer{
		cachedInputBundle:  &idrf.Bundle{},
		cachedOutputBundle: &idrf.Bundle{DataChan: outData},
	}

	if err := trans.Start(errChan); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// exit properly when input data channel is empty
	outData = make(chan idrf.Row)
	inData := make(chan idrf.Row)
	errChan = make(chan error)
	close(inData)
	trans = &Transformer{
		cachedInputBundle:  &idrf.Bundle{DataChan: inData},
		cachedOutputBundle: &idrf.Bundle{DataChan: outData},
	}
	if err := trans.Start(errChan); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	for _ = range outData {
		t.Errorf("no output should have been produced")
	}

	//input row should be transformed
	outData = make(chan idrf.Row, 1)
	inData = make(chan idrf.Row, 1)
	errChan = make(chan error)
	inCols := []*idrf.Column{
		&idrf.Column{Name: "col1", DataType: idrf.IDRFTimestamp},
		&idrf.Column{Name: "col2", DataType: idrf.IDRFBoolean},
	}
	outCols := []*idrf.Column{
		inCols[0], &idrf.Column{Name: "col2", DataType: idrf.IDRFJson},
	}
	inDataDef, _ := idrf.NewDataSet("ds", inCols, inCols[0].Name)
	outDataDef, _ := idrf.NewDataSet("ds", outCols, outCols[0].Name)
	trans = &Transformer{
		jsonCreator:        &mockCreator{},
		combinedIndexes:    map[int]string{1: "col2"},
		cachedOutputBundle: &idrf.Bundle{DataDef: outDataDef, DataChan: outData},
		cachedInputBundle:  &idrf.Bundle{DataDef: inDataDef, DataChan: inData},
	}
	inRow := []interface{}{"1", true}
	inData <- inRow
	close(inData)
	if err := trans.Start(errChan); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	numRows := 0
	for transformed := range outData {
		numRows++
		if transformed[0] != "1" || transformed[1] == nil {
			t.Errorf("expected values: 1, nil\ngot: %v, %v", transformed[0], transformed[1])
		}
	}

	if numRows != 1 {
		t.Error("expected exactly one row to be produced")
	}

}
func TestRowTransform(t *testing.T) {
	// error on crating json
	creator := &mockCreator{err: fmt.Errorf("error")}
	trans := &Transformer{jsonCreator: creator}
	_, err := trans.transformRow(nil)
	if err == nil {
		t.Errorf("expected error to be returned when json creator returns error")
	}

	// good conversion
	cols := []*idrf.Column{
		&idrf.Column{Name: "col1", DataType: idrf.IDRFTimestamp},
		&idrf.Column{Name: "res", DataType: idrf.IDRFJson},
	}

	outDs, _ := idrf.NewDataSet("ds", cols, "col1")
	combinedIndexes := map[int]string{1: "col2"}

	jsonBytes, _ := json.Marshal(map[string]string{"col2": "val"})
	trans = &Transformer{
		jsonCreator:        &mockCreator{res: jsonBytes},
		cachedOutputBundle: &idrf.Bundle{DataDef: outDs},
		combinedIndexes:    combinedIndexes,
	}

	exampleRow := []interface{}{"1", "val"}
	transformed, err := trans.transformRow(exampleRow)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	} else if transformed[0] != "1" || string(transformed[1].([]byte)) != "{\"col2\":\"val\"}" {
		t.Errorf("unexpected values: %v", transformed)
	}
}

type mock struct {
	valErr  error
	combRes []*idrf.Column
}

func (m *mock) validate(originData *idrf.DataSet, resCol string, columnsToCombine map[string]bool) error {
	return m.valErr
}

func (m *mock) combine(columns []*idrf.Column, toCombine map[string]bool, result string) []*idrf.Column {
	return m.combRes
}

type mockCreator struct {
	err error
	res []byte
}

func (m *mockCreator) toJSON(row idrf.Row) ([]byte, error) {
	return m.res, m.err
}
