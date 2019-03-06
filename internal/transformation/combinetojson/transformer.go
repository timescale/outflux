package combinetojson

import (
	"fmt"

	"github.com/timescale/outflux/internal/idrf"
)

// Transformer defines a Transformer that combines specified columns into one json column
type Transformer struct {
	id                string
	columnsToCombine  map[string]bool
	resultColumn      string
	bufferSize        uint16
	cachedInputBundle *idrf.Bundle
	validator         validator
	colColmbiner      columnCombiner
}

// NewTransformer returns a new instance of a transformer that combines multiple columns
// into one JSON column
func NewTransformer(id string, columnsToCombine []string, resultColumn string) *Transformer {
	columnsSet := make(map[string]bool)
	for _, colName := range columnsToCombine {
		columnsSet[colName] = true
	}

	return &Transformer{
		id: id, columnsToCombine: columnsSet, resultColumn: resultColumn,
		validator: &defValidator{id: id}, colColmbiner: &defColCombiner{},
	}
}

// ID returns a string that identifies the transformer instance
func (c *Transformer) ID() string {
	return c.id
}

// Prepare verifies that the transformation can be executed, creates the output channel
// and the transformed data set definition and returns them as a idrf.Bundle
func (c *Transformer) Prepare(input *idrf.Bundle) (*idrf.Bundle, error) {
	originDataSet := input.DataDef

	validationErr := c.validator.validate(originDataSet, c.resultColumn, c.columnsToCombine)
	if validationErr != nil {
		return nil, validationErr
	}

	newColumns := c.colColmbiner.combine(originDataSet.Columns, c.columnsToCombine, c.resultColumn)
	newDataSet, err := idrf.NewDataSet(originDataSet.DataSetName, newColumns, originDataSet.TimeColumn)
	if err != nil {
		return nil, fmt.Errorf("%s: could not generate the transformed data set definition.\nProblem was:%v", c.id, err)
	}

	c.cachedInputBundle = input
	return &idrf.Bundle{
		DataDef:  newDataSet,
		DataChan: make(chan idrf.Row, c.bufferSize),
	}, nil
}

// Start consumes the data channel sent as an argument in Prepare
// for each row in the channel it combines some columns as a single JSON column
// and feeds the transformed row to the channel returned in Prepare
func (c *Transformer) Start(errChanchan error) error {
	return nil
}
