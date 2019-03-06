package jsoncombiner

import (
	"fmt"
	"log"

	"github.com/timescale/outflux/internal/idrf"
	"github.com/timescale/outflux/internal/utils"
)

// Transformer defines a Transformer that combines specified columns into one json column
type Transformer struct {
	id                 string
	columnsToCombine   map[string]bool
	resultColumn       string
	bufferSize         uint16
	cachedInputBundle  *idrf.Bundle
	cachedOutputBundle *idrf.Bundle
	combinedIndexes    map[int]string
	validator          validator
	colColmbiner       columnCombiner
	jsonCreator        jsonCreator
}

// NewTransformer returns a new instance of a transformer that combines multiple columns
// into one JSON column
func NewTransformer(id string, columnsToCombine []string, resultColumn string) (*Transformer, error) {
	if columnsToCombine == nil || len(columnsToCombine) == 0 {
		return nil, fmt.Errorf("at least one column must be selected for combination")
	}

	if resultColumn == "" {
		return nil, fmt.Errorf("result column can't be an empty string")
	}

	columnsSet := make(map[string]bool)
	for _, colName := range columnsToCombine {
		columnsSet[colName] = true
	}

	return &Transformer{
		id: id, columnsToCombine: columnsSet, resultColumn: resultColumn,
		validator: &defValidator{id: id}, colColmbiner: &defColCombiner{},
	}, nil
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

	c.cacheItems(input, newDataSet)
	return c.cachedOutputBundle, nil
}

// Start consumes the data channel sent as an argument in Prepare
// for each row in the channel it combines some columns as a single JSON column
// and feeds the transformed row to the channel returned in Prepare
func (c *Transformer) Start(errChan chan error) error {
	if c.cachedInputBundle == nil || c.cachedOutputBundle == nil {
		return fmt.Errorf("%s: Prepare must be called before Start", c.id)
	}

	defer close(c.cachedOutputBundle.DataChan)
	log.Printf("%s: starting transformation", c.id)
	if err := utils.CheckError(errChan); err != nil {
		log.Printf("%s: error received from outside, aborting:%v", c.id, err)
		return nil
	}

	inputData := c.cachedInputBundle.DataChan
	outputChannel := c.cachedOutputBundle.DataChan
	for row := range inputData {
		transformed, err := c.transformRow(row)
		if err != nil {
			return err
		}
		outputChannel <- transformed
	}

	return nil
}

func (c *Transformer) cacheItems(input *idrf.Bundle, output *idrf.DataSet) {
	dataDef := input.DataDef
	c.cachedInputBundle = input
	combinedColumnIndexes := make(map[int]string)

	for i, col := range dataDef.Columns {
		_, isCombined := c.columnsToCombine[col.Name]
		if isCombined {
			combinedColumnIndexes[i] = col.Name
		}
	}

	c.combinedIndexes = combinedColumnIndexes

	c.cachedOutputBundle = &idrf.Bundle{
		DataDef:  output,
		DataChan: make(chan idrf.Row, c.bufferSize),
	}

	c.jsonCreator = &defCreator{
		colsToCombine: combinedColumnIndexes,
	}
}

func (c *Transformer) transformRow(row idrf.Row) (idrf.Row, error) {
	jsonVal, err := c.jsonCreator.toJSON(row)
	if err != nil {
		return nil, fmt.Errorf("%s: could not combine some of the columns into JSON\n%v", c.id, err)
	}

	newRow := make([]interface{}, len(c.cachedOutputBundle.DataDef.Columns))
	currentCol := 0
	jsonAdded := false
	for i, val := range row {
		_, isCombined := c.combinedIndexes[i]
		if !jsonAdded && isCombined {
			newRow[currentCol] = jsonVal
			jsonAdded = true
		} else if jsonAdded && isCombined {
			continue
		} else {
			newRow[currentCol] = val
		}

		currentCol++
	}

	return newRow, nil
}
