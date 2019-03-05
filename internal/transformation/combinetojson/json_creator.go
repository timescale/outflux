package combinetojson

import (
	"encoding/json"

	"github.com/timescale/outflux/internal/idrf"
)

type jsonCreator interface {
	toJSON(row idrf.Row) ([]byte, error)
}

type defCreator struct {
	colsToCombine map[int]string
}

func (d *defCreator) toJSON(row idrf.Row) ([]byte, error) {
	data := make(map[string]interface{})
	for colInd, colName := range d.colsToCombine {
		val := row[colInd]
		data[colName] = val
	}

	return json.Marshal(data)
}
