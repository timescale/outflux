package influx

import (
	"fmt"
	"log"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/timescale/outflux/internal/extraction/config"
	"github.com/timescale/outflux/internal/extraction/influx/idrfconversion"
	"github.com/timescale/outflux/internal/idrf"
	"github.com/timescale/outflux/internal/schemamanagement"
)

// Extractor is an implementaiton of the extraction.Extractor interface for
// pulling data out of InfluxDB
type Extractor struct {
	Config            *config.ExtractionConfig
	SM                schemamanagement.SchemaManager
	cachedElementData *idrf.Bundle
	DataProducer      DataProducer
}

// ID of the extractor, usefull for logging and error reporting
func (e *Extractor) ID() string {
	return e.Config.ExtractorID
}

// Prepare discoveres the data set schema for the measure in the config
func (e *Extractor) Prepare() (*idrf.Bundle, error) {
	discoveredDataSet, err := e.SM.FetchDataSet(e.Config.MeasureExtraction.Measure)
	if err != nil {
		return nil, err
	}

	e.cachedElementData = &idrf.Bundle{
		DataDef: discoveredDataSet,
		Data:    make(chan idrf.Row, e.Config.DataBufferSize),
	}

	return e.cachedElementData, nil
}

// Start pulls the data from an InfluxDB measure and feeds it to a data channel
// Peridicly (between chunks) checks for external errors and quits if it detects them
func (e *Extractor) Start(errChan chan error) error {
	if e.cachedElementData == nil {
		return fmt.Errorf("%s: Prepare not called before start", e.ID())
	}

	id := e.Config.ExtractorID
	dataDef := e.cachedElementData.DataDef
	measureConf := e.Config.MeasureExtraction
	if measureConf.Limit == 0 {
		log.Printf("Skipping data transfer for measure '%s', limit set to 0", dataDef.DataSetName)
		close(e.cachedElementData.Data)
		return nil
	}

	log.Printf("Starting extractor '%s' for measure: %s\n", id, dataDef.DataSetName)
	intChunkSize := int(measureConf.ChunkSize)

	query := &influx.Query{
		Command:   buildSelectCommand(measureConf, dataDef.Columns),
		Database:  measureConf.Database,
		Chunked:   true,
		ChunkSize: intChunkSize,
	}

	log.Printf("%s: Extracting data from database '%s'\n", id, query.Database)
	log.Printf("%s: %s\n", id, query.Command)
	log.Printf("%s:Pulling chunks with size %d\n", id, intChunkSize)

	idrfConverter := idrfconversion.NewIdrfConverter(dataDef)
	producerArgs := &producerArgs{
		dataChannel: e.cachedElementData.Data,
		errChannel:  errChan,
		query:       query,
		converter:   idrfConverter,
	}

	return e.DataProducer.Fetch(producerArgs)
}
