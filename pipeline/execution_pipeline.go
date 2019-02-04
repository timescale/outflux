package pipeline

import (
	"fmt"
	"log"

	"github.com/timescale/outflux/extraction"
	"github.com/timescale/outflux/ingestion"

	extractionConfig "github.com/timescale/outflux/extraction/config"
	ingestionConfig "github.com/timescale/outflux/ingestion/config"
	"github.com/timescale/outflux/utils"
)

// ExecutionPipeline combines an extractor with a ingestor
type ExecutionPipeline interface {
	Start() error
	ID() string
}
type defaultExecutionPipeline struct {
	id     string
	config *PipelineConfig
}

func (p *defaultExecutionPipeline) ID() string {
	return p.id
}

// Start the extractor and ingestor and wait for them to complete
func (p *defaultExecutionPipeline) Start() error {
	errorBroadcaster := utils.NewErrorBroadcaster()
	errorChannel, err := errorBroadcaster.Subscribe(p.id)
	if err != nil {
		return fmt.Errorf("'%s': could not subscribe for errors\n%v", p.id, err)
	}

	defer errorBroadcaster.Close()

	extractionConf := p.config.ExtractionConfig
	extractor, err := extraction.NewExtractor(extractionConf)
	if err != nil {
		return fmt.Errorf("'%s': could not create the extractor\n%v", p.id, err)
	}

	dataChannel, err := extractor.Start(errorBroadcaster)
	if err != nil {
		return fmt.Errorf("'%s': could not start the extractor\n%v", p.id, err)
	}

	ingestor := ingestion.NewIngestor(p.config.IngestionConfig, extractionConf.DataSet, dataChannel)

	ackChannel, err := ingestor.Start(errorBroadcaster)
	if err != nil {
		errorBroadcaster.Broadcast(p.id, err)
		return fmt.Errorf("'%s': could not start the ingestor.\n%v", p.id, err)
	}

	ingestorProperlyEnded := false
	for range ackChannel {
		ingestorProperlyEnded = true
	}

	if err := utils.CheckError(errorChannel); err != nil {
		return fmt.Errorf("'%s': received error in pipeline\n%v", p.id, err)
	}

	log.Printf("Pipeline '%s' for measure '%s' completed", p.id, extractionConf.DataSet.DataSetName)
	if ingestorProperlyEnded {
		return nil
	}

	return fmt.Errorf("'%s' no error received, but ingestor didn't end properly", p.id)
}

// PipelineConfig contains all the requirements to instantiate an extractor and ingestor
type PipelineConfig struct {
	IngestionConfig  *ingestionConfig.Config
	ExtractionConfig *extractionConfig.Config
}
