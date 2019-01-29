package pipeline

import (
	"fmt"

	"github.com/timescale/outflux/extraction"
	"github.com/timescale/outflux/ingestion"

	extractionConfig "github.com/timescale/outflux/extraction/config"
	ingestionConfig "github.com/timescale/outflux/ingestion/config"
	"github.com/timescale/outflux/utils"
)

// ExecutionPipeline combines an extractor with a ingestor
type ExecutionPipeline struct {
	ID               string
	Config           *OutfluxConfig
	ErrorBroadcaster utils.ErrorBroadcaster
}

// Start the extractor and ingestor and wait for them to complete
func (pipe *ExecutionPipeline) Start() error {
	errorChannel, err := pipe.ErrorBroadcaster.Subscribe(pipe.ID)
	if err != nil {
		return fmt.Errorf("could not subscribe for errors\n%v", err)
	}

	defer pipe.ErrorBroadcaster.Close()
	extractor, err := extraction.NewExtractor(pipe.Config.ExtractionConfig)
	if err != nil {
		return fmt.Errorf("could not create the extractor\n%v", err)
	}

	extractionInfo, err := extractor.Start(pipe.ErrorBroadcaster)
	if err != nil {
		return fmt.Errorf("could not start the extractor\n%s", err.Error())
	}

	ingestor := ingestion.NewIngestor(pipe.Config.IngestionConfig, extractionInfo)

	ackChannel, err := ingestor.Start(pipe.ErrorBroadcaster)
	if err != nil {
		pipe.ErrorBroadcaster.Broadcast(pipe.ID, err)
		return fmt.Errorf("could not start the ingestor\n%s", err.Error())
	}

	ingestorProperlyEnded := false
	for range ackChannel {
		ingestorProperlyEnded = true
	}

	for err := range errorChannel {
		return fmt.Errorf("error in the execution pipleine %s\n%v", pipe.ID, err)
	}

	if ingestorProperlyEnded {
		return nil
	}

	return fmt.Errorf("no error received from extractor and ingestor, but ingestor didn't end properly")

}

// OutfluxConfig contains all the requirements to instantiate an extractor and ingestor
type OutfluxConfig struct {
	IngestionConfig  *ingestionConfig.Config
	ExtractionConfig *extractionConfig.Config
}
