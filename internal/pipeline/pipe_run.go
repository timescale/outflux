package pipeline

import (
	"fmt"
	"sync"

	"github.com/timescale/outflux/internal/transformation"

	"github.com/timescale/outflux/internal/extraction"
	"github.com/timescale/outflux/internal/ingestion"
	"github.com/timescale/outflux/internal/utils"
)

func (p *defPipe) run(
	extractor extraction.Extractor,
	ingestor ingestion.Ingestor,
	transformers []transformation.Transformer) error {
	errorBroadcaster := utils.NewErrorBroadcaster()
	ingErrors, err := errorBroadcaster.Subscribe(ingestor.ID())
	if err != nil {
		return fmt.Errorf("%s: could not subscribe ingestor for errors\n%v", p.id, err)
	}
	extErrors, err := errorBroadcaster.Subscribe(extractor.ID())
	if err != nil {
		return fmt.Errorf("%s: could not subscribe extractor for errors\n%v", p.id, err)
	}

	transformerErrChannels := make([]chan error, len(transformers))
	for i, transformer := range transformers {
		transformerErrChannels[i], err = errorBroadcaster.Subscribe(transformer.ID())
		if err != nil {
			return fmt.Errorf("%s: could not subscribe transformer '%s' for errors\n%v", p.id, transformer.ID(), err)
		}
	}

	defer errorBroadcaster.Close()
	var waitgroup sync.WaitGroup
	waitgroup.Add(2 + len(transformers))
	go extractorRoutine(&extractorRoutineArgs{
		wg: &waitgroup,
		e:  extractor,
		eb: wrappedBroadcast(extractor.ID(), errorBroadcaster),
		ec: extErrors,
	})
	for i, transformer := range transformers {
		go transformerRoutine(&transformerRoutineArgs{
			wg: &waitgroup,
			t:  transformer,
			eb: wrappedBroadcast(transformer.ID(), errorBroadcaster),
			ec: transformerErrChannels[i],
		})
	}
	go ingestorRoutine(&ingestorRoutineArgs{
		wg: &waitgroup,
		i:  ingestor,
		eb: wrappedBroadcast(ingestor.ID(), errorBroadcaster),
		ec: ingErrors,
	})

	waitgroup.Wait()
	return nil
}

func wrappedBroadcast(id string, eb utils.ErrorBroadcaster) func(error) {
	return func(e error) { eb.Broadcast(id, e) }
}
