package pipeline

import (
	"fmt"
	"sync"

	"github.com/timescale/outflux/internal/extraction"
	"github.com/timescale/outflux/internal/ingestion"
	"github.com/timescale/outflux/internal/utils"
)

func (p *defPipe) run(extractor extraction.Extractor, ingestor ingestion.Ingestor) error {
	errorBroadcaster := utils.NewErrorBroadcaster()
	ingErrors, err := errorBroadcaster.Subscribe(ingestor.ID())
	if err != nil {
		return fmt.Errorf("'%s': could not subscribe ingestor for errors\n%v", p.id, err)
	}
	extErrors, err := errorBroadcaster.Subscribe(extractor.ID())
	if err != nil {
		return fmt.Errorf("'%s': could not subscribe extractor for errors\n%v", p.id, err)
	}

	defer errorBroadcaster.Close()
	var waitgroup sync.WaitGroup
	waitgroup.Add(2)
	go extractorRoutine(&extractorRoutineArgs{
		wg: &waitgroup,
		e:  extractor,
		eb: wrappedBroadcast(extractor.ID(), errorBroadcaster),
		ec: extErrors,
	})
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
