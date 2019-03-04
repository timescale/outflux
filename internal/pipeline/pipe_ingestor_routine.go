package pipeline

import (
	"sync"

	"github.com/timescale/outflux/internal/ingestion"
)

type ingestorRoutineArgs struct {
	wg *sync.WaitGroup
	i  ingestion.Ingestor
	eb func(error)
	ec chan error
}

func ingestorRoutine(args *ingestorRoutineArgs) {
	err := args.i.Start(args.ec)
	if err != nil {
		args.eb(err)
	}

	args.wg.Done()
}
