package pipeline

import (
	"sync"

	"github.com/timescale/outflux/internal/transformation"
)

type transformerRoutineArgs struct {
	wg *sync.WaitGroup
	t  transformation.Transformer
	eb func(error)
	ec chan error
}

func transformerRoutine(args *transformerRoutineArgs) {
	err := args.t.Start(args.ec)
	if err != nil {
		args.eb(err)
	}
	args.wg.Done()
}
