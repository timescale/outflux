package ingestion

import (
	"database/sql"
	"fmt"

	"github.com/timescale/outflux/utils"

	"github.com/timescale/outflux/idrf"
)

type ingestDataArgs struct {
	// id of the ingestor used to subscribe and unsubscribe to errors from other goroutines
	ingestorID string
	// the error broadcaster that delivers errors from other goroutines and can also send errors to them
	errorBroadcaster utils.ErrorBroadcaster
	// the channel notified when the ingestor has completed
	ackChannel chan bool
	// the input channel that delivers the data to be inserted
	dataChannel chan idrf.Row
	// the prepared statement for inserting the data, created in the supplied transaction
	preparedStatement *sql.Stmt
	// the supplied transaction that will commit if all data is inserted, or rollback
	transaction *sql.Tx
	// the IDRF to SQL converter
	converter IdrfConverter
	// on each ${batchSize} rows inserted the ingestor checks if there is an error in some of the other goroutines
	batchSize uint
	// if an error occured in another goroutine should a rollback be done
	rollbackOnExternalError bool
}
type Routine interface {
	ingestData(args *ingestDataArgs)
}

func NewIngestionRoutine() Routine {
	return &defaultIngestionRoutine{}
}

type defaultIngestionRoutine struct{}

func (routine *defaultIngestionRoutine) ingestData(args *ingestDataArgs) {
	defer close(args.ackChannel)
	defer args.preparedStatement.Close()

	errorChannel, err := args.errorBroadcaster.Subscribe(args.ingestorID)
	if err != nil {
		err = fmt.Errorf("ingestor %s: could not subscribe for errors.\n%v", args.ingestorID, err)
		args.errorBroadcaster.Broadcast(args.ingestorID, err)
		return
	}

	defer args.errorBroadcaster.Unsubscribe(args.ingestorID)

	err = checkError(errorChannel)
	if err != nil {
		return
	}

	numInserts := uint(0)
	for row := range args.dataChannel {
		values, err := args.converter.ConvertValues(row)
		if err != nil {
			fmt.Printf("Could not convert row:\n%v\n", err)
			args.transaction.Rollback()
			return
		}
		_, err = args.preparedStatement.Exec(values...)
		if err != nil {
			fmt.Printf("Could not execute prepared statement:\n%v\n", err)
			args.transaction.Rollback()
			return
		}

		numInserts++
		if numInserts%args.batchSize == 0 {
			fmt.Printf("Number of inserted rows:%d\n", numInserts)
		}

		if numInserts%args.batchSize == 0 && checkError(errorChannel) != nil {
			if args.rollbackOnExternalError {
				args.transaction.Rollback()
			}
			break
		}
	}

	err = args.transaction.Commit()
	if err != nil {
		err = fmt.Errorf("ingestor %s: couldn't commit transaction.\n%v", args.ingestorID, err)
		args.errorBroadcaster.Broadcast(args.ingestorID, err)
		return
	}

	args.ackChannel <- true
}

func checkError(errorChannel chan error) error {
	select {
	case err := <-errorChannel:
		return err
	default:
		return nil
	}
}
