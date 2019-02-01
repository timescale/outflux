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
	batchSize uint16
	// if an error occured in another goroutine should a rollback be done
	rollbackOnExternalError bool
	// logger
	log utils.Logger
}
type Routine interface {
	ingestData(args *ingestDataArgs)
}

// NewIngestionRoutine creates an instance of the routine that will ingest data in the target db
func NewIngestionRoutine() Routine {
	return &defaultIngestionRoutine{}
}

type defaultIngestionRoutine struct{}

func (routine *defaultIngestionRoutine) ingestData(args *ingestDataArgs) {
	args.log.Log(fmt.Sprintf("Starting data ingestor '%s'", args.ingestorID))
	defer close(args.ackChannel)

	errorChannel, err := args.errorBroadcaster.Subscribe(args.ingestorID)
	if err != nil {
		err = fmt.Errorf("ingestor '%s': could not subscribe for errors.\n%v", args.ingestorID, err)
		args.errorBroadcaster.Broadcast(args.ingestorID, err)
		args.preparedStatement.Close()
		args.log.Log("Data ingestor closing early")
		return
	}

	defer args.errorBroadcaster.Unsubscribe(args.ingestorID)

	err = utils.CheckError(errorChannel)
	if err != nil {
		args.preparedStatement.Close()
		args.log.Log(fmt.Sprintf("Ingestor '%s' received external error before starting data insertion. Quitting", args.ingestorID))
		return
	}

	numInserts := uint(0)
	for row := range args.dataChannel {
		err := insertRow(args, row)
		if err != nil {
			args.log.Log(fmt.Sprintf("Ingestor '%s' could not insert row in output db", args.ingestorID))
			args.errorBroadcaster.Broadcast(args.ingestorID, err)
			return
		}

		numInserts++
		if numInserts%uint(args.batchSize) == 0 {
			args.log.Log(fmt.Sprintf("Ingestor '%s': Inserted %d rows\n", args.ingestorID, numInserts))
		}

		if numInserts%uint(args.batchSize) == 0 && utils.CheckError(errorChannel) != nil {
			if args.rollbackOnExternalError {
				args.log.Log(fmt.Sprintf("Ingestor '%s': Error received from outside of ingestor. Rollbacking", args.ingestorID))
				closeAndRollback(args.preparedStatement, args.transaction)
				return
			}
			break
		}
	}

	if utils.CheckError(errorChannel) != nil && args.rollbackOnExternalError {
		args.log.Log(fmt.Sprintf("Ingestor '%s': Error received from outside of ingestor. Rollbacking", args.ingestorID))
		closeAndRollback(args.preparedStatement, args.transaction)
		return
	}

	err = args.preparedStatement.Close()
	if err != nil {
		err = fmt.Errorf("ingestor '%s': couldn't close prepared statement.\n%v", args.ingestorID, err)
		args.log.Log(err)
		args.errorBroadcaster.Broadcast(args.ingestorID, err)
		return
	}

	err = args.transaction.Commit()
	if err != nil {
		err = fmt.Errorf("ingestor '%s': couldn't commit transaction.\n%v", args.ingestorID, err)
		args.log.Log(err)
		args.errorBroadcaster.Broadcast(args.ingestorID, err)
		return
	}

	args.log.Log(fmt.Sprintf("Ingestor '%s' complete. Inserted %d rows.", args.ingestorID, numInserts))
	args.ackChannel <- true
}

func insertRow(args *ingestDataArgs, row idrf.Row) error {
	values, err := args.converter.ConvertValues(row)
	if err != nil {
		err = fmt.Errorf("ingestor '%s': could not convert row:%v", args.ingestorID, err)
		args.log.Log(err)
		closeAndRollback(args.preparedStatement, args.transaction)
		return err
	}
	_, err = args.preparedStatement.Exec(values...)
	if err != nil {
		err = fmt.Errorf("ingestor '%s': could not execute prepared statement:\n%v", args.ingestorID, err)
		args.log.Log(err)
		closeAndRollback(args.preparedStatement, args.transaction)
		return err
	}

	return nil
}

func closeAndRollback(preparedStatement *sql.Stmt, transaction *sql.Tx) {
	preparedStatement.Close()
	transaction.Rollback()
}
