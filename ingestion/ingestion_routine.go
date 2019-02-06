package ingestion

import (
	"database/sql"
	"fmt"
	"log"

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
	// the database connection
	dbConn *sql.DB
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
	log.Printf("Starting data ingestor '%s'", args.ingestorID)
	defer close(args.ackChannel)
	defer args.dbConn.Close()

	errorChannel, err := args.errorBroadcaster.Subscribe(args.ingestorID)
	if err != nil {
		err = fmt.Errorf("ingestor '%s': could not subscribe for errors.\n%v", args.ingestorID, err)
		args.errorBroadcaster.Broadcast(args.ingestorID, err)
		args.preparedStatement.Close()
		log.Printf("%s: Data ingestor closing early", args.ingestorID)
		return
	}

	defer args.errorBroadcaster.Unsubscribe(args.ingestorID)

	err = utils.CheckError(errorChannel)
	if err != nil {
		args.preparedStatement.Close()
		log.Printf("%s: received external error before starting data insertion. Quitting\n", args.ingestorID)
		return
	}

	numInserts := uint(0)
	for row := range args.dataChannel {
		err := insertRow(args, row)
		if err != nil {
			log.Printf("%s could not insert row in output db. Rolling back\n", args.ingestorID)
			args.errorBroadcaster.Broadcast(args.ingestorID, err)
			return
		}

		numInserts++
		if numInserts%uint(args.batchSize) == 0 && utils.CheckError(errorChannel) != nil {
			if args.rollbackOnExternalError {
				log.Printf("%s: Error received from outside of ingestor. Rolling back\n", args.ingestorID)
				closeAndRollback(args.preparedStatement, args.transaction)
				return
			}
			break
		}
	}

	if utils.CheckError(errorChannel) != nil && args.rollbackOnExternalError {
		log.Printf("%s: Error received from outside of ingestor. Rollbacking\n", args.ingestorID)
		closeAndRollback(args.preparedStatement, args.transaction)
		return
	}

	err = args.preparedStatement.Close()
	if err != nil {
		err = fmt.Errorf("ingestor '%s': couldn't close prepared statement.\n%v", args.ingestorID, err)
		log.Printf("%v\n", err)
		args.errorBroadcaster.Broadcast(args.ingestorID, err)
		return
	}

	err = args.transaction.Commit()
	if err != nil {
		err = fmt.Errorf("ingestor '%s': couldn't commit transaction.\n%v", args.ingestorID, err)
		log.Printf("%v\n", err)
		args.errorBroadcaster.Broadcast(args.ingestorID, err)
		return
	}

	log.Printf("Ingestor '%s' complete. Inserted %d rows.\n", args.ingestorID, numInserts)
	args.ackChannel <- true
}

func insertRow(args *ingestDataArgs, row idrf.Row) error {
	values, err := args.converter.ConvertValues(row)
	if err != nil {
		err = fmt.Errorf("ingestor '%s': could not convert row:%v", args.ingestorID, err)
		log.Printf("%v\n", err)
		closeAndRollback(args.preparedStatement, args.transaction)
		return err
	}
	_, err = args.preparedStatement.Exec(values...)
	if err != nil {
		err = fmt.Errorf("ingestor '%s': could not execute prepared statement:\n%v", args.ingestorID, err)
		log.Printf("%v\n", err)
		closeAndRollback(args.preparedStatement, args.transaction)
		return err
	}

	return nil
}

func closeAndRollback(preparedStatement *sql.Stmt, transaction *sql.Tx) {
	preparedStatement.Close()
	transaction.Rollback()
}
