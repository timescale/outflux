package ingestion

import (
	"fmt"
	"log"

	"github.com/jackc/pgx"
	"github.com/timescale/outflux/internal/idrf"
	"github.com/timescale/outflux/internal/utils"
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
	// on each ${batchSize} rows inserted the ingestor checks if there is an error in some of the other goroutines
	batchSize uint16
	// if an error occured in another goroutine should a rollback be done
	rollbackOnExternalError bool
	// the database connection
	dbConn *pgx.Conn
	// column names
	colNames []string
	// name of table where inserts happen
	tableName string
	// name of schema where the table is
	schemaName string
	// commit strategy
	commitStrategy CommitStrategy
}

// Routine defines an interface that consumes a channel of idrf.Rows and
// and writes them to a TimescaleDB
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
		log.Printf("%s: Data ingestor closing early", args.ingestorID)
		return
	}

	defer args.errorBroadcaster.Unsubscribe(args.ingestorID)

	err = utils.CheckError(errorChannel)
	if err != nil {
		log.Printf("%s: received external error before starting data insertion. Quitting\n", args.ingestorID)
		return
	}

	var tx *pgx.Tx
	if tx, err = openTx(args); err != nil {
		return
	}

	numInserts := uint(0)
	batchInserts := uint16(0)
	log.Printf("Will batch insert %d rows at once. With strategy: %v", args.batchSize, args.commitStrategy)
	batch := make([][]interface{}, args.batchSize)
	var tableIdentifier *pgx.Identifier
	if args.schemaName != "" {
		tableIdentifier = &pgx.Identifier{args.schemaName, args.tableName}
	} else {
		tableIdentifier = &pgx.Identifier{args.tableName}
	}

	for row := range args.dataChannel {
		batch[batchInserts] = row
		batchInserts++
		if batchInserts < args.batchSize {
			continue
		}

		if args.rollbackOnExternalError && utils.CheckError(errorChannel) != nil {
			log.Printf("%s: Error received from outside of ingestor. Rolling back\n", args.ingestorID)
			_ = tx.Rollback()
			return
		}

		numInserts += uint(batchInserts)
		batchInserts = 0
		if err = copyToDb(args, tableIdentifier, tx, batch); err != nil {
			return
		}

		if args.commitStrategy != CommitOnEachBatch {
			continue
		}

		if err = commitTx(args, tx); err != nil {
			return
		}

		if tx, err = openTx(args); err != nil {
			return
		}
	}

	if args.rollbackOnExternalError && utils.CheckError(errorChannel) != nil {
		log.Printf("%s: Error received from outside of ingestor. Rollbacking\n", args.ingestorID)
		_ = tx.Rollback()
		return
	}

	if batchInserts > 0 {
		batch = batch[:batchInserts]
		if err = copyToDb(args, tableIdentifier, tx, batch); err != nil {
			return
		}
		numInserts += uint(batchInserts)
	}

	if err = commitTx(args, tx); err != nil {
		return
	}

	log.Printf("Ingestor '%s' complete. Inserted %d rows.\n", args.ingestorID, numInserts)
	args.ackChannel <- true
}

func commitTx(args *ingestDataArgs, tx *pgx.Tx) error {
	err := tx.Commit()
	if err != nil {
		log.Printf("%s could not commit transaction in output db\n%v", args.ingestorID, err)
		args.errorBroadcaster.Broadcast(args.ingestorID, err)
	}

	return err
}

func copyToDb(args *ingestDataArgs, identifier *pgx.Identifier, tx *pgx.Tx, batch [][]interface{}) error {
	source := pgx.CopyFromRows(batch)
	_, err := args.dbConn.CopyFrom(*identifier, args.colNames, source)
	if err != nil {
		log.Printf("%s could not insert batch of rows in output db\n%v", args.ingestorID, err)
		args.errorBroadcaster.Broadcast(args.ingestorID, err)
		_ = tx.Rollback()
	}

	return err
}

func openTx(args *ingestDataArgs) (*pgx.Tx, error) {
	tx, err := args.dbConn.Begin()
	if err != nil {
		err = fmt.Errorf("%s: could not create transaction\n%v", args.ingestorID, err)
		args.errorBroadcaster.Broadcast(args.ingestorID, err)
	}

	return tx, err
}
