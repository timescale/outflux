package ts

import (
	"fmt"
	"log"

	"github.com/jackc/pgx"
	"github.com/timescale/outflux/internal/idrf"
	"github.com/timescale/outflux/internal/ingestion/config"
	"github.com/timescale/outflux/internal/utils"
)

type ingestDataArgs struct {
	// id of the ingestor used to subscribe and unsubscribe to errors from other goroutines
	ingestorID string
	// channel delivering errors that happened in other routines
	errChan chan error
	// the channel notified when the ingestor has completed
	ackChannel chan bool
	// the input channel that delivers the data to be inserted
	dataChannel chan idrf.Row
	// on each ${batchSize} rows inserted the ingestor checks if there is an error in some of the other goroutines
	batchSize uint16
	// if an error occurred in another goroutine should a rollback be done
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
	commitStrategy config.CommitStrategy
}

// Routine defines an interface that consumes a channel of idrf.Rows and
// and writes them to a TimescaleDB
type Routine interface {
	ingest(args *ingestDataArgs) error
}

// NewRoutine creates a new
func NewRoutine() Routine {
	return &defaultRoutine{}
}

type defaultRoutine struct{}

func (routine *defaultRoutine) ingest(args *ingestDataArgs) error {
	log.Printf("Starting data ingestor '%s'", args.ingestorID)

	err := utils.CheckError(args.errChan)
	if err != nil {
		log.Printf("%s: received external error before starting data insertion. Quitting\n", args.ingestorID)
		return nil
	}

	tx, err := openTx(args)
	if err != nil {
		return err
	}

	numInserts := uint(0)
	batchInserts := uint16(0)
	log.Printf("Will batch insert %d rows at once. With commit strategy: %v", args.batchSize, args.commitStrategy)
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

		if args.rollbackOnExternalError && utils.CheckError(args.errChan) != nil {
			log.Printf("%s: Error received from outside of ingestor. Rolling back\n", args.ingestorID)
			_ = tx.Rollback()
			return nil
		}

		numInserts += uint(batchInserts)
		batchInserts = 0
		if err = copyToDb(args, tableIdentifier, tx, batch); err != nil {
			return err
		}
		if args.commitStrategy != config.CommitOnEachBatch {
			continue
		}
		if err = commitTx(args, tx); err != nil {
			return err
		}
		if tx, err = openTx(args); err != nil {
			return err
		}
	}

	if batchInserts > 0 {
		batch = batch[:batchInserts]
		if err = copyToDb(args, tableIdentifier, tx, batch); err != nil {
			return err
		}
		numInserts += uint(batchInserts)
	}

	if err = commitTx(args, tx); err != nil {
		return err
	}

	log.Printf("%s: Complete. Inserted %d rows.\n", args.ingestorID, numInserts)
	return nil
}

func commitTx(args *ingestDataArgs, tx *pgx.Tx) error {
	err := tx.Commit()
	if err != nil {
		log.Printf("%s could not commit transaction in output db\n%v", args.ingestorID, err)
	}

	return err
}

func copyToDb(args *ingestDataArgs, identifier *pgx.Identifier, tx *pgx.Tx, batch [][]interface{}) error {
	source := pgx.CopyFromRows(batch)
	_, err := args.dbConn.CopyFrom(*identifier, args.colNames, source)
	if err != nil {
		log.Printf("%s could not insert batch of rows in output db\n%v", args.ingestorID, err)
		_ = tx.Rollback()
	}

	return err
}

func openTx(args *ingestDataArgs) (*pgx.Tx, error) {
	tx, err := args.dbConn.Begin()
	if err != nil {
		return nil, fmt.Errorf("%s: could not create transaction\n%v", args.ingestorID, err)
	}

	return tx, err
}
