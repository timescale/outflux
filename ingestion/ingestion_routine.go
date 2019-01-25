package ingestion

import (
	"database/sql"
	"fmt"

	"github.com/timescale/outflux/extraction"
)

type ingestDataArgs struct {
	ackChannel        chan bool
	preparedStatement *sql.Stmt
	transaction       *sql.Tx
	extractionInfo    *extraction.ExtractedInfo
	converter         IdrfConverter
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

	numInserts := 0
	for row := range args.extractionInfo.DataChannel {
		values, err := args.converter.ConvertValues(row)
		if err != nil {
			fmt.Printf("Could not convert row:\n%v\n", err)
			args.ackChannel <- false
			args.preparedStatement.Close()
			args.transaction.Rollback()
			return
		}
		_, err = args.preparedStatement.Exec(values...)
		if err != nil {
			fmt.Printf("Could not execute prepared statement:\n%v\n", err)
			args.ackChannel <- false
			args.preparedStatement.Close()
			args.transaction.Rollback()
			return
		}

		numInserts++
		if numInserts%10000 == 0 {
			fmt.Printf("Inserted: %d\n", numInserts)
		}
	}

	args.preparedStatement.Close()
	err := args.transaction.Commit()
	if err != nil {
		fmt.Printf("Could not commit transaction\n%v\n", err)
		args.ackChannel <- false
	}

	args.ackChannel <- true
}
