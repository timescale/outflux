package main

import (
	"fmt"
	"log"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/timescale/outflux/internal/connections"
	"github.com/timescale/outflux/internal/idrf"
	"github.com/timescale/outflux/internal/pipeline"
	influxSchema "github.com/timescale/outflux/internal/schemamanagement/influx"
)

func createInfluxClient(args *pipeline.ConnectionConfig, connService connections.InfluxConnectionService) (influx.Client, error) {
	influxConnectionParams := &connections.InfluxConnectionParams{
		Server:   args.InputHost,
		Username: args.InputUser,
		Password: args.InputPass,
	}
	client, err := connService.NewConnection(influxConnectionParams)
	if err != nil {
		return nil, fmt.Errorf("could not create an influx client.\n%v", err)
	}

	return client, nil
}
func discoverSchemaForDatabase(app *appContext, args *pipeline.ConnectionConfig, influxClient influx.Client) ([]*idrf.DataSetInfo, error) {
	log.Println("All measurements selected for exporting")

	schemaManager := influxSchema.NewInfluxSchemaManager(influxClient, app.iqs, args.InputDb)
	discoveredDataSets, err := schemaManager.DiscoverDataSets()
	if err != nil {
		log.Printf("Couldn't discover the database schema\n%v", err)
		return nil, fmt.Errorf("could not discover the input database schema\n%v", err)
	}

	return discoveredDataSets, nil
}

func discoverSchemaForMeasures(app *appContext, args *pipeline.ConnectionConfig, influxClient influx.Client) ([]*idrf.DataSetInfo, error) {
	log.Printf("Selected measurements for exporting: %v\n", args.InputMeasures)

	schemaManager := influxSchema.NewInfluxSchemaManager(influxClient, app.iqs, args.InputDb)
	discoveredDataSets := make([]*idrf.DataSetInfo, len(args.InputMeasures))
	var err error
	for i, measureName := range args.InputMeasures {
		discoveredDataSets[i], err = schemaManager.FetchDataSet(args.InputDb, measureName)
		if err != nil {
			log.Printf("Could not discover schema for measurement: %s\n", measureName)
			return nil, err
		}
	}

	return discoveredDataSets, nil
}
