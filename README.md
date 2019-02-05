# Outflux - Migrate InfluxDB to TimescaleDB

This repo contains code for exporting complete InfluxDB databases or selected measurements to TimescaleDB.

## Installation

Outflux is a Go project managed by `dep` (The go dependency management tool). To download the proper dependency versions, `dep` must be installed on your system. Instructions can be found on the [official documentation page](https://golang.github.io/dep/docs/installation.html). 

```bash
# Fetch the source code of Outflux
$ go get github.com/timescale/outflux
$ cd $GOPATH/src/github.com/timescale/outflux

# Fetch the required dependencies
$ dep ensure -v

# Install the Outflux binary:
$ go install 
```

## How to use

### Before using it

It is recommended that you have some InfluxDB database with some data. For testing purposes you can check out the [TSBS Data Loader Tool](https://github.com/timescale/tsbs) part of the Time Series Benchmark Suite. It can generate large ammounts of data for and load them in influx. Data can be generated with [one command](https://github.com/timescale/tsbs#data-generation), just specify the format as 'influx', and them load it in with [another command](https://github.com/timescale/tsbs#data-generation).

### Migrate

The Outlux CLI has only one command currently, `migrate`. The possible flags for the command can be seen by running:

```bash
$ cd $GOPATH/bin/
$ ./outflux migrate --help
```

Usage of the command is `outflux migrate database [measure1 measure2 ...] [flags]`. Where database is the name of the `database` you wish to export. `[measure1 measure2 ...]` are optional and if specified will export only those measurements from the selected database.

For example `outflux migrate benchmark cpu mem` will export the `cpu` and `mem` measurements from the `benchmark` database. On the other hand `outflux migrate benchmark` will export all measurements in the `benchmark` database.

### Available flags

| flag                       | type    | default               | description|
|----------------------------|---------|-----------------------|------------|
| input-host                 | string  | http://localhost:8086 | Host of the input database, http(s)://location:port. |
| input-pass                 | string  |                       | Password to use when connecting to the input database |
| input-user                 | string  |                       | Username to use when connecting to the input database |
| limit                      | uint64  | 0                     | If specified will limit the export points to its value. 0 = NO LIMIT |
| from                       | string  |                       | If specified will export data with a timestamp >= of its value. Accepted format: RFC3339 |
| to                         | string  |                       | If specified will export data with a timestamp <= of its value. Accepted format: RFC3339 |
| output-db                  | string  |                       | Output (Target) database that the data will be inserted into |
| output-db-ssl-mode         | string  | disable               | SSL mode to use when connecting to the output server. Valid options: disable, require, verify-ca, verify-full |
| output-host                | string  | localhost:5432        | Host of the output database, location:port. |
| output-user                | string  |                       | Username to use when connecting to the output database.
| output-pass                | string  |                       | Password to use when connecting to the output database |
| output-schema              | string  | public                | The schema of the output database that the data will be inserted into. |
| schema-strategy            | string  | CreateIfMissing       | Strategy to use for preparing the schema of the output database. Valid options: ValidateOnly, CreateIfMissing, DropAndCreate, DropCascadeAndCreate |
| chunk-size                 | uint16  | 15000                 | The export query will request data in chunks of this size. Must be > 0 |
| data-buffer                | uint16  | 15000                 | Size of the buffer holding exported data ready to be inserted in the output database |
| max-parallel               | uint8   | 2                     | Number of parallel measure extractions. One InfluxDB measure is exported using 1 worker |
| rollback-on-external-error | bool    | true                  | If set, when an error occurs while extracting the data, the insertion will be rollbacked. Otherwise it will try to commit |
| quiet                      | bool    | false                 | If specified will suppress any log to STDOUT |

### Examples

* Export the complete 'benchmark' database on localhost:8086 to the targetdb database on localhost:5432

```bash
$ outflux migrate benchmark \
> --input-user=test \
> --input-pass=test \
> --output-db=targetdb \
> --output-user=test \
> --output-pass=test
```

* Export only measurement 'cpu' from the 'benchmark' drop the existing 'cpu' table in 'targetdb' if exists, create if not
```bash
$ outflux migrate benchmark cpu \
> --input-user=test \
> --input-pass=test \
> --output-db=targetdb \
> --output-user=test \
> --output-pass=test \
> --schema-strategy=DropAndCreate
```

* Export only the 1,000,000 rows from measurements 'cpu' and 'mem' from 'benchmark', starting from Jan 14th 2019 09:00
```bash
$ ./outflux migrate benchmark cpu mem \
> --input-user=test \
> --input-pass=test \
> --output-db=targetdb \
> --output-user=test \
> --output-pass=test \
> --limit=1000000 \
> --from=2019-01-01T09:00:00Z
```