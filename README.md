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
$ cd cmd/outlux
$ go install 
```

## How to use

### Before using it

It is recommended that you have some InfluxDB database with some data. For testing purposes you can check out the [TSBS Data Loader Tool](https://github.com/timescale/tsbs) part of the Time Series Benchmark Suite. It can generate large ammounts of data for and load them in influx. Data can be generated with [one command](https://github.com/timescale/tsbs#data-generation), just specify the format as 'influx', and them load it in with [another command](https://github.com/timescale/tsbs#data-generation).

### !Connection params!
Detailed information about how to pass the connection parameters to Outflux can be found at the bottom of this document at the [Connection](section)


### Schema Transfer

The Outflux CLI has two commands. The first one is `schema-transfer`. This command will discover the schema of a InfluxDB database, or specific measurements in a InfluxDB database, and depending on the strategy selected create or verify a TimescaleDB database that could hold the data.

The possible flags for the command can be seen by running 

```bash
$ cd $GOPATH/bin/
$ ./outflux schema-transfer --help
```

Usage of the is `outflux schema-transfer database [measure1 measure2 ...] [flags]`. Where database is the name of the InfluxDB database you wish to export. `[measure1 ...] ` are optional and if specified will export only those measurements from the selected database.

For example `outflux schema-transfer benchmark cpu mem` will discover the schema for the `cpu` and `mem` measurements from the `benchmark` database.

Available flags for schema-transfer are:

| flag             | type    | default               | description |
|------------------|---------|-----------------------|-------------|
| input-server     | string  | http://localhost:8086 | Location of the input database, http(s)://location:port. |
| input-pass       | string  |                       | Password to use when connecting to the input database |
| input-user       | string  |                       | Username to use when connecting to the input database |
| output-conn      | string  | sslmode=disable       | Connection string to use to connect to the output database|
| output-schema    | string  |                       | The schema of the output database that the data will be inserted into |
| schema-strategy  | string  | CreateIfMissing       | Strategy to use for preparing the schema of the output database. Valid options: ValidateOnly, CreateIfMissing, DropAndCreate, DropCascadeAndCreate |
| quiet            | bool    | false                 | If specified will suppress any log to STDOUT |

### Migrate

The second command of the Outflux CLI is `migrate`. The possible flags for the command can be seen by running:

```bash
$ cd $GOPATH/bin/
$ ./outflux migrate --help
```

Usage of the command is `outflux migrate database [measure1 measure2 ...] [flags]`. Where database is the name of the `database` you wish to export. `[measure1 measure2 ...]` are optional and if specified will export only those measurements from the selected database.

For example `outflux migrate benchmark cpu mem` will export the `cpu` and `mem` measurements from the `benchmark` database. On the other hand `outflux migrate benchmark` will export all measurements in the `benchmark` database.

Available flags are:

| flag                       | type    | default               | description|
|----------------------------|---------|-----------------------|------------|
| input-server               | string  | http://localhost:8086 | Location of the input database, http(s)://location:port. |
| input-pass                 | string  |                       | Password to use when connecting to the input database |
| input-user                 | string  |                       | Username to use when connecting to the input database |
| limit                      | uint64  | 0                     | If specified will limit the export points to its value. 0 = NO LIMIT |
| from                       | string  |                       | If specified will export data with a timestamp >= of its value. Accepted format: RFC3339 |
| to                         | string  |                       | If specified will export data with a timestamp <= of its value. Accepted format: RFC3339 |
| output-conn                | string  | sslmode=disable       | Connection string to use to connect to the output database|
| output-schema              | string  | public                | The schema of the output database that the data will be inserted into. |
| schema-strategy            | string  | CreateIfMissing       | Strategy to use for preparing the schema of the output database. Valid options: ValidateOnly, CreateIfMissing, DropAndCreate, DropCascadeAndCreate |
| chunk-size                 | uint16  | 15000                 | The export query will request data in chunks of this size. Must be > 0 |
| batch-size                 | uint16  | 8000                  | The size of the batch inserted in to the output database |
| data-buffer                | uint16  | 15000                 | Size of the buffer holding exported data ready to be inserted in the output database |
| max-parallel               | uint8   | 2                     | Number of parallel measure extractions. One InfluxDB measure is exported using 1 worker |
| rollback-on-external-error | bool    | true                  | If set, when an error occurs while extracting the data, the insertion will be rollbacked. Otherwise it will try to commit |
| quiet                      | bool    | false                 | If specified will suppress any log to STDOUT |

### Examples

* Use environment variables for determining output db connection
```bash
$ PGPORT=5433
$ PGDATABASE=test
$ PGUSER=test
...
$ ./outflux schema-transfer benchmark
```

* Export the complete 'benchmark' database on 'localhost:8086' to the 'targetdb' database on localhost:5432. Use environment variable to set InfluxDB password

```bash
$ PGDATABASE=some_default_db
$ INFLUX_PASSWORD=test
...
$ outflux migrate benchmark \
> --input-user=test \
> --input-pass=test \
> --output-conn='dbname=targetdb user=test password=test' \
```

* Export only measurement 'cpu' from the 'benchmark' drop the existing 'cpu' table in 'targetdb' if exists, create if not
```bash
$ outflux migrate benchmark cpu \
> --input-user=test \
> --input-pass=test \
> --output-con='dbname=targetdb user=test pass=test'\
> --schema-strategy=DropAndCreate
```

* Export only the 1,000,000 rows from measurements 'cpu' and 'mem' from 'benchmark', starting from Jan 14th 2019 09:00
```bash
$ ./outflux migrate benchmark cpu mem \
> --input-user=test \
> --input-pass=test \
> --limit=1000000 \
> --from=2019-01-01T09:00:00Z
```


## Connection 

### TimescaleDB connection params

The connection parameters to the TimescaleDB instance can be passed to Outflux in several ways. One is through the Postgres Environment Variables. Supported envrionment variables are: `PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD, PGSSLMODE, PGSSLCERT, PGSSLKEY, PGSSLROOTCERT, PGAPPNAME, PGCONNECT_TIMEOUT`. If they are not specified defaults used are: host=localhost, dbname=postgres, pguser=$USER, and sslmode=disable.

The values of the enviroment variables can be **OVERRIDEN** by specifying the '--output-con' flag when executing Outflux. 

The connection string can be in the format URI or DSN format:
* example URI: "postgresql://username:password@host:port/dbname?connect_timeout=10"
* example DSN: "user=username password=password host=1.2.3.4 port=5432 dbname=mydb sslmode=disable"

### InfluxDB connection params

The connection parameters to the InfluxDB instance can be passed also through flags or environment variables. Supported/Expected environment variables are: `INFLUX_USERNAME, INFLUX_PASSWORD`.
These are the same environment variables that the InfluxDB CLI uses. 

If they are not set, or if you wish to override them, you can do so with the `--input-user` and `--input-pass`. 