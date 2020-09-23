# Outflux - Migrate InfluxDB to TimescaleDB
[![Go Report Card](https://goreportcard.com/badge/github.com/timescale/outflux)](https://goreportcard.com/report/github.com/timescale/outflux)


This repo contains code for exporting complete InfluxDB databases or selected measurements to TimescaleDB.

## Table of Contents

1. [Installation](#installation)
  - [Installing from source](#installing-from-source)
  - [Binary releases](#binary-releases)
2. [How to use](#how-to-use)
  - [Before using it](#before-using-it)
  - [Connection params](#connection-params)
  - [Schema Transfer](#schema-transfer)
  - [Migrate](#igrate)
  - [Examples](#examples)
3. [Connection](#connection)
  - [TimescaleDB connection params](#timescaledb-connection-params)
  - [InfluxDB connection params](#influxdb-connection-params)
4. [Known limitations](#known-limitations)

## Installation

### Installing from source

Outflux is a Go project managed by `go modules`. You can download it 
in any directory and on the first build it will download it's required dependencies.

Depending on where you downloaded it and the go version you're using, you may 
 need to set the `GO111MODULE` to `auto`, `on` or `off`. Learn about the `GO111MODULE` 
 environment variable [here](https://golang.org/cmd/go/#hdr-Module_support).

```bash
# Fetch the source code of Outflux in any directory
$ git clone git@github.com:timescale/outflux.git
$ cd ./outflux

# Install the Outflux binary (will automaticly detect and download)
# dependencies.
$ cd cmd/outflux
$ GO111MODULE=auto go install

# Building without installing will also fetch the required dependencies
$ GO111MODULE=auto go build ./... 
```

### Binary releases
We upload prepackaged binaries available for GNU/Linux, Windows and MacOS in the [releases](https://github.com/timescale/outflux/releases).
Just download the binary, extract the compressed tarball and run the executable

## How to use

Outflux supports InfluxDB versions 1.0 and upwards. We explicitly test for compatibility for versions 1.0, 1.5, 1.6, 1.7 and the `latest` tag of the InfluxDB docker container.

### Before using it

It is recommended that you have some InfluxDB database with some data. 
For testing purposes you can check out the [TSBS Data Loader Tool](https://github.com/timescale/tsbs) part of the Time Series Benchmark Suite. 
It can generate large ammounts of data for and load them in influx. 
Data can be generated with [one command](https://github.com/timescale/tsbs#data-generation), just specify the format as 'influx', and then load it in with [another command](https://github.com/timescale/tsbs#data-generation).

### Connection params
Detailed information about how to pass the connection parameters to Outflux can be found at the bottom of this document at the [Connection](section)


### Schema Transfer

The Outflux CLI has two commands. The first one is `schema-transfer`. This command will discover the schema of a InfluxDB database, or specific measurements in a InfluxDB database, and depending on the strategy selected create or verify a TimescaleDB database that could hold the data.

The possible flags for the command can be seen by running 

```bash
$ cd $GOPATH/bin/
$ ./outflux schema-transfer --help
```

Usage of the is `outflux schema-transfer database [measure1 measure2 ...] [flags]`. Where database is the name of the InfluxDB database you wish to export. `[measure1 ...] ` are optional and if specified will export only those measurements from the selected database. 
Additionally you can specify the retention policy with the `retention-policy` flag.

For example `outflux schema-transfer benchmark cpu mem` will discover the schema for the `cpu` and `mem` measurements from the `benchmark` database.

Available flags for schema-transfer are:

| flag                      | type    | default               | description |
|---------------------------|---------|-----------------------|-------------|
| input-server              | string  | http://localhost:8086 | Location of the input database, http(s)://location:port. |
| input-pass                | string  |                       | Password to use when connecting to the input database |
| input-user                | string  |                       | Username to use when connecting to the input database |
| input-unsafe-https        | bool    | false                 | Should 'InsecureSkipVerify' be passed to the input connection |
| retention-policy          | string  | autogen               | The retention policy to select the tags and fields from |
| output-conn               | string  | sslmode=disable       | Connection string to use to connect to the output database|
| output-schema             | string  |                       | The schema of the output database that the data will be inserted into |
| schema-strategy           | string  | CreateIfMissing       | Strategy to use for preparing the schema of the output database. Valid options: ValidateOnly, CreateIfMissing, DropAndCreate, DropCascadeAndCreate |
| tags-as-json              | bool    | false                 | If this flag is set to true, then the Tags of the influx measures being exported will be combined into a single JSONb column in Timescale |
| tags-column               | string  | tags                  | When `tags-as-json` is set, this column specifies the name of the JSON column for the tags |
| fields-as-json            | bool    | false                 | If this flag is set to true, then the Fields of the influx measures being exported will be combined into a single JSONb column in Timescale |
| fields-column             | string  | fields                | When `fields-as-json` is set, this column specifies the name of the JSON column for the fields |
| multishard-int-float-cast | bool    | false                 | If a field is Int64 in one shard, and Float64 in another, with this flag it will be cast to Float64 despite possible data loss |
| quiet                     | bool    | false                 | If specified will suppress any log to STDOUT |

### Migrate

The second command of the Outflux CLI is `migrate`. The possible flags for the command can be seen by running:

```bash
$ cd $GOPATH/bin/
$ ./outflux migrate --help
```

Usage of the command is `outflux migrate database [measure1 measure2 ...] [flags]`. Where database is the name of the `database` you wish to export. `[measure1 measure2 ...]` are optional and if specified will export only those measurements from the selected database. 

The retention policy can be specified with the `retention-policy` flag. By default the 'autogen' retention policy is used.

For example `outflux migrate benchmark cpu mem` will export the `cpu` and `mem` measurements from the `benchmark` database. On the other hand `outflux migrate benchmark` will export all measurements in the `benchmark` database.

Available flags are:

| flag                       | type    | default               | description|
|----------------------------|---------|-----------------------|------------|
| input-server               | string  | http://localhost:8086 | Location of the input database, http(s)://location:port. |
| input-pass                 | string  |                       | Password to use when connecting to the input database |
| input-user                 | string  |                       | Username to use when connecting to the input database |
| input-unsafe-https         | bool    | false                 | Should 'InsecureSkipVerify' be passed to the input connection |
| retention-policy           | string  | autogen               | The retention policy to select the data from |
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
| tags-as-json     | bool    | false                 | If this flag is set to true, then the Tags of the influx measures being exported will be combined into a single JSONb column in Timescale |
| tags-column      | string  | tags                  | When `tags-as-json` is set, this column specifies the name of the JSON column for the tags |
| fields-as-json   | bool    | false                 | If this flag is set to true, then the Fields of the influx measures being exported will be combined into a single JSONb column in Timescale |
| fields-column    | string  | fields                | When `fields-as-json` is set, this column specifies the name of the JSON column for the fields |
| multishard-int-float-cast | bool    | false                 | If a field is Int64 in one shard, and Float64 in another, with this flag it will be cast to Float64 despite possible data loss |
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

* Export only measurement 'cpu' from 'two_week' retention policy in the 'benchmark' database. 
Drop the existing '"two_week.cpu"' table in 'targetdb' if exists, create if not
```bash
$ outflux migrate benchmark two_week.cpu \
> --input-user=test \
> --input-pass=test \
> --output-conn='dbname=targetdb user=test pass=test'\
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

The connection parameters to the TimescaleDB instance can be passed to Outflux in several ways. One is through the Postgres Environment Variables. Supported environment variables are: `PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD, PGSSLMODE, PGSSLCERT, PGSSLKEY, PGSSLROOTCERT, PGAPPNAME, PGCONNECT_TIMEOUT`. If they are not specified defaults used are: host=localhost, dbname=postgres, pguser=$USER, and sslmode=disable.

The values of the enviroment variables can be **OVERRIDEN** by specifying the '--output-conn' flag when executing Outflux. 

The connection string can be in the format URI or DSN format:
* example URI: "postgresql://username:password@host:port/dbname?connect_timeout=10"
* example DSN: "user=username password=password host=1.2.3.4 port=5432 dbname=mydb sslmode=disable"

### InfluxDB connection params

The connection parameters to the InfluxDB instance can be passed also through flags or environment variables. Supported/Expected environment variables are: `INFLUX_USERNAME, INFLUX_PASSWORD`.
These are the same environment variables that the InfluxDB CLI uses. 

If they are not set, or if you wish to override them, you can do so with the `--input-user` and `--input-pass`. 
Also you can specify to Outflux to skip HTTPS verification when communicating with the InfluxDB server by setting the 
`--input-unsafe-https` flag to `true`. 

## Known limitations

### Fields with different data types across shards

Outflux doesn't support fields that have the same name but different data types across shards in InfluxDB, 
**UNLESS** the field is an `integer` and `float` in the InfluxDB shards. 
InfluxDB can store the fields as `integer` (64bit integer), `float` (64bit float), `string`, and `boolean`.
You can specify the `multishard-int-float-cast` flag. This will tell Outflux to cast the `integer` values to 
`float` values. A 64bit float can't hold all the int64 values, so this may result in scrambled data (for values > 2^53). 

If the same field is of any of the other possible InfluxDB types, an error will be thrown, since the values can't be 
converted.

This is also an issue even if you select a time interval in which a field has a consistent type, but exists as a different type
in a shard outside of that interval. This is because the `SHOW FIELD KEYS FROM measurement_name` doesn't accept a time interval
for which you would be asking
