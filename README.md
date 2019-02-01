# Outflux - Migrate InfluxDB to TimescaleDB

This repo contains code for exporting complete InfluxDB databases or selected measurements to TimescaleDB.

# Instalation

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