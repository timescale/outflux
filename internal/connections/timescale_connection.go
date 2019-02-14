package connections

import (
	"database/sql"
	"fmt"
	"github.com/jackc/pgx"
	"log"
	"strconv"
	"strings"

	// Postgres driver
	_ "github.com/lib/pq"
)

const (
	// example: postgres://test:test@localhost:5432/test?sslmode=disable
	postgresConnectionStringTemplate = "postgres://%s:%s@%s/%s%s"
)

// TSConnectionParams contains all the required info to open a connection to a Timescale database
type TSConnectionParams struct {
	Server               string
	Username             string
	Password             string
	Database             string
	AdditionalConnParams map[string]string
}

// TSConnectionService creates new timescale db connections
type TSConnectionService interface {
	NewConnection(params *TSConnectionParams) (*sql.DB, error)
	NewPGXConnection(params *TSConnectionParams) (*pgx.Conn, error)
}

type defaultTSConnectionService struct{}

// NewTSConnectionService creates a new TSConnectionService instance
func NewTSConnectionService() TSConnectionService {
	return &defaultTSConnectionService{}
}

// NewTimescaleConnection opens a new database connection
func (s *defaultTSConnectionService) NewConnection(params *TSConnectionParams) (*sql.DB, error) {
	connStr := buildConnectionString(params)
	log.Printf("Will connect to output database with: %s", connStr)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		err = fmt.Errorf("couldn't connect to target database: %v", err)
		return nil, err
	}

	return db, nil
}

func buildConnectionString(params *TSConnectionParams) string {
	additionalParams := connectionParamsToString(params.AdditionalConnParams)

	//postgresConnectionStringTemplate = "postgres://%s:%s@%s/%s?%s"
	return fmt.Sprintf(
		postgresConnectionStringTemplate,
		params.Username, params.Password, params.Server, params.Database, additionalParams)
}

func connectionParamsToString(params map[string]string) string {
	if params == nil {
		return ""
	}

	singleParams := make([]string, len(params))
	current := 0
	for key, value := range params {
		singleParams[current] = fmt.Sprintf("%s=%s", key, value)
		current++
	}

	return "?" + strings.Join(singleParams, "&")
}

func (s *defaultTSConnectionService) NewPGXConnection(params *TSConnectionParams) (*pgx.Conn, error) {
	serverAndPort := strings.Split(params.Server, ":")
	port, _ := strconv.ParseUint(serverAndPort[1], 10, 16)
	connConfig := pgx.ConnConfig{
		Host:     serverAndPort[0],
		Port:     uint16(port),
		Database: params.Database,
		User:     params.Username,
		Password: params.Password,
	}
	return pgx.Connect(connConfig)
}
