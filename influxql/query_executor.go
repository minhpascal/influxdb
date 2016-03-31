package influxql

import (
	"errors"
	"expvar"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/models"
)

var (
	// ErrInvalidQuery is returned when executing an unknown query type.
	ErrInvalidQuery = errors.New("invalid query")

	// ErrNotExecuted is returned when a statement is not executed in a query.
	// This can occur when a previous statement in the same query has errored.
	ErrNotExecuted = errors.New("not executed")
)

const (
	// DefaultQueryTimeout is the default timeout for executing a query.
	// A value of zero will have no query timeout.
	DefaultQueryTimeout = time.Duration(0)
)

// Statistics for the QueryExecutor
const (
	statQueriesActive          = "queriesActive"   // Number of queries currently being executed
	statQueryExecutionDuration = "queryDurationNs" // Total (wall) time spent executing queries
)

// ErrDatabaseNotFound returns a database not found error for the given database name.
func ErrDatabaseNotFound(name string) error { return fmt.Errorf("database not found: %s", name) }

// ErrMeasurementNotFound returns a measurement not found error for the given measurement name.
func ErrMeasurementNotFound(name string) error { return fmt.Errorf("measurement not found: %s", name) }

// ExecutionContext contains state that the query is currently executing with.
type ExecutionContext struct {
	// The statement ID of the executing query.
	StatementID int

	// The query ID of the executing query.
	QueryID uint64

	// Output channel where results and errors should be sent.
	Results chan *Result

	// The database the query is running against.
	Database string

	// The requested maximum number of points to return in each result.
	ChunkSize int

	// A channel that is closed when the query is interrupted.
	InterruptCh <-chan struct{}
}

// StatementExecutor executes a statement within the QueryExecutor.
type StatementExecutor interface {
	// ExecuteStatement executes a statement. Results should be sent to the
	// results channel in the ExecutionContext.
	ExecuteStatement(stmt Statement, ctx *ExecutionContext) error

	// NormalizeStatement adds a default database and policy to the
	// measurements in the statement.
	NormalizeStatement(stmt Statement, database string) error
}

// QueryExecutor executes every statement in an Query.
type QueryExecutor struct {
	// Used for executing a statement in the query.
	StatementExecutor StatementExecutor

	// Used for managing and tracking running queries.
	QueryManager QueryManager

	// Query execution timeout.
	QueryTimeout time.Duration

	// Output of all logging.
	// Defaults to discarding all log output.
	LogOutput io.Writer

	// expvar-based stats.
	statMap *expvar.Map
}

// NewQueryExecutor returns a new instance of QueryExecutor.
func NewQueryExecutor() *QueryExecutor {
	return &QueryExecutor{
		QueryTimeout: DefaultQueryTimeout,
		LogOutput:    ioutil.Discard,
		statMap:      influxdb.NewStatistics("queryExecutor", "queryExecutor", nil),
	}
}

// ExecuteQuery executes each statement within a query.
func (e *QueryExecutor) ExecuteQuery(query *Query, database string, chunkSize int, closing chan struct{}) <-chan *Result {
	results := make(chan *Result)
	go e.executeQuery(query, database, chunkSize, closing, results)
	return results
}

func (e *QueryExecutor) executeQuery(query *Query, database string, chunkSize int, closing <-chan struct{}, results chan *Result) {
	defer close(results)

	e.statMap.Add(statQueriesActive, 1)
	defer func(start time.Time) {
		e.statMap.Add(statQueriesActive, -1)
		e.statMap.Add(statQueryExecutionDuration, time.Since(start).Nanoseconds())
	}(time.Now())

	qerr := &QueryError{}
	var qid uint64
	if e.QueryManager != nil {
		var err error
		qid, closing, err = e.QueryManager.AttachQuery(&QueryParams{
			Query:       query,
			Database:    database,
			Timeout:     e.QueryTimeout,
			InterruptCh: closing,
			Error:       qerr,
		})
		if err != nil {
			results <- &Result{Err: err}
			return
		}

		defer e.QueryManager.KillQuery(qid)
	}

	logger := e.logger()

	// Setup the execution context that will be used when executing statements.
	ctx := ExecutionContext{
		Results:     results,
		Database:    database,
		ChunkSize:   chunkSize,
		InterruptCh: closing,
	}

	var i int
loop:
	for ; i < len(query.Statements); i++ {
		ctx.StatementID = i
		stmt := query.Statements[i]

		// If a default database wasn't passed in by the caller, check the statement.
		defaultDB := database
		if defaultDB == "" {
			if s, ok := stmt.(HasDefaultDatabase); ok {
				defaultDB = s.DefaultDatabase()
			}
		}

		// Rewrite statements, if necessary.
		// This can occur on meta read statements which convert to SELECT statements.
		newStmt, err := RewriteStatement(stmt)
		if err != nil {
			results <- &Result{Err: err}
			break
		}
		stmt = newStmt

		// Normalize each statement.
		if err := e.StatementExecutor.NormalizeStatement(stmt, defaultDB); err != nil {
			results <- &Result{Err: err}
			break
		}

		// Log each normalized statement.
		logger.Println(stmt.String())

		// Handle a query management queries specially so they don't go
		// to the underlying statement executor.
		switch stmt := stmt.(type) {
		case *ShowQueriesStatement:
			rows, err := e.executeShowQueriesStatement(stmt)
			results <- &Result{
				StatementID: i,
				Series:      rows,
				Err:         err,
			}

			if err != nil {
				break loop
			}
			continue loop
		case *KillQueryStatement:
			err := e.executeKillQueryStatement(stmt)
			results <- &Result{
				StatementID: i,
				Err:         err,
			}

			if err != nil {
				break loop
			}
			continue loop
		}

		// Send any other statements to the underlying statement executor.
		err = e.StatementExecutor.ExecuteStatement(stmt, &ctx)
		if err == ErrQueryInterrupted {
			// Query was interrupted so retrieve the real interrupt error from
			// the query manager.
			err = qerr.Error()
		}

		// Send an error for this result if it failed for some reason.
		if err != nil {
			results <- &Result{
				StatementID: i,
				Err:         err,
			}
			// Stop after the first error.
			break
		}
	}

	// Send error results for any statements which were not executed.
	for ; i < len(query.Statements)-1; i++ {
		results <- &Result{
			StatementID: i,
			Err:         ErrNotExecuted,
		}
	}
}

func (e *QueryExecutor) executeKillQueryStatement(stmt *KillQueryStatement) error {
	if e.QueryManager == nil {
		return ErrNoQueryManager
	}
	return e.QueryManager.KillQuery(stmt.QueryID)
}

func (e *QueryExecutor) executeShowQueriesStatement(q *ShowQueriesStatement) (models.Rows, error) {
	return ExecuteShowQueriesStatement(e.QueryManager, q)
}

func (e *QueryExecutor) logger() *log.Logger {
	return log.New(e.LogOutput, "[query] ", log.LstdFlags)
}
