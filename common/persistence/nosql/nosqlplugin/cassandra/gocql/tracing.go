package gocql

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.12.0"
	"go.opentelemetry.io/otel/trace"
)

const (
	instrumentationName = "go.temporal.io/common/persistence/nosqlplugin/cassandra/gocql"
	batchOperationName  = "batch"
	queryOperationName  = "query"
)

var (
	batchOperationAttr = semconv.DBOperationKey.String(batchOperationName)
	queryOperationAttr = semconv.DBOperationKey.String(queryOperationName)
)

type traceObserver struct {
	dbNameAttr    attribute.KeyValue
	batchSpanName string
	querySpanName string
	tracer        trace.Tracer

	nextBatchObserver gocql.BatchObserver
	nextQueryObserver gocql.QueryObserver
}

// ConfigureTracing registers tracing observers with the supplied
// gocql.ClusterConfig. Any pre-existing observers are preserved and will be
// called after the appropriate tracing listener.
func ConfigureTracing(config *gocql.ClusterConfig, tp trace.TracerProvider) {
	obs := traceObserver{

		// > In Cassandra, db.name SHOULD be set to the keyspace name
		// https://github.com/open-telemetry/opentelemetry-specification/blob/4f8d9f679d8e138693eb6fd085e6f6654012d116/specification/trace/semantic_conventions/database.md?plain=1#L179
		dbNameAttr: semconv.DBNameKey.String(config.Keyspace),

		// > If db.sql.table is not available [...] the span SHOULD be named
		// > "<db.operation> <db.name>"
		// https://github.com/open-telemetry/opentelemetry-specification/blob/4f8d9f679d8e138693eb6fd085e6f6654012d116/specification/trace/semantic_conventions/database.md?plain=1#L29
		batchSpanName:     fmt.Sprintf("%s %s", batchOperationName, config.Keyspace),
		querySpanName:     fmt.Sprintf("%s %s", queryOperationName, config.Keyspace),
		tracer:            tp.Tracer(instrumentationName),
		nextBatchObserver: config.BatchObserver,
		nextQueryObserver: config.QueryObserver,
	}
	config.BatchObserver = &obs
	config.QueryObserver = &obs
}

// ObserveBatch handles callbacks from gocql upon completion of a Cassandra
// batch operation.
func (sto *traceObserver) ObserveBatch(
	ctx context.Context,
	batch gocql.ObservedBatch,
) {
	sto.observe(
		ctx,
		sto.batchSpanName,
		batchOperationAttr,
		batch.Start,
		batch.End,
		"",
		batch.Statements,
		batch.Host.ConnectAddress(),
		batch.Host.Port(),
		batch.Err,
	)
	if sto.nextBatchObserver != nil {
		sto.nextBatchObserver.ObserveBatch(ctx, batch)
	}
}

// ObserveQuery handles callbacks from gocql upon completion of a Cassandra
// query operation.
func (sto *traceObserver) ObserveQuery(
	ctx context.Context,
	query gocql.ObservedQuery,
) {
	sto.observe(
		ctx,
		sto.querySpanName,
		queryOperationAttr,
		query.Start,
		query.End,
		query.Statement,
		nil,
		query.Host.ConnectAddress(),
		query.Host.Port(),
		query.Err,
	)
	if sto.nextQueryObserver != nil {
		sto.nextQueryObserver.ObserveQuery(ctx, query)
	}
}

func (sto *traceObserver) observe(
	ctx context.Context,
	spanName string,
	operation attribute.KeyValue,
	start time.Time,
	end time.Time,
	statement string,
	statements []string,
	addr net.IP,
	port int,
	err error,
) {
	_, span := sto.tracer.Start(
		ctx,
		spanName,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithTimestamp(start),
	)
	defer span.End(trace.WithTimestamp(end))
	if !span.IsRecording() {
		return
	}
	if len(statements) > 0 {
		statement = strings.Join(statements, "; ")
	}
	span.SetAttributes(
		semconv.DBSystemCassandra,
		sto.dbNameAttr,
		operation,
		semconv.DBStatementKey.String(statement),
		semconv.NetPeerIPKey.String(addr.String()),
		semconv.NetPeerPortKey.Int(port),
	)
	span.RecordError(err)
}
