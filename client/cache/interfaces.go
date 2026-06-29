package cache

import (
	"go.digitalxero.dev/spacetimedb-client/bsatn"
)

// CallbackID is an opaque identifier returned when registering a callback
// via [TableCache.OnInsert], [TableCache.OnDelete], or [TableCache.OnUpdate].
// Pass it to [TableCache.RemoveCallback] to unregister the callback.
type CallbackID uint64

// RowDecoder decodes a single BSATN-encoded row from a [bsatn.Reader]
// into a Go value. The returned value should be a pointer to the table
// struct type.
type RowDecoder func(r bsatn.Reader) (any, error)

// RowEncoder encodes a row value into BSATN bytes. The resulting bytes
// serve as the cache key for the row within a [TableCache].
type RowEncoder func(row any) []byte

// TableDef defines the schema for a SpacetimeDB table. Implement this
// interface for each table type you want to cache locally.
//
// TableName returns the server-side table name (must match exactly).
// DecodeRow reads a BSATN-encoded row into a Go struct (typically a pointer).
// EncodeRow converts a Go struct back into BSATN bytes for use as a cache key.
//
// Register a TableDef with [ClientCache.RegisterTable] or
// [client.DbConnection.RegisterTable] before starting the connection.
type TableDef interface {
	TableName() string
	DecodeRow(r bsatn.Reader) (any, error)
	EncodeRow(row any) []byte
}

// InsertCallback is invoked when a new row is inserted into the table cache.
// The row parameter is the decoded Go value (as returned by TableDef.DecodeRow).
type InsertCallback func(row any)

// DeleteCallback is invoked when a row is removed from the table cache.
// The row parameter is the decoded Go value that was deleted.
type DeleteCallback func(row any)

// UpdateCallback is invoked when a row is updated in the table cache.
// This only fires for tables whose [TableDef] also implements [TableDefWithPK].
// The oldRow and newRow parameters are the decoded Go values before and after
// the update.
type UpdateCallback func(oldRow any, newRow any)

// EventCallback is invoked when an ephemeral event row is delivered for an
// event table. Unlike inserts, event rows are not retained in the cache, so
// they fire only this callback and never appear in Count or Iter.
type EventCallback func(row any)

// TableDefWithPK extends [TableDef] for tables that have a primary key.
// When implemented, the cache detects updates by matching delete+insert pairs
// within the same transaction that share the same primary key value.
// Matched pairs fire [UpdateCallback] instead of separate delete and insert callbacks.
//
// PrimaryKey should return a comparable value (e.g., uint64, string) that
// uniquely identifies the row.
type TableDefWithPK interface {
	TableDef
	PrimaryKey(row any) any
}

// TypedTableDef defines a type-safe table definition for compile-time type safety.
// Implement this interface for each table type in generated client code.
type TypedTableDef[T any] interface {
	TableName() string
	DecodeRow(r bsatn.Reader) (T, error)
	EncodeRow(row T) []byte
}

// TypedTableDefWithPK extends TypedTableDef for tables with a primary key.
type TypedTableDefWithPK[T any, K comparable] interface {
	TypedTableDef[T]
	PrimaryKey(row T) K
}

// TypedInsertCallback is invoked when a new row is inserted, with compile-time type safety.
type TypedInsertCallback[T any] func(row T)

// TypedDeleteCallback is invoked when a row is deleted, with compile-time type safety.
type TypedDeleteCallback[T any] func(row T)

// TypedUpdateCallback is invoked when a row is updated, with compile-time type safety.
type TypedUpdateCallback[T any] func(oldRow T, newRow T)

// TypedEventCallback is invoked when an ephemeral event row is delivered for an
// event table, with compile-time type safety.
type TypedEventCallback[T any] func(row T)
