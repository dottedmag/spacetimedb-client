package cache

import (
	"go.digitalxero.dev/spacetimedb-client/bsatn"
)

// TypedTableBridge adapts a TypedTableDef[T] to the untyped TableDef interface
// for internal registration with the ClientCache.
type TypedTableBridge[T any] struct {
	Def TypedTableDef[T]
}

func (b TypedTableBridge[T]) TableName() string {
	return b.Def.TableName()
}

func (b TypedTableBridge[T]) DecodeRow(r bsatn.Reader) (any, error) {
	return b.Def.DecodeRow(r)
}

func (b TypedTableBridge[T]) EncodeRow(row any) []byte {
	return b.Def.EncodeRow(row.(T))
}

// TypedTableBridgeWithPK adapts a TypedTableDefWithPK[T, K] to the untyped
// TableDefWithPK interface for internal registration with the ClientCache.
type TypedTableBridgeWithPK[T any, K comparable] struct {
	Def TypedTableDefWithPK[T, K]
}

func (b TypedTableBridgeWithPK[T, K]) TableName() string {
	return b.Def.TableName()
}

func (b TypedTableBridgeWithPK[T, K]) DecodeRow(r bsatn.Reader) (any, error) {
	return b.Def.DecodeRow(r)
}

func (b TypedTableBridgeWithPK[T, K]) EncodeRow(row any) []byte {
	return b.Def.EncodeRow(row.(T))
}

func (b TypedTableBridgeWithPK[T, K]) PrimaryKey(row any) any {
	return b.Def.PrimaryKey(row.(T))
}
