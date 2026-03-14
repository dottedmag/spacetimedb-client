package cache

import (
	"github.com/puzpuzpuz/xsync/v4"
	"go.digitalxero.dev/spacetimedb-client/bsatn"
)

// ClientCache manages per-table row caches for all registered tables.
// It provides table lookup by name, row decoding, and primary key
// introspection. Create one with [NewClientCache].
type ClientCache interface {
	GetTable(name string) TableCache
	RegisterTable(def TableDef)
	DecodeRow(tableName string, r bsatn.Reader) (any, error)
	GetTableDef(tableName string) (TableDefWithPK, bool)
}

func NewClientCache() ClientCache {
	return &clientCache{
		tables: xsync.NewMap[string, *tableCache](),
		defs:   xsync.NewMap[string, TableDef](),
	}
}

type clientCache struct {
	tables *xsync.Map[string, *tableCache]
	defs   *xsync.Map[string, TableDef]
}

func (cc *clientCache) GetTable(name string) TableCache {
	tc, ok := cc.tables.Load(name)
	if !ok {
		return nil
	}
	return tc
}

func (cc *clientCache) RegisterTable(def TableDef) {
	cc.tables.Store(def.TableName(), newTableCache(def))
	cc.defs.Store(def.TableName(), def)
}

func (cc *clientCache) DecodeRow(tableName string, r bsatn.Reader) (any, error) {
	def, ok := cc.defs.Load(tableName)
	if !ok {
		return nil, nil
	}
	return def.DecodeRow(r)
}

func (cc *clientCache) GetTableDef(tableName string) (TableDefWithPK, bool) {
	def, ok := cc.defs.Load(tableName)
	if !ok {
		return nil, false
	}
	pkDef, hasPK := def.(TableDefWithPK)
	return pkDef, hasPK
}

// RegisterTypedTable registers a generic table definition and returns a typed cache handle.
// This is the primary API for generated client code to register tables without primary keys.
func RegisterTypedTable[T any](cc ClientCache, def TypedTableDef[T]) *TypedTableCache[T] {
	cc.RegisterTable(TypedTableBridge[T]{Def: def})
	return NewTypedTableCache[T](cc.GetTable(def.TableName()))
}

// RegisterTypedTableWithPK registers a generic table definition with a primary key
// and returns a typed cache handle. Tables with primary keys support update detection
// and fire UpdateCallback instead of separate delete+insert callbacks.
func RegisterTypedTableWithPK[T any, K comparable](cc ClientCache, def TypedTableDefWithPK[T, K]) *TypedTableCache[T] {
	cc.RegisterTable(TypedTableBridgeWithPK[T, K]{Def: def})
	return NewTypedTableCache[T](cc.GetTable(def.TableName()))
}
