package cache

import (
	"go.digitalxero.dev/spacetimedb-client/bsatn"
	"github.com/puzpuzpuz/xsync/v4"
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
