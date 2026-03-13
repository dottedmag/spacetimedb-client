package cache

import (
	"go.digitalxero.dev/spacetimedb-client/bsatn"
	"github.com/puzpuzpuz/xsync/v4"
)

// ClientCache manages all table caches.
type ClientCache interface {
	GetTable(name string) TableCache
	RegisterTable(def TableDef)
	DecodeRow(tableName string, r bsatn.Reader) (any, error)
	GetTableDef(tableName string) (TableDefWithPK, bool)
}

func NewClientCache() ClientCache {
	return &clientCache{
		tables: xsync.NewMapOf[string, *tableCache](),
		defs:   xsync.NewMapOf[string, TableDef](),
	}
}

type clientCache struct {
	tables *xsync.MapOf[string, *tableCache]
	defs   *xsync.MapOf[string, TableDef]
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
