package cache

import (
	"sync/atomic"

	"github.com/puzpuzpuz/xsync/v4"
)

// TableCache stores decoded rows for a single table, keyed by their
// BSATN-encoded bytes. It supports insert, delete, and update callbacks
// and provides row iteration via Iter.
//
// Rows are automatically managed by the [client.DbConnection] event loop
// as subscription updates arrive. Use the On* methods to observe changes
// and Iter/Count to query the current state.
//
// The ApplyInsert, ApplyDelete, and ApplyUpdate methods are intended for
// internal use by the connection event loop and should not be called
// directly by application code.
type TableCache interface {
	Count() int
	Iter(fn func(row any) bool)
	OnInsert(cb InsertCallback) CallbackID
	OnDelete(cb DeleteCallback) CallbackID
	OnUpdate(cb UpdateCallback) CallbackID
	RemoveCallback(id CallbackID)
	ApplyInsert(rowBytes []byte, row any)
	ApplyDelete(rowBytes []byte, row any)
	ApplyUpdate(oldRowBytes []byte, oldRow any, newRowBytes []byte, newRow any)
}

// newTableCache creates a new table cache.
func newTableCache(def TableDef) *tableCache {
	return &tableCache{
		def:             def,
		rows:            xsync.NewMap[string, any](),
		insertCallbacks: xsync.NewMap[CallbackID, InsertCallback](),
		deleteCallbacks: xsync.NewMap[CallbackID, DeleteCallback](),
		updateCallbacks: xsync.NewMap[CallbackID, UpdateCallback](),
	}
}

type tableCache struct {
	def             TableDef
	rows            *xsync.Map[string, any]
	insertCallbacks *xsync.Map[CallbackID, InsertCallback]
	deleteCallbacks *xsync.Map[CallbackID, DeleteCallback]
	updateCallbacks *xsync.Map[CallbackID, UpdateCallback]
	nextCallbackID  atomic.Uint64
}

func (tc *tableCache) Count() int {
	return tc.rows.Size()
}

func (tc *tableCache) Iter(fn func(row any) bool) {
	tc.rows.Range(func(key string, value any) bool {
		return fn(value)
	})
}

func (tc *tableCache) OnInsert(cb InsertCallback) CallbackID {
	id := CallbackID(tc.nextCallbackID.Add(1))
	tc.insertCallbacks.Store(id, cb)
	return id
}

func (tc *tableCache) OnDelete(cb DeleteCallback) CallbackID {
	id := CallbackID(tc.nextCallbackID.Add(1))
	tc.deleteCallbacks.Store(id, cb)
	return id
}

func (tc *tableCache) OnUpdate(cb UpdateCallback) CallbackID {
	id := CallbackID(tc.nextCallbackID.Add(1))
	tc.updateCallbacks.Store(id, cb)
	return id
}

func (tc *tableCache) RemoveCallback(id CallbackID) {
	tc.insertCallbacks.Delete(id)
	tc.deleteCallbacks.Delete(id)
	tc.updateCallbacks.Delete(id)
}

// ApplyInsert stores a row in the cache and fires all registered
// [InsertCallback] functions. This is called internally by the connection
// event loop and should not be called by application code.
func (tc *tableCache) ApplyInsert(rowBytes []byte, row any) {
	key := string(rowBytes)
	tc.rows.Store(key, row)
	tc.insertCallbacks.Range(func(_ CallbackID, cb InsertCallback) bool {
		cb(row)
		return true
	})
}

// ApplyDelete removes a row from the cache and fires all registered
// [DeleteCallback] functions. This is called internally by the connection
// event loop and should not be called by application code.
func (tc *tableCache) ApplyDelete(rowBytes []byte, row any) {
	key := string(rowBytes)
	tc.rows.Delete(key)
	tc.deleteCallbacks.Range(func(_ CallbackID, cb DeleteCallback) bool {
		cb(row)
		return true
	})
}

// ApplyUpdate replaces an old row with a new row in the cache and fires
// all registered [UpdateCallback] functions. This is called internally by
// the connection event loop and should not be called by application code.
func (tc *tableCache) ApplyUpdate(oldRowBytes []byte, oldRow any, newRowBytes []byte, newRow any) {
	tc.rows.Delete(string(oldRowBytes))
	tc.rows.Store(string(newRowBytes), newRow)
	tc.updateCallbacks.Range(func(_ CallbackID, cb UpdateCallback) bool {
		cb(oldRow, newRow)
		return true
	})
}
