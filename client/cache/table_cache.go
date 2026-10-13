package cache

import (
	"sync"
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
	OnEvent(cb EventCallback) CallbackID
	RemoveCallback(id CallbackID)
	ApplyInsert(rowBytes []byte, row any)
	ApplyDelete(rowBytes []byte, row any)
	ApplyUpdate(oldRowBytes []byte, oldRow any, newRowBytes []byte, newRow any)
	ApplyEvent(rowBytes []byte, row any)
}

// newTableCache creates a new table cache.
func newTableCache(def TableDef) *tableCache {
	return &tableCache{
		refs:            make(map[string]uint32),
		def:             def,
		rows:            xsync.NewMap[string, any](),
		insertCallbacks: xsync.NewMap[CallbackID, InsertCallback](),
		deleteCallbacks: xsync.NewMap[CallbackID, DeleteCallback](),
		updateCallbacks: xsync.NewMap[CallbackID, UpdateCallback](),
		eventCallbacks:  xsync.NewMap[CallbackID, EventCallback](),
	}
}

type tableCache struct {
	mu              sync.Mutex
	refs            map[string]uint32
	def             TableDef
	rows            *xsync.Map[string, any]
	insertCallbacks *xsync.Map[CallbackID, InsertCallback]
	deleteCallbacks *xsync.Map[CallbackID, DeleteCallback]
	updateCallbacks *xsync.Map[CallbackID, UpdateCallback]
	eventCallbacks  *xsync.Map[CallbackID, EventCallback]
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

func (tc *tableCache) OnEvent(cb EventCallback) CallbackID {
	id := CallbackID(tc.nextCallbackID.Add(1))
	tc.eventCallbacks.Store(id, cb)
	return id
}

func (tc *tableCache) RemoveCallback(id CallbackID) {
	tc.insertCallbacks.Delete(id)
	tc.deleteCallbacks.Delete(id)
	tc.updateCallbacks.Delete(id)
	tc.eventCallbacks.Delete(id)
}

// ApplyInsert stores a row in the cache and fires all registered
// [InsertCallback] functions. This is called internally by the connection
// event loop and should not be called by application code.
func (tc *tableCache) ApplyInsert(rowBytes []byte, row any) {
	key := string(rowBytes)
	tc.mu.Lock()
	previous := tc.refs[key]
	tc.refs[key]++
	tc.rows.Store(key, row)
	tc.mu.Unlock()
	if previous > 0 {
		return
	}
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
	tc.mu.Lock()
	if count := tc.refs[key]; count > 1 {
		tc.refs[key]--
		tc.mu.Unlock()
		return
	}
	if tc.refs[key] == 0 {
		tc.mu.Unlock()
		return
	}
	delete(tc.refs, key)
	_, visible := tc.rows.Load(key)
	tc.rows.Delete(key)
	tc.mu.Unlock()
	if !visible {
		return
	}
	tc.deleteCallbacks.Range(func(_ CallbackID, cb DeleteCallback) bool {
		cb(row)
		return true
	})
}

// ApplyUpdate replaces an old row with a new row in the cache and fires
// all registered [UpdateCallback] functions. This is called internally by
// the connection event loop and should not be called by application code.
func (tc *tableCache) ApplyUpdate(oldRowBytes []byte, oldRow any, newRowBytes []byte, newRow any) {
	oldKey, newKey := string(oldRowBytes), string(newRowBytes)
	tc.mu.Lock()
	if oldKey == newKey {
		tc.rows.Store(newKey, newRow)
		tc.mu.Unlock()
		return
	}
	if tc.refs[oldKey] > 1 {
		tc.refs[oldKey]--
	} else {
		delete(tc.refs, oldKey)
	}
	tc.rows.Delete(oldKey)
	previous := tc.refs[newKey]
	tc.refs[newKey]++
	tc.rows.Store(newKey, newRow)
	tc.mu.Unlock()
	if previous > 0 {
		return
	}
	tc.updateCallbacks.Range(func(_ CallbackID, cb UpdateCallback) bool {
		cb(oldRow, newRow)
		return true
	})
}

// ApplyEvent fires all registered [EventCallback] functions for an ephemeral
// event-table row. Unlike ApplyInsert, the row is NOT stored in the cache, so
// it never appears in Count or Iter. This is called internally by the
// connection event loop and should not be called by application code.
func (tc *tableCache) ApplyEvent(rowBytes []byte, row any) {
	tc.eventCallbacks.Range(func(_ CallbackID, cb EventCallback) bool {
		cb(row)
		return true
	})
}
