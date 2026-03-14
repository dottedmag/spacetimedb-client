// Package cache provides a client-side row cache that automatically mirrors
// subscribed SpacetimeDB tables in local memory.
//
// # Table definitions
//
// Every cached table requires a [TableDef] implementation that tells the cache
// how to serialize and deserialize rows:
//
//	type TableDef interface {
//	    TableName() string
//	    DecodeRow(r bsatn.Reader) (any, error)
//	    EncodeRow(row any) []byte
//	}
//
// Tables that have a primary key should also implement [TableDefWithPK].
// This enables the cache to detect updates by matching deletes and inserts
// that share the same primary key within a single transaction, and to fire
// [UpdateCallback] instead of separate delete and insert callbacks.
//
// # ClientCache and TableCache
//
// [NewClientCache] returns a [ClientCache] that manages the full set of
// registered table caches. Call [ClientCache.RegisterTable] for each table
// before the connection starts receiving data.
//
// Each registered table gets its own [TableCache]. Use [TableCache.Count] to
// read the current row count and [TableCache.Iter] to walk all cached rows.
//
// # Change callbacks
//
// [TableCache] supports three kinds of change callbacks:
//
//   - [TableCache.OnInsert] fires when a new row is added.
//   - [TableCache.OnDelete] fires when a row is removed.
//   - [TableCache.OnUpdate] fires when a row with a matching primary key is
//     deleted and re-inserted within the same transaction (requires
//     [TableDefWithPK]).
//
// Each On* method returns a [CallbackID] that can be passed to
// [TableCache.RemoveCallback] to unregister the callback.
//
// # Row storage
//
// Rows are keyed internally by their raw BSATN byte representation and
// decoded through the registered [TableDef].
//
// # Example
//
// Define a table, register it, and listen for changes:
//
//	type Player struct {
//	    ID   uint64
//	    Name string
//	}
//
//	type playerDef struct{}
//
//	func (playerDef) TableName() string                          { return "Player" }
//	func (playerDef) DecodeRow(r bsatn.Reader) (any, error)      { /* decode Player from BSATN */ }
//	func (playerDef) EncodeRow(row any) []byte                    { /* encode Player to BSATN */ }
//	func (playerDef) PrimaryKey(row any) any                     { return row.(Player).ID }
//
//	cc := cache.NewClientCache()
//	cc.RegisterTable(playerDef{})
//
//	tc := cc.GetTable("Player")
//	tc.OnInsert(func(row any) {
//	    p := row.(Player)
//	    fmt.Println("new player:", p.Name)
//	})
//	tc.OnUpdate(func(oldRow, newRow any) {
//	    fmt.Println("player updated:", newRow.(Player).Name)
//	})
//	fmt.Println("cached players:", tc.Count())
//	tc.Iter(func(row any) bool {
//	    fmt.Println(row.(Player).Name)
//	    return true // continue iteration
//	})
package cache
