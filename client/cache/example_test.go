package cache_test

import (
	"fmt"

	"go.digitalxero.dev/spacetimedb-client/bsatn"
	"go.digitalxero.dev/spacetimedb-client/client/cache"
)

// userRow represents a table row.
type userRow struct {
	ID   uint64
	Name string
}

// userTableDef implements cache.TableDef.
type userTableDef struct{}

func (d *userTableDef) TableName() string { return "users" }

func (d *userTableDef) DecodeRow(r bsatn.Reader) (any, error) {
	id, err := r.GetU64()
	if err != nil {
		return nil, err
	}
	name, err := r.GetString()
	if err != nil {
		return nil, err
	}
	return &userRow{ID: id, Name: name}, nil
}

func (d *userTableDef) EncodeRow(row any) []byte {
	u := row.(*userRow)
	w := bsatn.NewWriter(32)
	w.PutU64(u.ID)
	w.PutString(u.Name)
	return w.Bytes()
}

func ExampleNewClientCache() {
	cc := cache.NewClientCache()
	cc.RegisterTable(&userTableDef{})
	tc := cc.GetTable("users")
	fmt.Println("table registered:", tc != nil)
	// Output:
	// table registered: true
}

func ExampleTableCache_OnInsert() {
	cc := cache.NewClientCache()
	cc.RegisterTable(&userTableDef{})
	tc := cc.GetTable("users")

	id := tc.OnInsert(func(row any) {
		u := row.(*userRow)
		fmt.Printf("inserted: %s\n", u.Name)
	})
	_ = id // can be used with tc.RemoveCallback(id)
}

func ExampleTableCache_Iter() {
	cc := cache.NewClientCache()
	cc.RegisterTable(&userTableDef{})
	tc := cc.GetTable("users")

	// In practice, rows are populated by the connection event loop.
	// Here we manually insert for demonstration.
	w := bsatn.NewWriter(32)
	w.PutU64(1)
	w.PutString("Alice")
	tc.ApplyInsert(w.Bytes(), &userRow{ID: 1, Name: "Alice"})

	tc.Iter(func(row any) bool {
		u := row.(*userRow)
		fmt.Printf("user: %s\n", u.Name)
		return true
	})
	// Output:
	// user: Alice
}
