package client

import (
	"github.com/stretchr/testify/require"
	"go.digitalxero.dev/spacetimedb-client/bsatn"
	"go.digitalxero.dev/spacetimedb-client/client/protocol"
	"go.digitalxero.dev/spacetimedb-client/types"
	"testing"
)

type identityKeyRow struct {
	ID   types.Identity
	Name string
}
type identityKeyDef struct{}

func (identityKeyDef) TableName() string      { return "identity_key" }
func (identityKeyDef) PrimaryKey(row any) any { return row.(*identityKeyRow).ID }
func (identityKeyDef) EncodeRow(row any) []byte {
	v := row.(*identityKeyRow)
	w := bsatn.NewWriter(64)
	v.ID.WriteBsatn(w)
	w.PutString(v.Name)
	return w.Bytes()
}
func (identityKeyDef) DecodeRow(r bsatn.Reader) (any, error) {
	id, err := types.ReadIdentity(r)
	if err != nil {
		return nil, err
	}
	name, err := r.GetString()
	if err != nil {
		return nil, err
	}
	return &identityKeyRow{id, name}, nil
}

func TestIdentityPrimaryKeyMatchesSeparateDecodedValues(t *testing.T) {
	c := newDispatchConnection()
	def := identityKeyDef{}
	c.cache.RegisterTable(def)
	tc := c.cache.GetTable(def.TableName())
	id := [32]byte{1, 2, 3}
	old := &identityKeyRow{types.NewIdentity(id), "old"}
	replacement := &identityKeyRow{types.NewIdentity(id), "new"}
	oldData, newData := def.EncodeRow(old), def.EncodeRow(replacement)
	tc.ApplyInsert(oldData, old)
	updates, inserts, deletes := 0, 0, 0
	tc.OnUpdate(func(any, any) { updates++ })
	tc.OnInsert(func(any) { inserts++ })
	tc.OnDelete(func(any) { deletes++ })
	list := func(data []byte) *protocol.BsatnRowList {
		return &protocol.BsatnRowList{SizeHint: protocol.RowOffsetsHint{Offsets: []uint64{0}}, RowsData: data}
	}
	c.applyPKUpdate(tc, def, list(newData), list(oldData))
	require.Equal(t, 1, updates)
	require.Zero(t, inserts)
	require.Zero(t, deletes)
	require.Equal(t, 1, tc.Count())
}
