package schema

import (
	"go.digitalxero.dev/spacetimedb-client/bsatn"
	"go.digitalxero.dev/spacetimedb-client/client/cache"
	"go.digitalxero.dev/spacetimedb-client/types"
)

// --- LargeTable ---

// LargeTable has 30+ fields of various types.
type LargeTable struct {
	A uint8
	B uint16
	C uint32
	D uint64
	E types.Uint128
	F types.Uint256
	G int8
	H int16
	I int32
	J int64
	K types.Int128
	L types.Int256
	M bool
	N float32
	O float64
	P string
	Q SimpleEnum
	R EnumWithPayload
	S UnitStruct
	T ByteStruct
	U EveryPrimitiveStruct
	V EveryVecStruct
}

func (t *LargeTable) WriteBsatn(w bsatn.Writer) {
	w.PutU8(t.A)
	w.PutU16(t.B)
	w.PutU32(t.C)
	w.PutU64(t.D)
	t.E.WriteBsatn(w)
	t.F.WriteBsatn(w)
	w.PutI8(t.G)
	w.PutI16(t.H)
	w.PutI32(t.I)
	w.PutI64(t.J)
	t.K.WriteBsatn(w)
	t.L.WriteBsatn(w)
	w.PutBool(t.M)
	w.PutF32(t.N)
	w.PutF64(t.O)
	w.PutString(t.P)
	t.Q.WriteBsatn(w)
	t.R.WriteBsatn(w)
	t.S.WriteBsatn(w)
	t.T.WriteBsatn(w)
	t.U.WriteBsatn(w)
	t.V.WriteBsatn(w)
}

func ReadLargeTable(r bsatn.Reader) (*LargeTable, error) {
	var t LargeTable
	var err error
	if t.A, err = r.GetU8(); err != nil {
		return nil, err
	}
	if t.B, err = r.GetU16(); err != nil {
		return nil, err
	}
	if t.C, err = r.GetU32(); err != nil {
		return nil, err
	}
	if t.D, err = r.GetU64(); err != nil {
		return nil, err
	}
	if t.E, err = types.ReadUint128(r); err != nil {
		return nil, err
	}
	if t.F, err = types.ReadUint256(r); err != nil {
		return nil, err
	}
	if t.G, err = r.GetI8(); err != nil {
		return nil, err
	}
	if t.H, err = r.GetI16(); err != nil {
		return nil, err
	}
	if t.I, err = r.GetI32(); err != nil {
		return nil, err
	}
	if t.J, err = r.GetI64(); err != nil {
		return nil, err
	}
	if t.K, err = types.ReadInt128(r); err != nil {
		return nil, err
	}
	if t.L, err = types.ReadInt256(r); err != nil {
		return nil, err
	}
	if t.M, err = r.GetBool(); err != nil {
		return nil, err
	}
	if t.N, err = r.GetF32(); err != nil {
		return nil, err
	}
	if t.O, err = r.GetF64(); err != nil {
		return nil, err
	}
	if t.P, err = r.GetString(); err != nil {
		return nil, err
	}
	if t.Q, err = ReadSimpleEnum(r); err != nil {
		return nil, err
	}
	if t.R, err = ReadEnumWithPayload(r); err != nil {
		return nil, err
	}
	var sPtr *UnitStruct
	if sPtr, err = ReadUnitStruct(r); err != nil {
		return nil, err
	}
	t.S = *sPtr
	var tPtr *ByteStruct
	if tPtr, err = ReadByteStruct(r); err != nil {
		return nil, err
	}
	t.T = *tPtr
	var uPtr *EveryPrimitiveStruct
	if uPtr, err = ReadEveryPrimitiveStruct(r); err != nil {
		return nil, err
	}
	t.U = *uPtr
	var vPtr *EveryVecStruct
	if vPtr, err = ReadEveryVecStruct(r); err != nil {
		return nil, err
	}
	t.V = *vPtr
	return &t, nil
}

var LargeTableDef cache.TableDef = newTableDef[LargeTable]("large_table",
	func(w bsatn.Writer, row *LargeTable) { row.WriteBsatn(w) },
	func(r bsatn.Reader) (*LargeTable, error) { return ReadLargeTable(r) },
)

// --- Users ---

// UsersRow represents a row in the users table.
type UsersRow struct {
	Identity types.Identity
	Name     string
	Online   bool
}

func (u *UsersRow) WriteBsatn(w bsatn.Writer) {
	u.Identity.WriteBsatn(w)
	w.PutString(u.Name)
	w.PutBool(u.Online)
}

func ReadUsersRow(r bsatn.Reader) (*UsersRow, error) {
	var u UsersRow
	var err error
	if u.Identity, err = types.ReadIdentity(r); err != nil {
		return nil, err
	}
	if u.Name, err = r.GetString(); err != nil {
		return nil, err
	}
	if u.Online, err = r.GetBool(); err != nil {
		return nil, err
	}
	return &u, nil
}

var UsersDef cache.TableDefWithPK = &usersTableDef{}

type usersTableDef struct{}

func (t *usersTableDef) TableName() string { return "users" }
func (t *usersTableDef) DecodeRow(r bsatn.Reader) (any, error) {
	return ReadUsersRow(r)
}
func (t *usersTableDef) EncodeRow(row any) []byte {
	w := bsatn.NewWriter(64)
	row.(*UsersRow).WriteBsatn(w)
	return w.Bytes()
}
func (t *usersTableDef) PrimaryKey(row any) any {
	return row.(*UsersRow).Identity
}

// --- BTreeU32 ---

// BTreeU32Row represents a row in the btree_u32 table.
type BTreeU32Row = KeyDataRow[uint32]

var BTreeU32Def cache.TableDef = newTableDef[BTreeU32Row]("btree_u_32",
	func(w bsatn.Writer, row *BTreeU32Row) { encodeKeyData(w, row, encodeU32) },
	func(r bsatn.Reader) (*BTreeU32Row, error) { return decodeKeyData(r, decodeU32) },
)

// --- IndexedTable ---

// IndexedTableRow represents a row in an indexed table with id, key, data.
type IndexedTableRow struct {
	ID   uint32
	Key  uint32
	Data int32
}

func (t *IndexedTableRow) WriteBsatn(w bsatn.Writer) {
	w.PutU32(t.ID)
	w.PutU32(t.Key)
	w.PutI32(t.Data)
}

func ReadIndexedTableRow(r bsatn.Reader) (*IndexedTableRow, error) {
	var row IndexedTableRow
	var err error
	if row.ID, err = r.GetU32(); err != nil {
		return nil, err
	}
	if row.Key, err = r.GetU32(); err != nil {
		return nil, err
	}
	if row.Data, err = r.GetI32(); err != nil {
		return nil, err
	}
	return &row, nil
}

var IndexedTableDef cache.TableDefWithPK = &indexedTableDef{}

type indexedTableDef struct{}

func (t *indexedTableDef) TableName() string { return "indexed_table" }
func (t *indexedTableDef) DecodeRow(r bsatn.Reader) (any, error) {
	return ReadIndexedTableRow(r)
}
func (t *indexedTableDef) EncodeRow(row any) []byte {
	w := bsatn.NewWriter(12)
	row.(*IndexedTableRow).WriteBsatn(w)
	return w.Bytes()
}
func (t *indexedTableDef) PrimaryKey(row any) any {
	return row.(*IndexedTableRow).ID
}

// --- TableHoldsTable ---

// TableHoldsTableRow has two embedded OneU8 rows.
type TableHoldsTableRow struct {
	A Row[uint8]
	B Row[uint8]
}

func (t *TableHoldsTableRow) WriteBsatn(w bsatn.Writer) {
	w.PutU8(t.A.Value)
	w.PutU8(t.B.Value)
}

func ReadTableHoldsTableRow(r bsatn.Reader) (*TableHoldsTableRow, error) {
	var row TableHoldsTableRow
	var err error
	if row.A.Value, err = r.GetU8(); err != nil {
		return nil, err
	}
	if row.B.Value, err = r.GetU8(); err != nil {
		return nil, err
	}
	return &row, nil
}

var TableHoldsTableDef cache.TableDef = newTableDef[TableHoldsTableRow]("table_holds_table",
	func(w bsatn.Writer, row *TableHoldsTableRow) { row.WriteBsatn(w) },
	func(r bsatn.Reader) (*TableHoldsTableRow, error) { return ReadTableHoldsTableRow(r) },
)
