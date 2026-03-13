package schema

import (
	"go.digitalxero.dev/spacetimedb-client/bsatn"
	"go.digitalxero.dev/spacetimedb-client/client/cache"
	"go.digitalxero.dev/spacetimedb-client/types"
)

// EncoderFunc writes a value of type T to a BSATN writer.
type EncoderFunc[T any] func(w bsatn.Writer, v T)

// DecoderFunc reads a value of type T from a BSATN reader.
type DecoderFunc[T any] func(r bsatn.Reader) (T, error)

// Row is a generic single-field row (for One*, Vec*, Option* tables).
type Row[T any] struct {
	Value T
}

// KeyDataRow is a generic two-field row (for Unique* and Pk* tables).
type KeyDataRow[K any] struct {
	Key  K
	Data int32
}

// tableDef implements cache.TableDef generically.
type tableDef[T any] struct {
	name   string
	encode EncoderFunc[*T]
	decode DecoderFunc[*T]
}

func newTableDef[T any](name string, encode EncoderFunc[*T], decode DecoderFunc[*T]) cache.TableDef {
	return &tableDef[T]{name: name, encode: encode, decode: decode}
}

func (t *tableDef[T]) TableName() string { return t.name }

func (t *tableDef[T]) DecodeRow(r bsatn.Reader) (any, error) {
	return t.decode(r)
}

func (t *tableDef[T]) EncodeRow(row any) []byte {
	w := bsatn.NewWriter(64)
	t.encode(w, row.(*T))
	return w.Bytes()
}

// tableDefWithPK implements cache.TableDefWithPK generically.
type tableDefWithPK[K comparable] struct {
	tableDef[KeyDataRow[K]]
	extractPK func(row *KeyDataRow[K]) any
}

func newTableDefWithPK[K comparable](
	name string,
	encode EncoderFunc[*KeyDataRow[K]],
	decode DecoderFunc[*KeyDataRow[K]],
	extractPK func(*KeyDataRow[K]) any,
) cache.TableDefWithPK {
	return &tableDefWithPK[K]{
		tableDef:  tableDef[KeyDataRow[K]]{name: name, encode: encode, decode: decode},
		extractPK: extractPK,
	}
}

func (t *tableDefWithPK[K]) PrimaryKey(row any) any {
	return t.extractPK(row.(*KeyDataRow[K]))
}

// args implements bsatn.Serializable for single-value reducer args.
type args[T any] struct {
	value  T
	encode EncoderFunc[T]
}

func newArgs[T any](v T, enc EncoderFunc[T]) bsatn.Serializable {
	return &args[T]{value: v, encode: enc}
}

func (a *args[T]) WriteBsatn(w bsatn.Writer) {
	a.encode(w, a.value)
}

// multiArgs implements Serializable for multi-field reducer args.
type multiArgs struct {
	data []byte
}

func (m *multiArgs) WriteBsatn(w bsatn.Writer) {
	w.PutBytes(m.data)
}

// emptyArgs implements Serializable for reducers with no arguments.
type emptyArgs struct{}

func (e *emptyArgs) WriteBsatn(_ bsatn.Writer) {}

// --- Primitive encode/decode helpers ---

func encodeU8(w bsatn.Writer, v uint8)       { w.PutU8(v) }
func decodeU8(r bsatn.Reader) (uint8, error)  { return r.GetU8() }
func encodeU16(w bsatn.Writer, v uint16)      { w.PutU16(v) }
func decodeU16(r bsatn.Reader) (uint16, error) { return r.GetU16() }
func encodeU32(w bsatn.Writer, v uint32)      { w.PutU32(v) }
func decodeU32(r bsatn.Reader) (uint32, error) { return r.GetU32() }
func encodeU64(w bsatn.Writer, v uint64)      { w.PutU64(v) }
func decodeU64(r bsatn.Reader) (uint64, error) { return r.GetU64() }
func encodeI8(w bsatn.Writer, v int8)         { w.PutI8(v) }
func decodeI8(r bsatn.Reader) (int8, error)    { return r.GetI8() }
func encodeI16(w bsatn.Writer, v int16)       { w.PutI16(v) }
func decodeI16(r bsatn.Reader) (int16, error)  { return r.GetI16() }
func encodeI32(w bsatn.Writer, v int32)       { w.PutI32(v) }
func decodeI32(r bsatn.Reader) (int32, error)  { return r.GetI32() }
func encodeI64(w bsatn.Writer, v int64)       { w.PutI64(v) }
func decodeI64(r bsatn.Reader) (int64, error)  { return r.GetI64() }
func encodeBool(w bsatn.Writer, v bool)       { w.PutBool(v) }
func decodeBool(r bsatn.Reader) (bool, error)  { return r.GetBool() }
func encodeF32(w bsatn.Writer, v float32)     { w.PutF32(v) }
func decodeF32(r bsatn.Reader) (float32, error) { return r.GetF32() }
func encodeF64(w bsatn.Writer, v float64)     { w.PutF64(v) }
func decodeF64(r bsatn.Reader) (float64, error) { return r.GetF64() }
func encodeString(w bsatn.Writer, v string)   { w.PutString(v) }
func decodeString(r bsatn.Reader) (string, error) { return r.GetString() }

func encodeIdentity(w bsatn.Writer, v types.Identity)         { v.WriteBsatn(w) }
func decodeIdentity(r bsatn.Reader) (types.Identity, error)    { return types.ReadIdentity(r) }
func encodeConnectionId(w bsatn.Writer, v types.ConnectionId)  { v.WriteBsatn(w) }
func decodeConnectionId(r bsatn.Reader) (types.ConnectionId, error) { return types.ReadConnectionId(r) }
func encodeUuid(w bsatn.Writer, v types.Uuid)                 { v.WriteBsatn(w) }
func decodeUuid(r bsatn.Reader) (types.Uuid, error)            { return types.ReadUuid(r) }
func encodeTimestamp(w bsatn.Writer, v types.Timestamp)            { v.WriteBsatn(w) }
func decodeTimestamp(r bsatn.Reader) (types.Timestamp, error)     { return types.ReadTimestamp(r) }
func encodeTimeDuration(w bsatn.Writer, v types.TimeDuration)     { v.WriteBsatn(w) }
func decodeTimeDuration(r bsatn.Reader) (types.TimeDuration, error) { return types.ReadTimeDuration(r) }
func encodeUint128(w bsatn.Writer, v types.Uint128)            { v.WriteBsatn(w) }
func decodeUint128(r bsatn.Reader) (types.Uint128, error)       { return types.ReadUint128(r) }
func encodeInt128(w bsatn.Writer, v types.Int128)              { v.WriteBsatn(w) }
func decodeInt128(r bsatn.Reader) (types.Int128, error)         { return types.ReadInt128(r) }
func encodeUint256(w bsatn.Writer, v types.Uint256)            { v.WriteBsatn(w) }
func decodeUint256(r bsatn.Reader) (types.Uint256, error)       { return types.ReadUint256(r) }
func encodeInt256(w bsatn.Writer, v types.Int256)              { v.WriteBsatn(w) }
func decodeInt256(r bsatn.Reader) (types.Int256, error)         { return types.ReadInt256(r) }

// --- Row encode/decode helpers ---

func encodeRow[T any](w bsatn.Writer, row *Row[T], enc EncoderFunc[T]) {
	enc(w, row.Value)
}

func decodeRow[T any](r bsatn.Reader, dec DecoderFunc[T]) (*Row[T], error) {
	v, err := dec(r)
	if err != nil {
		return nil, err
	}
	return &Row[T]{Value: v}, nil
}

func encodeKeyData[K any](w bsatn.Writer, row *KeyDataRow[K], enc EncoderFunc[K]) {
	enc(w, row.Key)
	w.PutI32(row.Data)
}

func decodeKeyData[K any](r bsatn.Reader, dec DecoderFunc[K]) (*KeyDataRow[K], error) {
	k, err := dec(r)
	if err != nil {
		return nil, err
	}
	d, err := r.GetI32()
	if err != nil {
		return nil, err
	}
	return &KeyDataRow[K]{Key: k, Data: d}, nil
}

// --- Vector encode/decode helpers ---

func encodeVec[T any](w bsatn.Writer, items []T, enc EncoderFunc[T]) {
	w.PutArrayLen(uint32(len(items)))
	for _, item := range items {
		enc(w, item)
	}
}

func decodeVec[T any](r bsatn.Reader, dec DecoderFunc[T]) ([]T, error) {
	n, err := r.GetArrayLen()
	if err != nil {
		return nil, err
	}
	items := make([]T, 0, n)
	for i := uint32(0); i < n; i++ {
		v, err := dec(r)
		if err != nil {
			return nil, err
		}
		items = append(items, v)
	}
	return items, nil
}

// --- Option encode/decode helpers ---

func encodeOption[T any](w bsatn.Writer, v *T, enc EncoderFunc[T]) {
	if v != nil {
		w.PutSumTag(0)
		enc(w, *v)
	} else {
		w.PutSumTag(1)
	}
}

func decodeOption[T any](r bsatn.Reader, dec DecoderFunc[T]) (*T, error) {
	tag, err := r.GetSumTag()
	if err != nil {
		return nil, err
	}
	switch tag {
	case 0:
		v, err := dec(r)
		if err != nil {
			return nil, err
		}
		return &v, nil
	case 1:
		return nil, nil
	default:
		return nil, &bsatn.ErrInvalidTag{Tag: tag, SumName: "Option"}
	}
}
