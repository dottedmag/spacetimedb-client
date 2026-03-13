package schema

import (
	"go.digitalxero.dev/spacetimedb-client/bsatn"
	"go.digitalxero.dev/spacetimedb-client/types"
)

// --- SimpleEnum ---

// SimpleEnum is a sum type with unit variants.
type SimpleEnum uint8

const (
	SimpleEnumZero SimpleEnum = 0
	SimpleEnumOne  SimpleEnum = 1
	SimpleEnumTwo  SimpleEnum = 2
)

func (s SimpleEnum) WriteBsatn(w bsatn.Writer) {
	w.PutSumTag(uint8(s))
}

func ReadSimpleEnum(r bsatn.Reader) (SimpleEnum, error) {
	tag, err := r.GetSumTag()
	if err != nil {
		return 0, err
	}
	if tag > 2 {
		return 0, &bsatn.ErrInvalidTag{Tag: tag, SumName: "SimpleEnum"}
	}
	return SimpleEnum(tag), nil
}

func encodeSimpleEnum(w bsatn.Writer, v SimpleEnum) { w.PutSumTag(uint8(v)) }
func decodeSimpleEnum(r bsatn.Reader) (SimpleEnum, error) {
	return ReadSimpleEnum(r)
}

// --- EnumWithPayload ---

// EnumWithPayload is a sum type with typed variants.
type EnumWithPayload interface {
	bsatn.Serializable
	enumWithPayloadTag() uint8
}

type enumU8 struct{ V uint8 }
type enumU16 struct{ V uint16 }
type enumU32 struct{ V uint32 }
type enumU64 struct{ V uint64 }
type enumU128 struct{ V types.Uint128 }
type enumU256 struct{ V types.Uint256 }
type enumI8 struct{ V int8 }
type enumI16 struct{ V int16 }
type enumI32 struct{ V int32 }
type enumI64 struct{ V int64 }
type enumI128 struct{ V types.Int128 }
type enumI256 struct{ V types.Int256 }
type enumBool struct{ V bool }
type enumF32 struct{ V float32 }
type enumF64 struct{ V float64 }
type enumString struct{ V string }
type enumIdentity struct{ V types.Identity }
type enumConnectionId struct{ V types.ConnectionId }
type enumUuid struct{ V types.Uuid }
type enumTimestamp struct{ V types.Timestamp }
type enumSimpleEnum struct{ V SimpleEnum }
type enumUnitStruct struct{}
type enumByteStruct struct{ V *ByteStruct }
type enumEveryPrimitiveStruct struct{ V *EveryPrimitiveStruct }
type enumEveryVecStruct struct{ V *EveryVecStruct }

func (e *enumU8) enumWithPayloadTag() uint8                  { return 0 }
func (e *enumU16) enumWithPayloadTag() uint8                 { return 1 }
func (e *enumU32) enumWithPayloadTag() uint8                 { return 2 }
func (e *enumU64) enumWithPayloadTag() uint8                 { return 3 }
func (e *enumU128) enumWithPayloadTag() uint8                { return 4 }
func (e *enumU256) enumWithPayloadTag() uint8                { return 5 }
func (e *enumI8) enumWithPayloadTag() uint8                  { return 6 }
func (e *enumI16) enumWithPayloadTag() uint8                 { return 7 }
func (e *enumI32) enumWithPayloadTag() uint8                 { return 8 }
func (e *enumI64) enumWithPayloadTag() uint8                 { return 9 }
func (e *enumI128) enumWithPayloadTag() uint8                { return 10 }
func (e *enumI256) enumWithPayloadTag() uint8                { return 11 }
func (e *enumBool) enumWithPayloadTag() uint8                { return 12 }
func (e *enumF32) enumWithPayloadTag() uint8                 { return 13 }
func (e *enumF64) enumWithPayloadTag() uint8                 { return 14 }
func (e *enumString) enumWithPayloadTag() uint8              { return 15 }
func (e *enumIdentity) enumWithPayloadTag() uint8            { return 16 }
func (e *enumConnectionId) enumWithPayloadTag() uint8        { return 17 }
func (e *enumUuid) enumWithPayloadTag() uint8                { return 18 }
func (e *enumTimestamp) enumWithPayloadTag() uint8           { return 19 }
func (e *enumSimpleEnum) enumWithPayloadTag() uint8          { return 20 }
func (e *enumUnitStruct) enumWithPayloadTag() uint8          { return 21 }
func (e *enumByteStruct) enumWithPayloadTag() uint8          { return 22 }
func (e *enumEveryPrimitiveStruct) enumWithPayloadTag() uint8 { return 23 }
func (e *enumEveryVecStruct) enumWithPayloadTag() uint8      { return 24 }

func (e *enumU8) WriteBsatn(w bsatn.Writer)   { w.PutSumTag(0); w.PutU8(e.V) }
func (e *enumU16) WriteBsatn(w bsatn.Writer)  { w.PutSumTag(1); w.PutU16(e.V) }
func (e *enumU32) WriteBsatn(w bsatn.Writer)  { w.PutSumTag(2); w.PutU32(e.V) }
func (e *enumU64) WriteBsatn(w bsatn.Writer)  { w.PutSumTag(3); w.PutU64(e.V) }
func (e *enumU128) WriteBsatn(w bsatn.Writer) { w.PutSumTag(4); e.V.WriteBsatn(w) }
func (e *enumU256) WriteBsatn(w bsatn.Writer) { w.PutSumTag(5); e.V.WriteBsatn(w) }
func (e *enumI8) WriteBsatn(w bsatn.Writer)   { w.PutSumTag(6); w.PutI8(e.V) }
func (e *enumI16) WriteBsatn(w bsatn.Writer)  { w.PutSumTag(7); w.PutI16(e.V) }
func (e *enumI32) WriteBsatn(w bsatn.Writer)  { w.PutSumTag(8); w.PutI32(e.V) }
func (e *enumI64) WriteBsatn(w bsatn.Writer)  { w.PutSumTag(9); w.PutI64(e.V) }
func (e *enumI128) WriteBsatn(w bsatn.Writer) { w.PutSumTag(10); e.V.WriteBsatn(w) }
func (e *enumI256) WriteBsatn(w bsatn.Writer) { w.PutSumTag(11); e.V.WriteBsatn(w) }
func (e *enumBool) WriteBsatn(w bsatn.Writer) { w.PutSumTag(12); w.PutBool(e.V) }
func (e *enumF32) WriteBsatn(w bsatn.Writer)  { w.PutSumTag(13); w.PutF32(e.V) }
func (e *enumF64) WriteBsatn(w bsatn.Writer)  { w.PutSumTag(14); w.PutF64(e.V) }
func (e *enumString) WriteBsatn(w bsatn.Writer) {
	w.PutSumTag(15)
	w.PutString(e.V)
}
func (e *enumIdentity) WriteBsatn(w bsatn.Writer) {
	w.PutSumTag(16)
	e.V.WriteBsatn(w)
}
func (e *enumConnectionId) WriteBsatn(w bsatn.Writer) {
	w.PutSumTag(17)
	e.V.WriteBsatn(w)
}
func (e *enumUuid) WriteBsatn(w bsatn.Writer) {
	w.PutSumTag(18)
	e.V.WriteBsatn(w)
}
func (e *enumTimestamp) WriteBsatn(w bsatn.Writer) {
	w.PutSumTag(19)
	e.V.WriteBsatn(w)
}
func (e *enumSimpleEnum) WriteBsatn(w bsatn.Writer) {
	w.PutSumTag(20)
	e.V.WriteBsatn(w)
}
func (e *enumUnitStruct) WriteBsatn(w bsatn.Writer) {
	w.PutSumTag(21)
}
func (e *enumByteStruct) WriteBsatn(w bsatn.Writer) {
	w.PutSumTag(22)
	e.V.WriteBsatn(w)
}
func (e *enumEveryPrimitiveStruct) WriteBsatn(w bsatn.Writer) {
	w.PutSumTag(23)
	e.V.WriteBsatn(w)
}
func (e *enumEveryVecStruct) WriteBsatn(w bsatn.Writer) {
	w.PutSumTag(24)
	e.V.WriteBsatn(w)
}

// Constructors for EnumWithPayload variants.
func NewEnumU8(v uint8) EnumWithPayload                                   { return &enumU8{V: v} }
func NewEnumU16(v uint16) EnumWithPayload                                 { return &enumU16{V: v} }
func NewEnumU32(v uint32) EnumWithPayload                                 { return &enumU32{V: v} }
func NewEnumU64(v uint64) EnumWithPayload                                 { return &enumU64{V: v} }
func NewEnumU128(v types.Uint128) EnumWithPayload                         { return &enumU128{V: v} }
func NewEnumU256(v types.Uint256) EnumWithPayload                         { return &enumU256{V: v} }
func NewEnumI8(v int8) EnumWithPayload                                    { return &enumI8{V: v} }
func NewEnumI16(v int16) EnumWithPayload                                  { return &enumI16{V: v} }
func NewEnumI32(v int32) EnumWithPayload                                  { return &enumI32{V: v} }
func NewEnumI64(v int64) EnumWithPayload                                  { return &enumI64{V: v} }
func NewEnumI128(v types.Int128) EnumWithPayload                          { return &enumI128{V: v} }
func NewEnumI256(v types.Int256) EnumWithPayload                          { return &enumI256{V: v} }
func NewEnumBool(v bool) EnumWithPayload                                  { return &enumBool{V: v} }
func NewEnumF32(v float32) EnumWithPayload                                { return &enumF32{V: v} }
func NewEnumF64(v float64) EnumWithPayload                                { return &enumF64{V: v} }
func NewEnumString(v string) EnumWithPayload                              { return &enumString{V: v} }
func NewEnumIdentity(v types.Identity) EnumWithPayload                    { return &enumIdentity{V: v} }
func NewEnumConnectionId(v types.ConnectionId) EnumWithPayload            { return &enumConnectionId{V: v} }
func NewEnumUuid(v types.Uuid) EnumWithPayload                            { return &enumUuid{V: v} }
func NewEnumTimestamp(v types.Timestamp) EnumWithPayload                  { return &enumTimestamp{V: v} }
func NewEnumSimpleEnum(v SimpleEnum) EnumWithPayload                      { return &enumSimpleEnum{V: v} }
func NewEnumUnitStruct() EnumWithPayload                                  { return &enumUnitStruct{} }
func NewEnumByteStruct(v *ByteStruct) EnumWithPayload                     { return &enumByteStruct{V: v} }
func NewEnumEveryPrimitiveStruct(v *EveryPrimitiveStruct) EnumWithPayload { return &enumEveryPrimitiveStruct{V: v} }
func NewEnumEveryVecStruct(v *EveryVecStruct) EnumWithPayload             { return &enumEveryVecStruct{V: v} }

// ReadEnumWithPayload reads an EnumWithPayload from a BSATN reader.
func ReadEnumWithPayload(r bsatn.Reader) (EnumWithPayload, error) {
	tag, err := r.GetSumTag()
	if err != nil {
		return nil, err
	}
	switch tag {
	case 0:
		v, err := r.GetU8()
		if err != nil {
			return nil, err
		}
		return &enumU8{V: v}, nil
	case 1:
		v, err := r.GetU16()
		if err != nil {
			return nil, err
		}
		return &enumU16{V: v}, nil
	case 2:
		v, err := r.GetU32()
		if err != nil {
			return nil, err
		}
		return &enumU32{V: v}, nil
	case 3:
		v, err := r.GetU64()
		if err != nil {
			return nil, err
		}
		return &enumU64{V: v}, nil
	case 4:
		v, err := types.ReadUint128(r)
		if err != nil {
			return nil, err
		}
		return &enumU128{V: v}, nil
	case 5:
		v, err := types.ReadUint256(r)
		if err != nil {
			return nil, err
		}
		return &enumU256{V: v}, nil
	case 6:
		v, err := r.GetI8()
		if err != nil {
			return nil, err
		}
		return &enumI8{V: v}, nil
	case 7:
		v, err := r.GetI16()
		if err != nil {
			return nil, err
		}
		return &enumI16{V: v}, nil
	case 8:
		v, err := r.GetI32()
		if err != nil {
			return nil, err
		}
		return &enumI32{V: v}, nil
	case 9:
		v, err := r.GetI64()
		if err != nil {
			return nil, err
		}
		return &enumI64{V: v}, nil
	case 10:
		v, err := types.ReadInt128(r)
		if err != nil {
			return nil, err
		}
		return &enumI128{V: v}, nil
	case 11:
		v, err := types.ReadInt256(r)
		if err != nil {
			return nil, err
		}
		return &enumI256{V: v}, nil
	case 12:
		v, err := r.GetBool()
		if err != nil {
			return nil, err
		}
		return &enumBool{V: v}, nil
	case 13:
		v, err := r.GetF32()
		if err != nil {
			return nil, err
		}
		return &enumF32{V: v}, nil
	case 14:
		v, err := r.GetF64()
		if err != nil {
			return nil, err
		}
		return &enumF64{V: v}, nil
	case 15:
		v, err := r.GetString()
		if err != nil {
			return nil, err
		}
		return &enumString{V: v}, nil
	case 16:
		v, err := types.ReadIdentity(r)
		if err != nil {
			return nil, err
		}
		return &enumIdentity{V: v}, nil
	case 17:
		v, err := types.ReadConnectionId(r)
		if err != nil {
			return nil, err
		}
		return &enumConnectionId{V: v}, nil
	case 18:
		v, err := types.ReadUuid(r)
		if err != nil {
			return nil, err
		}
		return &enumUuid{V: v}, nil
	case 19:
		v, err := types.ReadTimestamp(r)
		if err != nil {
			return nil, err
		}
		return &enumTimestamp{V: v}, nil
	case 20:
		v, err := ReadSimpleEnum(r)
		if err != nil {
			return nil, err
		}
		return &enumSimpleEnum{V: v}, nil
	case 21:
		return &enumUnitStruct{}, nil
	case 22:
		v, err := ReadByteStruct(r)
		if err != nil {
			return nil, err
		}
		return &enumByteStruct{V: v}, nil
	case 23:
		v, err := ReadEveryPrimitiveStruct(r)
		if err != nil {
			return nil, err
		}
		return &enumEveryPrimitiveStruct{V: v}, nil
	case 24:
		v, err := ReadEveryVecStruct(r)
		if err != nil {
			return nil, err
		}
		return &enumEveryVecStruct{V: v}, nil
	default:
		return nil, &bsatn.ErrInvalidTag{Tag: tag, SumName: "EnumWithPayload"}
	}
}

func encodeEnumWithPayload(w bsatn.Writer, v EnumWithPayload) {
	v.WriteBsatn(w)
}

func decodeEnumWithPayload(r bsatn.Reader) (EnumWithPayload, error) {
	return ReadEnumWithPayload(r)
}
