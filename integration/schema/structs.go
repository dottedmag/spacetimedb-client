package schema

import (
	"go.digitalxero.dev/spacetimedb-client/bsatn"
	"go.digitalxero.dev/spacetimedb-client/types"
)

// --- UnitStruct ---

// UnitStruct is an empty struct. BSATN encoding is zero bytes.
type UnitStruct struct{}

func (u *UnitStruct) WriteBsatn(_ bsatn.Writer) {}

func ReadUnitStruct(_ bsatn.Reader) (*UnitStruct, error) {
	return &UnitStruct{}, nil
}

func encodeUnitStruct(w bsatn.Writer, v *UnitStruct) {
	v.WriteBsatn(w)
}

func decodeUnitStruct(r bsatn.Reader) (*UnitStruct, error) {
	return ReadUnitStruct(r)
}

// --- ByteStruct ---

// ByteStruct has a single uint8 field.
type ByteStruct struct {
	A uint8
}

func (b *ByteStruct) WriteBsatn(w bsatn.Writer) {
	w.PutU8(b.A)
}

func ReadByteStruct(r bsatn.Reader) (*ByteStruct, error) {
	a, err := r.GetU8()
	if err != nil {
		return nil, err
	}
	return &ByteStruct{A: a}, nil
}

func encodeByteStruct(w bsatn.Writer, v *ByteStruct) {
	v.WriteBsatn(w)
}

func decodeByteStruct(r bsatn.Reader) (*ByteStruct, error) {
	return ReadByteStruct(r)
}

// --- EveryPrimitiveStruct ---

// EveryPrimitiveStruct contains every primitive field type.
type EveryPrimitiveStruct struct {
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
	Q types.Identity
	R types.ConnectionId
	S types.Timestamp
	T types.TimeDuration
	U types.Uuid
}

func (e *EveryPrimitiveStruct) WriteBsatn(w bsatn.Writer) {
	w.PutU8(e.A)
	w.PutU16(e.B)
	w.PutU32(e.C)
	w.PutU64(e.D)
	e.E.WriteBsatn(w)
	e.F.WriteBsatn(w)
	w.PutI8(e.G)
	w.PutI16(e.H)
	w.PutI32(e.I)
	w.PutI64(e.J)
	e.K.WriteBsatn(w)
	e.L.WriteBsatn(w)
	w.PutBool(e.M)
	w.PutF32(e.N)
	w.PutF64(e.O)
	w.PutString(e.P)
	e.Q.WriteBsatn(w)
	e.R.WriteBsatn(w)
	e.S.WriteBsatn(w)
	e.T.WriteBsatn(w)
	e.U.WriteBsatn(w)
}

func ReadEveryPrimitiveStruct(r bsatn.Reader) (*EveryPrimitiveStruct, error) {
	var s EveryPrimitiveStruct
	var err error
	if s.A, err = r.GetU8(); err != nil {
		return nil, err
	}
	if s.B, err = r.GetU16(); err != nil {
		return nil, err
	}
	if s.C, err = r.GetU32(); err != nil {
		return nil, err
	}
	if s.D, err = r.GetU64(); err != nil {
		return nil, err
	}
	if s.E, err = types.ReadUint128(r); err != nil {
		return nil, err
	}
	if s.F, err = types.ReadUint256(r); err != nil {
		return nil, err
	}
	if s.G, err = r.GetI8(); err != nil {
		return nil, err
	}
	if s.H, err = r.GetI16(); err != nil {
		return nil, err
	}
	if s.I, err = r.GetI32(); err != nil {
		return nil, err
	}
	if s.J, err = r.GetI64(); err != nil {
		return nil, err
	}
	if s.K, err = types.ReadInt128(r); err != nil {
		return nil, err
	}
	if s.L, err = types.ReadInt256(r); err != nil {
		return nil, err
	}
	if s.M, err = r.GetBool(); err != nil {
		return nil, err
	}
	if s.N, err = r.GetF32(); err != nil {
		return nil, err
	}
	if s.O, err = r.GetF64(); err != nil {
		return nil, err
	}
	if s.P, err = r.GetString(); err != nil {
		return nil, err
	}
	if s.Q, err = types.ReadIdentity(r); err != nil {
		return nil, err
	}
	if s.R, err = types.ReadConnectionId(r); err != nil {
		return nil, err
	}
	if s.S, err = types.ReadTimestamp(r); err != nil {
		return nil, err
	}
	if s.T, err = types.ReadTimeDuration(r); err != nil {
		return nil, err
	}
	if s.U, err = types.ReadUuid(r); err != nil {
		return nil, err
	}
	return &s, nil
}

func encodeEveryPrimitiveStruct(w bsatn.Writer, v *EveryPrimitiveStruct) {
	v.WriteBsatn(w)
}

func decodeEveryPrimitiveStruct(r bsatn.Reader) (*EveryPrimitiveStruct, error) {
	return ReadEveryPrimitiveStruct(r)
}

// --- EveryVecStruct ---

// EveryVecStruct contains a vector of every primitive type.
type EveryVecStruct struct {
	A []uint8
	B []uint16
	C []uint32
	D []uint64
	E []types.Uint128
	F []types.Uint256
	G []int8
	H []int16
	I []int32
	J []int64
	K []types.Int128
	L []types.Int256
	M []bool
	N []float32
	O []float64
	P []string
	Q []types.Identity
	R []types.ConnectionId
	S []types.Timestamp
	T []types.TimeDuration
	U []types.Uuid
}

func (e *EveryVecStruct) WriteBsatn(w bsatn.Writer) {
	// A: []uint8 - use byte array optimization
	bsatn.WriteByteArray(w, e.A)
	encodeVec(w, e.B, encodeU16)
	encodeVec(w, e.C, encodeU32)
	encodeVec(w, e.D, encodeU64)
	encodeVec(w, e.E, encodeUint128)
	encodeVec(w, e.F, encodeUint256)
	encodeVecI8(w, e.G)
	encodeVec(w, e.H, encodeI16)
	encodeVec(w, e.I, encodeI32)
	encodeVec(w, e.J, encodeI64)
	encodeVec(w, e.K, encodeInt128)
	encodeVec(w, e.L, encodeInt256)
	encodeVec(w, e.M, encodeBool)
	encodeVec(w, e.N, encodeF32)
	encodeVec(w, e.O, encodeF64)
	encodeVec(w, e.P, encodeString)
	encodeVec(w, e.Q, encodeIdentity)
	encodeVec(w, e.R, encodeConnectionId)
	encodeVec(w, e.S, encodeTimestamp)
	encodeVec(w, e.T, encodeTimeDuration)
	encodeVec(w, e.U, encodeUuid)
}

// encodeVecI8 writes a []int8 as a BSATN array.
func encodeVecI8(w bsatn.Writer, items []int8) {
	w.PutArrayLen(uint32(len(items)))
	for _, item := range items {
		w.PutI8(item)
	}
}

func ReadEveryVecStruct(r bsatn.Reader) (*EveryVecStruct, error) {
	var s EveryVecStruct
	var err error
	// A: []uint8 - use byte array optimization
	if s.A, err = bsatn.ReadByteArray(r); err != nil {
		return nil, err
	}
	if s.B, err = decodeVec(r, decodeU16); err != nil {
		return nil, err
	}
	if s.C, err = decodeVec(r, decodeU32); err != nil {
		return nil, err
	}
	if s.D, err = decodeVec(r, decodeU64); err != nil {
		return nil, err
	}
	if s.E, err = decodeVec(r, decodeUint128); err != nil {
		return nil, err
	}
	if s.F, err = decodeVec(r, decodeUint256); err != nil {
		return nil, err
	}
	if s.G, err = decodeVec(r, decodeI8); err != nil {
		return nil, err
	}
	if s.H, err = decodeVec(r, decodeI16); err != nil {
		return nil, err
	}
	if s.I, err = decodeVec(r, decodeI32); err != nil {
		return nil, err
	}
	if s.J, err = decodeVec(r, decodeI64); err != nil {
		return nil, err
	}
	if s.K, err = decodeVec(r, decodeInt128); err != nil {
		return nil, err
	}
	if s.L, err = decodeVec(r, decodeInt256); err != nil {
		return nil, err
	}
	if s.M, err = decodeVec(r, decodeBool); err != nil {
		return nil, err
	}
	if s.N, err = decodeVec(r, decodeF32); err != nil {
		return nil, err
	}
	if s.O, err = decodeVec(r, decodeF64); err != nil {
		return nil, err
	}
	if s.P, err = decodeVec(r, decodeString); err != nil {
		return nil, err
	}
	if s.Q, err = decodeVec(r, decodeIdentity); err != nil {
		return nil, err
	}
	if s.R, err = decodeVec(r, decodeConnectionId); err != nil {
		return nil, err
	}
	if s.S, err = decodeVec(r, decodeTimestamp); err != nil {
		return nil, err
	}
	if s.T, err = decodeVec(r, decodeTimeDuration); err != nil {
		return nil, err
	}
	if s.U, err = decodeVec(r, decodeUuid); err != nil {
		return nil, err
	}
	return &s, nil
}

func encodeEveryVecStruct(w bsatn.Writer, v *EveryVecStruct) {
	v.WriteBsatn(w)
}

func decodeEveryVecStruct(r bsatn.Reader) (*EveryVecStruct, error) {
	return ReadEveryVecStruct(r)
}
