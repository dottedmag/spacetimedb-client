package schema

import (
	"go.digitalxero.dev/spacetimedb-client/bsatn"
	"go.digitalxero.dev/spacetimedb-client/client/cache"
	"go.digitalxero.dev/spacetimedb-client/types"
)

// Vec tables: each row contains a single slice field.

var VecU8Def cache.TableDef = newTableDef[Row[[]uint8]]("vec_u_8",
	func(w bsatn.Writer, row *Row[[]uint8]) {
		bsatn.WriteByteArray(w, row.Value)
	},
	func(r bsatn.Reader) (*Row[[]uint8], error) {
		v, err := bsatn.ReadByteArray(r)
		if err != nil {
			return nil, err
		}
		return &Row[[]uint8]{Value: v}, nil
	},
)

var VecU16Def cache.TableDef = newTableDef[Row[[]uint16]]("vec_u_16",
	func(w bsatn.Writer, row *Row[[]uint16]) { encodeVec(w, row.Value, encodeU16) },
	func(r bsatn.Reader) (*Row[[]uint16], error) {
		v, err := decodeVec(r, decodeU16)
		if err != nil {
			return nil, err
		}
		return &Row[[]uint16]{Value: v}, nil
	},
)

var VecU32Def cache.TableDef = newTableDef[Row[[]uint32]]("vec_u_32",
	func(w bsatn.Writer, row *Row[[]uint32]) { encodeVec(w, row.Value, encodeU32) },
	func(r bsatn.Reader) (*Row[[]uint32], error) {
		v, err := decodeVec(r, decodeU32)
		if err != nil {
			return nil, err
		}
		return &Row[[]uint32]{Value: v}, nil
	},
)

var VecU64Def cache.TableDef = newTableDef[Row[[]uint64]]("vec_u_64",
	func(w bsatn.Writer, row *Row[[]uint64]) { encodeVec(w, row.Value, encodeU64) },
	func(r bsatn.Reader) (*Row[[]uint64], error) {
		v, err := decodeVec(r, decodeU64)
		if err != nil {
			return nil, err
		}
		return &Row[[]uint64]{Value: v}, nil
	},
)

var VecU128Def cache.TableDef = newTableDef[Row[[]types.Uint128]]("vec_u_128",
	func(w bsatn.Writer, row *Row[[]types.Uint128]) { encodeVec(w, row.Value, encodeUint128) },
	func(r bsatn.Reader) (*Row[[]types.Uint128], error) {
		v, err := decodeVec(r, decodeUint128)
		if err != nil {
			return nil, err
		}
		return &Row[[]types.Uint128]{Value: v}, nil
	},
)

var VecU256Def cache.TableDef = newTableDef[Row[[]types.Uint256]]("vec_u_256",
	func(w bsatn.Writer, row *Row[[]types.Uint256]) { encodeVec(w, row.Value, encodeUint256) },
	func(r bsatn.Reader) (*Row[[]types.Uint256], error) {
		v, err := decodeVec(r, decodeUint256)
		if err != nil {
			return nil, err
		}
		return &Row[[]types.Uint256]{Value: v}, nil
	},
)

var VecI8Def cache.TableDef = newTableDef[Row[[]int8]]("vec_i_8",
	func(w bsatn.Writer, row *Row[[]int8]) { encodeVecI8(w, row.Value) },
	func(r bsatn.Reader) (*Row[[]int8], error) {
		v, err := decodeVec(r, decodeI8)
		if err != nil {
			return nil, err
		}
		return &Row[[]int8]{Value: v}, nil
	},
)

var VecI16Def cache.TableDef = newTableDef[Row[[]int16]]("vec_i_16",
	func(w bsatn.Writer, row *Row[[]int16]) { encodeVec(w, row.Value, encodeI16) },
	func(r bsatn.Reader) (*Row[[]int16], error) {
		v, err := decodeVec(r, decodeI16)
		if err != nil {
			return nil, err
		}
		return &Row[[]int16]{Value: v}, nil
	},
)

var VecI32Def cache.TableDef = newTableDef[Row[[]int32]]("vec_i_32",
	func(w bsatn.Writer, row *Row[[]int32]) { encodeVec(w, row.Value, encodeI32) },
	func(r bsatn.Reader) (*Row[[]int32], error) {
		v, err := decodeVec(r, decodeI32)
		if err != nil {
			return nil, err
		}
		return &Row[[]int32]{Value: v}, nil
	},
)

var VecI64Def cache.TableDef = newTableDef[Row[[]int64]]("vec_i_64",
	func(w bsatn.Writer, row *Row[[]int64]) { encodeVec(w, row.Value, encodeI64) },
	func(r bsatn.Reader) (*Row[[]int64], error) {
		v, err := decodeVec(r, decodeI64)
		if err != nil {
			return nil, err
		}
		return &Row[[]int64]{Value: v}, nil
	},
)

var VecI128Def cache.TableDef = newTableDef[Row[[]types.Int128]]("vec_i_128",
	func(w bsatn.Writer, row *Row[[]types.Int128]) { encodeVec(w, row.Value, encodeInt128) },
	func(r bsatn.Reader) (*Row[[]types.Int128], error) {
		v, err := decodeVec(r, decodeInt128)
		if err != nil {
			return nil, err
		}
		return &Row[[]types.Int128]{Value: v}, nil
	},
)

var VecI256Def cache.TableDef = newTableDef[Row[[]types.Int256]]("vec_i_256",
	func(w bsatn.Writer, row *Row[[]types.Int256]) { encodeVec(w, row.Value, encodeInt256) },
	func(r bsatn.Reader) (*Row[[]types.Int256], error) {
		v, err := decodeVec(r, decodeInt256)
		if err != nil {
			return nil, err
		}
		return &Row[[]types.Int256]{Value: v}, nil
	},
)

var VecBoolDef cache.TableDef = newTableDef[Row[[]bool]]("vec_bool",
	func(w bsatn.Writer, row *Row[[]bool]) { encodeVec(w, row.Value, encodeBool) },
	func(r bsatn.Reader) (*Row[[]bool], error) {
		v, err := decodeVec(r, decodeBool)
		if err != nil {
			return nil, err
		}
		return &Row[[]bool]{Value: v}, nil
	},
)

var VecF32Def cache.TableDef = newTableDef[Row[[]float32]]("vec_f_32",
	func(w bsatn.Writer, row *Row[[]float32]) { encodeVec(w, row.Value, encodeF32) },
	func(r bsatn.Reader) (*Row[[]float32], error) {
		v, err := decodeVec(r, decodeF32)
		if err != nil {
			return nil, err
		}
		return &Row[[]float32]{Value: v}, nil
	},
)

var VecF64Def cache.TableDef = newTableDef[Row[[]float64]]("vec_f_64",
	func(w bsatn.Writer, row *Row[[]float64]) { encodeVec(w, row.Value, encodeF64) },
	func(r bsatn.Reader) (*Row[[]float64], error) {
		v, err := decodeVec(r, decodeF64)
		if err != nil {
			return nil, err
		}
		return &Row[[]float64]{Value: v}, nil
	},
)

var VecStringDef cache.TableDef = newTableDef[Row[[]string]]("vec_string",
	func(w bsatn.Writer, row *Row[[]string]) { encodeVec(w, row.Value, encodeString) },
	func(r bsatn.Reader) (*Row[[]string], error) {
		v, err := decodeVec(r, decodeString)
		if err != nil {
			return nil, err
		}
		return &Row[[]string]{Value: v}, nil
	},
)

var VecIdentityDef cache.TableDef = newTableDef[Row[[]types.Identity]]("vec_identity",
	func(w bsatn.Writer, row *Row[[]types.Identity]) { encodeVec(w, row.Value, encodeIdentity) },
	func(r bsatn.Reader) (*Row[[]types.Identity], error) {
		v, err := decodeVec(r, decodeIdentity)
		if err != nil {
			return nil, err
		}
		return &Row[[]types.Identity]{Value: v}, nil
	},
)

var VecConnectionIdDef cache.TableDef = newTableDef[Row[[]types.ConnectionId]]("vec_connection_id",
	func(w bsatn.Writer, row *Row[[]types.ConnectionId]) { encodeVec(w, row.Value, encodeConnectionId) },
	func(r bsatn.Reader) (*Row[[]types.ConnectionId], error) {
		v, err := decodeVec(r, decodeConnectionId)
		if err != nil {
			return nil, err
		}
		return &Row[[]types.ConnectionId]{Value: v}, nil
	},
)

var VecUuidDef cache.TableDef = newTableDef[Row[[]types.Uuid]]("vec_uuid",
	func(w bsatn.Writer, row *Row[[]types.Uuid]) { encodeVec(w, row.Value, encodeUuid) },
	func(r bsatn.Reader) (*Row[[]types.Uuid], error) {
		v, err := decodeVec(r, decodeUuid)
		if err != nil {
			return nil, err
		}
		return &Row[[]types.Uuid]{Value: v}, nil
	},
)

var VecTimestampDef cache.TableDef = newTableDef[Row[[]types.Timestamp]]("vec_timestamp",
	func(w bsatn.Writer, row *Row[[]types.Timestamp]) { encodeVec(w, row.Value, encodeTimestamp) },
	func(r bsatn.Reader) (*Row[[]types.Timestamp], error) {
		v, err := decodeVec(r, decodeTimestamp)
		if err != nil {
			return nil, err
		}
		return &Row[[]types.Timestamp]{Value: v}, nil
	},
)

var VecSimpleEnumDef cache.TableDef = newTableDef[Row[[]SimpleEnum]]("vec_simple_enum",
	func(w bsatn.Writer, row *Row[[]SimpleEnum]) { encodeVec(w, row.Value, encodeSimpleEnum) },
	func(r bsatn.Reader) (*Row[[]SimpleEnum], error) {
		v, err := decodeVec(r, decodeSimpleEnum)
		if err != nil {
			return nil, err
		}
		return &Row[[]SimpleEnum]{Value: v}, nil
	},
)

var VecEnumWithPayloadDef cache.TableDef = newTableDef[Row[[]EnumWithPayload]]("vec_enum_with_payload",
	func(w bsatn.Writer, row *Row[[]EnumWithPayload]) { encodeVec(w, row.Value, encodeEnumWithPayload) },
	func(r bsatn.Reader) (*Row[[]EnumWithPayload], error) {
		v, err := decodeVec(r, decodeEnumWithPayload)
		if err != nil {
			return nil, err
		}
		return &Row[[]EnumWithPayload]{Value: v}, nil
	},
)

var VecUnitStructDef cache.TableDef = newTableDef[Row[[]*UnitStruct]]("vec_unit_struct",
	func(w bsatn.Writer, row *Row[[]*UnitStruct]) {
		w.PutArrayLen(uint32(len(row.Value)))
		for _, v := range row.Value {
			v.WriteBsatn(w)
		}
	},
	func(r bsatn.Reader) (*Row[[]*UnitStruct], error) {
		v, err := decodeVec(r, decodeUnitStruct)
		if err != nil {
			return nil, err
		}
		return &Row[[]*UnitStruct]{Value: v}, nil
	},
)

var VecByteStructDef cache.TableDef = newTableDef[Row[[]*ByteStruct]]("vec_byte_struct",
	func(w bsatn.Writer, row *Row[[]*ByteStruct]) {
		w.PutArrayLen(uint32(len(row.Value)))
		for _, v := range row.Value {
			v.WriteBsatn(w)
		}
	},
	func(r bsatn.Reader) (*Row[[]*ByteStruct], error) {
		v, err := decodeVec(r, decodeByteStruct)
		if err != nil {
			return nil, err
		}
		return &Row[[]*ByteStruct]{Value: v}, nil
	},
)

var VecEveryPrimitiveStructDef cache.TableDef = newTableDef[Row[[]*EveryPrimitiveStruct]]("vec_every_primitive_struct",
	func(w bsatn.Writer, row *Row[[]*EveryPrimitiveStruct]) {
		w.PutArrayLen(uint32(len(row.Value)))
		for _, v := range row.Value {
			v.WriteBsatn(w)
		}
	},
	func(r bsatn.Reader) (*Row[[]*EveryPrimitiveStruct], error) {
		v, err := decodeVec(r, decodeEveryPrimitiveStruct)
		if err != nil {
			return nil, err
		}
		return &Row[[]*EveryPrimitiveStruct]{Value: v}, nil
	},
)

var VecEveryVecStructDef cache.TableDef = newTableDef[Row[[]*EveryVecStruct]]("vec_every_vec_struct",
	func(w bsatn.Writer, row *Row[[]*EveryVecStruct]) {
		w.PutArrayLen(uint32(len(row.Value)))
		for _, v := range row.Value {
			v.WriteBsatn(w)
		}
	},
	func(r bsatn.Reader) (*Row[[]*EveryVecStruct], error) {
		v, err := decodeVec(r, decodeEveryVecStruct)
		if err != nil {
			return nil, err
		}
		return &Row[[]*EveryVecStruct]{Value: v}, nil
	},
)
