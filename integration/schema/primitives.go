package schema

import (
	"go.digitalxero.dev/spacetimedb-client/bsatn"
	"go.digitalxero.dev/spacetimedb-client/client/cache"
	"go.digitalxero.dev/spacetimedb-client/types"
)

var OneU8Def cache.TableDef = newTableDef[Row[uint8]]("one_u_8",
	func(w bsatn.Writer, row *Row[uint8]) { encodeRow(w, row, encodeU8) },
	func(r bsatn.Reader) (*Row[uint8], error) { return decodeRow(r, decodeU8) },
)

var OneU16Def cache.TableDef = newTableDef[Row[uint16]]("one_u_16",
	func(w bsatn.Writer, row *Row[uint16]) { encodeRow(w, row, encodeU16) },
	func(r bsatn.Reader) (*Row[uint16], error) { return decodeRow(r, decodeU16) },
)

var OneU32Def cache.TableDef = newTableDef[Row[uint32]]("one_u_32",
	func(w bsatn.Writer, row *Row[uint32]) { encodeRow(w, row, encodeU32) },
	func(r bsatn.Reader) (*Row[uint32], error) { return decodeRow(r, decodeU32) },
)

var OneU64Def cache.TableDef = newTableDef[Row[uint64]]("one_u_64",
	func(w bsatn.Writer, row *Row[uint64]) { encodeRow(w, row, encodeU64) },
	func(r bsatn.Reader) (*Row[uint64], error) { return decodeRow(r, decodeU64) },
)

var OneU128Def cache.TableDef = newTableDef[Row[types.Uint128]]("one_u_128",
	func(w bsatn.Writer, row *Row[types.Uint128]) { encodeRow(w, row, encodeUint128) },
	func(r bsatn.Reader) (*Row[types.Uint128], error) { return decodeRow(r, decodeUint128) },
)

var OneU256Def cache.TableDef = newTableDef[Row[types.Uint256]]("one_u_256",
	func(w bsatn.Writer, row *Row[types.Uint256]) { encodeRow(w, row, encodeUint256) },
	func(r bsatn.Reader) (*Row[types.Uint256], error) { return decodeRow(r, decodeUint256) },
)

var OneI8Def cache.TableDef = newTableDef[Row[int8]]("one_i_8",
	func(w bsatn.Writer, row *Row[int8]) { encodeRow(w, row, encodeI8) },
	func(r bsatn.Reader) (*Row[int8], error) { return decodeRow(r, decodeI8) },
)

var OneI16Def cache.TableDef = newTableDef[Row[int16]]("one_i_16",
	func(w bsatn.Writer, row *Row[int16]) { encodeRow(w, row, encodeI16) },
	func(r bsatn.Reader) (*Row[int16], error) { return decodeRow(r, decodeI16) },
)

var OneI32Def cache.TableDef = newTableDef[Row[int32]]("one_i_32",
	func(w bsatn.Writer, row *Row[int32]) { encodeRow(w, row, encodeI32) },
	func(r bsatn.Reader) (*Row[int32], error) { return decodeRow(r, decodeI32) },
)

var OneI64Def cache.TableDef = newTableDef[Row[int64]]("one_i_64",
	func(w bsatn.Writer, row *Row[int64]) { encodeRow(w, row, encodeI64) },
	func(r bsatn.Reader) (*Row[int64], error) { return decodeRow(r, decodeI64) },
)

var OneI128Def cache.TableDef = newTableDef[Row[types.Int128]]("one_i_128",
	func(w bsatn.Writer, row *Row[types.Int128]) { encodeRow(w, row, encodeInt128) },
	func(r bsatn.Reader) (*Row[types.Int128], error) { return decodeRow(r, decodeInt128) },
)

var OneI256Def cache.TableDef = newTableDef[Row[types.Int256]]("one_i_256",
	func(w bsatn.Writer, row *Row[types.Int256]) { encodeRow(w, row, encodeInt256) },
	func(r bsatn.Reader) (*Row[types.Int256], error) { return decodeRow(r, decodeInt256) },
)

var OneBoolDef cache.TableDef = newTableDef[Row[bool]]("one_bool",
	func(w bsatn.Writer, row *Row[bool]) { encodeRow(w, row, encodeBool) },
	func(r bsatn.Reader) (*Row[bool], error) { return decodeRow(r, decodeBool) },
)

var OneF32Def cache.TableDef = newTableDef[Row[float32]]("one_f_32",
	func(w bsatn.Writer, row *Row[float32]) { encodeRow(w, row, encodeF32) },
	func(r bsatn.Reader) (*Row[float32], error) { return decodeRow(r, decodeF32) },
)

var OneF64Def cache.TableDef = newTableDef[Row[float64]]("one_f_64",
	func(w bsatn.Writer, row *Row[float64]) { encodeRow(w, row, encodeF64) },
	func(r bsatn.Reader) (*Row[float64], error) { return decodeRow(r, decodeF64) },
)

var OneStringDef cache.TableDef = newTableDef[Row[string]]("one_string",
	func(w bsatn.Writer, row *Row[string]) { encodeRow(w, row, encodeString) },
	func(r bsatn.Reader) (*Row[string], error) { return decodeRow(r, decodeString) },
)

var OneIdentityDef cache.TableDef = newTableDef[Row[types.Identity]]("one_identity",
	func(w bsatn.Writer, row *Row[types.Identity]) { encodeRow(w, row, encodeIdentity) },
	func(r bsatn.Reader) (*Row[types.Identity], error) { return decodeRow(r, decodeIdentity) },
)

var OneConnectionIdDef cache.TableDef = newTableDef[Row[types.ConnectionId]]("one_connection_id",
	func(w bsatn.Writer, row *Row[types.ConnectionId]) { encodeRow(w, row, encodeConnectionId) },
	func(r bsatn.Reader) (*Row[types.ConnectionId], error) { return decodeRow(r, decodeConnectionId) },
)

var OneUuidDef cache.TableDef = newTableDef[Row[types.Uuid]]("one_uuid",
	func(w bsatn.Writer, row *Row[types.Uuid]) { encodeRow(w, row, encodeUuid) },
	func(r bsatn.Reader) (*Row[types.Uuid], error) { return decodeRow(r, decodeUuid) },
)

var OneTimestampDef cache.TableDef = newTableDef[Row[types.Timestamp]]("one_timestamp",
	func(w bsatn.Writer, row *Row[types.Timestamp]) { encodeRow(w, row, encodeTimestamp) },
	func(r bsatn.Reader) (*Row[types.Timestamp], error) { return decodeRow(r, decodeTimestamp) },
)

var OneSimpleEnumDef cache.TableDef = newTableDef[Row[SimpleEnum]]("one_simple_enum",
	func(w bsatn.Writer, row *Row[SimpleEnum]) { encodeRow(w, row, encodeSimpleEnum) },
	func(r bsatn.Reader) (*Row[SimpleEnum], error) { return decodeRow(r, decodeSimpleEnum) },
)

var OneEnumWithPayloadDef cache.TableDef = newTableDef[Row[EnumWithPayload]]("one_enum_with_payload",
	func(w bsatn.Writer, row *Row[EnumWithPayload]) { encodeRow(w, row, encodeEnumWithPayload) },
	func(r bsatn.Reader) (*Row[EnumWithPayload], error) { return decodeRow(r, decodeEnumWithPayload) },
)

var OneUnitStructDef cache.TableDef = newTableDef[Row[*UnitStruct]]("one_unit_struct",
	func(w bsatn.Writer, row *Row[*UnitStruct]) {
		row.Value.WriteBsatn(w)
	},
	func(r bsatn.Reader) (*Row[*UnitStruct], error) {
		v, err := ReadUnitStruct(r)
		if err != nil {
			return nil, err
		}
		return &Row[*UnitStruct]{Value: v}, nil
	},
)

var OneByteStructDef cache.TableDef = newTableDef[Row[*ByteStruct]]("one_byte_struct",
	func(w bsatn.Writer, row *Row[*ByteStruct]) {
		row.Value.WriteBsatn(w)
	},
	func(r bsatn.Reader) (*Row[*ByteStruct], error) {
		v, err := ReadByteStruct(r)
		if err != nil {
			return nil, err
		}
		return &Row[*ByteStruct]{Value: v}, nil
	},
)

var OneEveryPrimitiveStructDef cache.TableDef = newTableDef[Row[*EveryPrimitiveStruct]]("one_every_primitive_struct",
	func(w bsatn.Writer, row *Row[*EveryPrimitiveStruct]) {
		row.Value.WriteBsatn(w)
	},
	func(r bsatn.Reader) (*Row[*EveryPrimitiveStruct], error) {
		v, err := ReadEveryPrimitiveStruct(r)
		if err != nil {
			return nil, err
		}
		return &Row[*EveryPrimitiveStruct]{Value: v}, nil
	},
)

var OneEveryVecStructDef cache.TableDef = newTableDef[Row[*EveryVecStruct]]("one_every_vec_struct",
	func(w bsatn.Writer, row *Row[*EveryVecStruct]) {
		row.Value.WriteBsatn(w)
	},
	func(r bsatn.Reader) (*Row[*EveryVecStruct], error) {
		v, err := ReadEveryVecStruct(r)
		if err != nil {
			return nil, err
		}
		return &Row[*EveryVecStruct]{Value: v}, nil
	},
)
