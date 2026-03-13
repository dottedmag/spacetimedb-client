package schema

import (
	"go.digitalxero.dev/spacetimedb-client/bsatn"
	"go.digitalxero.dev/spacetimedb-client/client/cache"
	"go.digitalxero.dev/spacetimedb-client/types"
)

// Primary key tables: KeyDataRow with primary key constraint on Key.

var PkU8Def cache.TableDefWithPK = newTableDefWithPK[uint8]("pk_u_8",
	func(w bsatn.Writer, row *KeyDataRow[uint8]) { encodeKeyData(w, row, encodeU8) },
	func(r bsatn.Reader) (*KeyDataRow[uint8], error) { return decodeKeyData(r, decodeU8) },
	func(row *KeyDataRow[uint8]) any { return row.Key },
)

var PkU16Def cache.TableDefWithPK = newTableDefWithPK[uint16]("pk_u_16",
	func(w bsatn.Writer, row *KeyDataRow[uint16]) { encodeKeyData(w, row, encodeU16) },
	func(r bsatn.Reader) (*KeyDataRow[uint16], error) { return decodeKeyData(r, decodeU16) },
	func(row *KeyDataRow[uint16]) any { return row.Key },
)

var PkU32Def cache.TableDefWithPK = newTableDefWithPK[uint32]("pk_u_32",
	func(w bsatn.Writer, row *KeyDataRow[uint32]) { encodeKeyData(w, row, encodeU32) },
	func(r bsatn.Reader) (*KeyDataRow[uint32], error) { return decodeKeyData(r, decodeU32) },
	func(row *KeyDataRow[uint32]) any { return row.Key },
)

var PkU64Def cache.TableDefWithPK = newTableDefWithPK[uint64]("pk_u_64",
	func(w bsatn.Writer, row *KeyDataRow[uint64]) { encodeKeyData(w, row, encodeU64) },
	func(r bsatn.Reader) (*KeyDataRow[uint64], error) { return decodeKeyData(r, decodeU64) },
	func(row *KeyDataRow[uint64]) any { return row.Key },
)

var PkU128Def cache.TableDefWithPK = newTableDefWithPK[types.Uint128]("pk_u_128",
	func(w bsatn.Writer, row *KeyDataRow[types.Uint128]) { encodeKeyData(w, row, encodeUint128) },
	func(r bsatn.Reader) (*KeyDataRow[types.Uint128], error) { return decodeKeyData(r, decodeUint128) },
	func(row *KeyDataRow[types.Uint128]) any { return row.Key },
)

var PkU256Def cache.TableDefWithPK = newTableDefWithPK[types.Uint256]("pk_u_256",
	func(w bsatn.Writer, row *KeyDataRow[types.Uint256]) { encodeKeyData(w, row, encodeUint256) },
	func(r bsatn.Reader) (*KeyDataRow[types.Uint256], error) { return decodeKeyData(r, decodeUint256) },
	func(row *KeyDataRow[types.Uint256]) any { return row.Key },
)

var PkI8Def cache.TableDefWithPK = newTableDefWithPK[int8]("pk_i_8",
	func(w bsatn.Writer, row *KeyDataRow[int8]) { encodeKeyData(w, row, encodeI8) },
	func(r bsatn.Reader) (*KeyDataRow[int8], error) { return decodeKeyData(r, decodeI8) },
	func(row *KeyDataRow[int8]) any { return row.Key },
)

var PkI16Def cache.TableDefWithPK = newTableDefWithPK[int16]("pk_i_16",
	func(w bsatn.Writer, row *KeyDataRow[int16]) { encodeKeyData(w, row, encodeI16) },
	func(r bsatn.Reader) (*KeyDataRow[int16], error) { return decodeKeyData(r, decodeI16) },
	func(row *KeyDataRow[int16]) any { return row.Key },
)

var PkI32Def cache.TableDefWithPK = newTableDefWithPK[int32]("pk_i_32",
	func(w bsatn.Writer, row *KeyDataRow[int32]) { encodeKeyData(w, row, encodeI32) },
	func(r bsatn.Reader) (*KeyDataRow[int32], error) { return decodeKeyData(r, decodeI32) },
	func(row *KeyDataRow[int32]) any { return row.Key },
)

var PkI64Def cache.TableDefWithPK = newTableDefWithPK[int64]("pk_i_64",
	func(w bsatn.Writer, row *KeyDataRow[int64]) { encodeKeyData(w, row, encodeI64) },
	func(r bsatn.Reader) (*KeyDataRow[int64], error) { return decodeKeyData(r, decodeI64) },
	func(row *KeyDataRow[int64]) any { return row.Key },
)

var PkI128Def cache.TableDefWithPK = newTableDefWithPK[types.Int128]("pk_i_128",
	func(w bsatn.Writer, row *KeyDataRow[types.Int128]) { encodeKeyData(w, row, encodeInt128) },
	func(r bsatn.Reader) (*KeyDataRow[types.Int128], error) { return decodeKeyData(r, decodeInt128) },
	func(row *KeyDataRow[types.Int128]) any { return row.Key },
)

var PkI256Def cache.TableDefWithPK = newTableDefWithPK[types.Int256]("pk_i_256",
	func(w bsatn.Writer, row *KeyDataRow[types.Int256]) { encodeKeyData(w, row, encodeInt256) },
	func(r bsatn.Reader) (*KeyDataRow[types.Int256], error) { return decodeKeyData(r, decodeInt256) },
	func(row *KeyDataRow[types.Int256]) any { return row.Key },
)

var PkBoolDef cache.TableDefWithPK = newTableDefWithPK[bool]("pk_bool",
	func(w bsatn.Writer, row *KeyDataRow[bool]) { encodeKeyData(w, row, encodeBool) },
	func(r bsatn.Reader) (*KeyDataRow[bool], error) { return decodeKeyData(r, decodeBool) },
	func(row *KeyDataRow[bool]) any { return row.Key },
)

var PkStringDef cache.TableDefWithPK = newTableDefWithPK[string]("pk_string",
	func(w bsatn.Writer, row *KeyDataRow[string]) { encodeKeyData(w, row, encodeString) },
	func(r bsatn.Reader) (*KeyDataRow[string], error) { return decodeKeyData(r, decodeString) },
	func(row *KeyDataRow[string]) any { return row.Key },
)

var PkIdentityDef cache.TableDefWithPK = newTableDefWithPK[types.Identity]("pk_identity",
	func(w bsatn.Writer, row *KeyDataRow[types.Identity]) { encodeKeyData(w, row, encodeIdentity) },
	func(r bsatn.Reader) (*KeyDataRow[types.Identity], error) { return decodeKeyData(r, decodeIdentity) },
	func(row *KeyDataRow[types.Identity]) any { return row.Key },
)

var PkConnectionIdDef cache.TableDefWithPK = newTableDefWithPK[types.ConnectionId]("pk_connection_id",
	func(w bsatn.Writer, row *KeyDataRow[types.ConnectionId]) { encodeKeyData(w, row, encodeConnectionId) },
	func(r bsatn.Reader) (*KeyDataRow[types.ConnectionId], error) { return decodeKeyData(r, decodeConnectionId) },
	func(row *KeyDataRow[types.ConnectionId]) any { return row.Key },
)

var PkUuidDef cache.TableDefWithPK = newTableDefWithPK[types.Uuid]("pk_uuid",
	func(w bsatn.Writer, row *KeyDataRow[types.Uuid]) { encodeKeyData(w, row, encodeUuid) },
	func(r bsatn.Reader) (*KeyDataRow[types.Uuid], error) { return decodeKeyData(r, decodeUuid) },
	func(row *KeyDataRow[types.Uuid]) any { return row.Key },
)

var PkSimpleEnumDef cache.TableDefWithPK = newTableDefWithPK[SimpleEnum]("pk_simple_enum",
	func(w bsatn.Writer, row *KeyDataRow[SimpleEnum]) { encodeKeyData(w, row, encodeSimpleEnum) },
	func(r bsatn.Reader) (*KeyDataRow[SimpleEnum], error) { return decodeKeyData(r, decodeSimpleEnum) },
	func(row *KeyDataRow[SimpleEnum]) any { return row.Key },
)
