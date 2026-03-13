package schema

import (
	"go.digitalxero.dev/spacetimedb-client/bsatn"
	"go.digitalxero.dev/spacetimedb-client/client/cache"
	"go.digitalxero.dev/spacetimedb-client/types"
)

// Unique tables: KeyDataRow with unique constraint on Key, implements TableDefWithPK.

var UniqueU8Def cache.TableDefWithPK = newTableDefWithPK[uint8]("unique_u_8",
	func(w bsatn.Writer, row *KeyDataRow[uint8]) { encodeKeyData(w, row, encodeU8) },
	func(r bsatn.Reader) (*KeyDataRow[uint8], error) { return decodeKeyData(r, decodeU8) },
	func(row *KeyDataRow[uint8]) any { return row.Key },
)

var UniqueU16Def cache.TableDefWithPK = newTableDefWithPK[uint16]("unique_u_16",
	func(w bsatn.Writer, row *KeyDataRow[uint16]) { encodeKeyData(w, row, encodeU16) },
	func(r bsatn.Reader) (*KeyDataRow[uint16], error) { return decodeKeyData(r, decodeU16) },
	func(row *KeyDataRow[uint16]) any { return row.Key },
)

var UniqueU32Def cache.TableDefWithPK = newTableDefWithPK[uint32]("unique_u_32",
	func(w bsatn.Writer, row *KeyDataRow[uint32]) { encodeKeyData(w, row, encodeU32) },
	func(r bsatn.Reader) (*KeyDataRow[uint32], error) { return decodeKeyData(r, decodeU32) },
	func(row *KeyDataRow[uint32]) any { return row.Key },
)

var UniqueU64Def cache.TableDefWithPK = newTableDefWithPK[uint64]("unique_u_64",
	func(w bsatn.Writer, row *KeyDataRow[uint64]) { encodeKeyData(w, row, encodeU64) },
	func(r bsatn.Reader) (*KeyDataRow[uint64], error) { return decodeKeyData(r, decodeU64) },
	func(row *KeyDataRow[uint64]) any { return row.Key },
)

var UniqueU128Def cache.TableDefWithPK = newTableDefWithPK[types.Uint128]("unique_u_128",
	func(w bsatn.Writer, row *KeyDataRow[types.Uint128]) { encodeKeyData(w, row, encodeUint128) },
	func(r bsatn.Reader) (*KeyDataRow[types.Uint128], error) { return decodeKeyData(r, decodeUint128) },
	func(row *KeyDataRow[types.Uint128]) any { return row.Key },
)

var UniqueU256Def cache.TableDefWithPK = newTableDefWithPK[types.Uint256]("unique_u_256",
	func(w bsatn.Writer, row *KeyDataRow[types.Uint256]) { encodeKeyData(w, row, encodeUint256) },
	func(r bsatn.Reader) (*KeyDataRow[types.Uint256], error) { return decodeKeyData(r, decodeUint256) },
	func(row *KeyDataRow[types.Uint256]) any { return row.Key },
)

var UniqueI8Def cache.TableDefWithPK = newTableDefWithPK[int8]("unique_i_8",
	func(w bsatn.Writer, row *KeyDataRow[int8]) { encodeKeyData(w, row, encodeI8) },
	func(r bsatn.Reader) (*KeyDataRow[int8], error) { return decodeKeyData(r, decodeI8) },
	func(row *KeyDataRow[int8]) any { return row.Key },
)

var UniqueI16Def cache.TableDefWithPK = newTableDefWithPK[int16]("unique_i_16",
	func(w bsatn.Writer, row *KeyDataRow[int16]) { encodeKeyData(w, row, encodeI16) },
	func(r bsatn.Reader) (*KeyDataRow[int16], error) { return decodeKeyData(r, decodeI16) },
	func(row *KeyDataRow[int16]) any { return row.Key },
)

var UniqueI32Def cache.TableDefWithPK = newTableDefWithPK[int32]("unique_i_32",
	func(w bsatn.Writer, row *KeyDataRow[int32]) { encodeKeyData(w, row, encodeI32) },
	func(r bsatn.Reader) (*KeyDataRow[int32], error) { return decodeKeyData(r, decodeI32) },
	func(row *KeyDataRow[int32]) any { return row.Key },
)

var UniqueI64Def cache.TableDefWithPK = newTableDefWithPK[int64]("unique_i_64",
	func(w bsatn.Writer, row *KeyDataRow[int64]) { encodeKeyData(w, row, encodeI64) },
	func(r bsatn.Reader) (*KeyDataRow[int64], error) { return decodeKeyData(r, decodeI64) },
	func(row *KeyDataRow[int64]) any { return row.Key },
)

var UniqueI128Def cache.TableDefWithPK = newTableDefWithPK[types.Int128]("unique_i_128",
	func(w bsatn.Writer, row *KeyDataRow[types.Int128]) { encodeKeyData(w, row, encodeInt128) },
	func(r bsatn.Reader) (*KeyDataRow[types.Int128], error) { return decodeKeyData(r, decodeInt128) },
	func(row *KeyDataRow[types.Int128]) any { return row.Key },
)

var UniqueI256Def cache.TableDefWithPK = newTableDefWithPK[types.Int256]("unique_i_256",
	func(w bsatn.Writer, row *KeyDataRow[types.Int256]) { encodeKeyData(w, row, encodeInt256) },
	func(r bsatn.Reader) (*KeyDataRow[types.Int256], error) { return decodeKeyData(r, decodeInt256) },
	func(row *KeyDataRow[types.Int256]) any { return row.Key },
)

var UniqueBoolDef cache.TableDefWithPK = newTableDefWithPK[bool]("unique_bool",
	func(w bsatn.Writer, row *KeyDataRow[bool]) { encodeKeyData(w, row, encodeBool) },
	func(r bsatn.Reader) (*KeyDataRow[bool], error) { return decodeKeyData(r, decodeBool) },
	func(row *KeyDataRow[bool]) any { return row.Key },
)

var UniqueStringDef cache.TableDefWithPK = newTableDefWithPK[string]("unique_string",
	func(w bsatn.Writer, row *KeyDataRow[string]) { encodeKeyData(w, row, encodeString) },
	func(r bsatn.Reader) (*KeyDataRow[string], error) { return decodeKeyData(r, decodeString) },
	func(row *KeyDataRow[string]) any { return row.Key },
)

var UniqueIdentityDef cache.TableDefWithPK = newTableDefWithPK[types.Identity]("unique_identity",
	func(w bsatn.Writer, row *KeyDataRow[types.Identity]) { encodeKeyData(w, row, encodeIdentity) },
	func(r bsatn.Reader) (*KeyDataRow[types.Identity], error) { return decodeKeyData(r, decodeIdentity) },
	func(row *KeyDataRow[types.Identity]) any { return row.Key },
)

var UniqueConnectionIdDef cache.TableDefWithPK = newTableDefWithPK[types.ConnectionId]("unique_connection_id",
	func(w bsatn.Writer, row *KeyDataRow[types.ConnectionId]) { encodeKeyData(w, row, encodeConnectionId) },
	func(r bsatn.Reader) (*KeyDataRow[types.ConnectionId], error) { return decodeKeyData(r, decodeConnectionId) },
	func(row *KeyDataRow[types.ConnectionId]) any { return row.Key },
)

var UniqueUuidDef cache.TableDefWithPK = newTableDefWithPK[types.Uuid]("unique_uuid",
	func(w bsatn.Writer, row *KeyDataRow[types.Uuid]) { encodeKeyData(w, row, encodeUuid) },
	func(r bsatn.Reader) (*KeyDataRow[types.Uuid], error) { return decodeKeyData(r, decodeUuid) },
	func(row *KeyDataRow[types.Uuid]) any { return row.Key },
)
