package schema

import (
	"go.digitalxero.dev/spacetimedb-client/bsatn"
	"go.digitalxero.dev/spacetimedb-client/types"
)

// --- Insert args for One* tables (single value) ---

func InsertOneU8Args(v uint8) bsatn.Serializable                { return newArgs(v, encodeU8) }
func InsertOneU16Args(v uint16) bsatn.Serializable              { return newArgs(v, encodeU16) }
func InsertOneU32Args(v uint32) bsatn.Serializable              { return newArgs(v, encodeU32) }
func InsertOneU64Args(v uint64) bsatn.Serializable              { return newArgs(v, encodeU64) }
func InsertOneU128Args(v types.Uint128) bsatn.Serializable      { return newArgs(v, encodeUint128) }
func InsertOneU256Args(v types.Uint256) bsatn.Serializable      { return newArgs(v, encodeUint256) }
func InsertOneI8Args(v int8) bsatn.Serializable                 { return newArgs(v, encodeI8) }
func InsertOneI16Args(v int16) bsatn.Serializable               { return newArgs(v, encodeI16) }
func InsertOneI32Args(v int32) bsatn.Serializable               { return newArgs(v, encodeI32) }
func InsertOneI64Args(v int64) bsatn.Serializable               { return newArgs(v, encodeI64) }
func InsertOneI128Args(v types.Int128) bsatn.Serializable       { return newArgs(v, encodeInt128) }
func InsertOneI256Args(v types.Int256) bsatn.Serializable       { return newArgs(v, encodeInt256) }
func InsertOneBoolArgs(v bool) bsatn.Serializable               { return newArgs(v, encodeBool) }
func InsertOneF32Args(v float32) bsatn.Serializable             { return newArgs(v, encodeF32) }
func InsertOneF64Args(v float64) bsatn.Serializable             { return newArgs(v, encodeF64) }
func InsertOneStringArgs(v string) bsatn.Serializable           { return newArgs(v, encodeString) }
func InsertOneIdentityArgs(v types.Identity) bsatn.Serializable { return newArgs(v, encodeIdentity) }
func InsertOneConnectionIdArgs(v types.ConnectionId) bsatn.Serializable {
	return newArgs(v, encodeConnectionId)
}
func InsertOneUuidArgs(v types.Uuid) bsatn.Serializable           { return newArgs(v, encodeUuid) }
func InsertOneTimestampArgs(v types.Timestamp) bsatn.Serializable { return newArgs(v, encodeTimestamp) }
func InsertOneSimpleEnumArgs(v SimpleEnum) bsatn.Serializable     { return newArgs(v, encodeSimpleEnum) }
func InsertOneEnumWithPayloadArgs(v EnumWithPayload) bsatn.Serializable {
	return newArgs(v, encodeEnumWithPayload)
}
func InsertOneUnitStructArgs(v *UnitStruct) bsatn.Serializable {
	return newArgs(v, func(w bsatn.Writer, s *UnitStruct) { s.WriteBsatn(w) })
}
func InsertOneByteStructArgs(v *ByteStruct) bsatn.Serializable {
	return newArgs(v, func(w bsatn.Writer, s *ByteStruct) { s.WriteBsatn(w) })
}
func InsertOneEveryPrimitiveStructArgs(v *EveryPrimitiveStruct) bsatn.Serializable {
	return newArgs(v, func(w bsatn.Writer, s *EveryPrimitiveStruct) { s.WriteBsatn(w) })
}
func InsertOneEveryVecStructArgs(v *EveryVecStruct) bsatn.Serializable {
	return newArgs(v, func(w bsatn.Writer, s *EveryVecStruct) { s.WriteBsatn(w) })
}

// --- Insert args for Vec* tables ---

func InsertVecU8Args(v []uint8) bsatn.Serializable {
	return newArgs(v, func(w bsatn.Writer, data []uint8) { bsatn.WriteByteArray(w, data) })
}
func InsertVecU16Args(v []uint16) bsatn.Serializable {
	return newArgs(v, func(w bsatn.Writer, data []uint16) { encodeVec(w, data, encodeU16) })
}
func InsertVecU32Args(v []uint32) bsatn.Serializable {
	return newArgs(v, func(w bsatn.Writer, data []uint32) { encodeVec(w, data, encodeU32) })
}
func InsertVecU64Args(v []uint64) bsatn.Serializable {
	return newArgs(v, func(w bsatn.Writer, data []uint64) { encodeVec(w, data, encodeU64) })
}
func InsertVecU128Args(v []types.Uint128) bsatn.Serializable {
	return newArgs(v, func(w bsatn.Writer, data []types.Uint128) { encodeVec(w, data, encodeUint128) })
}
func InsertVecU256Args(v []types.Uint256) bsatn.Serializable {
	return newArgs(v, func(w bsatn.Writer, data []types.Uint256) { encodeVec(w, data, encodeUint256) })
}
func InsertVecI8Args(v []int8) bsatn.Serializable {
	return newArgs(v, func(w bsatn.Writer, data []int8) { encodeVecI8(w, data) })
}
func InsertVecI16Args(v []int16) bsatn.Serializable {
	return newArgs(v, func(w bsatn.Writer, data []int16) { encodeVec(w, data, encodeI16) })
}
func InsertVecI32Args(v []int32) bsatn.Serializable {
	return newArgs(v, func(w bsatn.Writer, data []int32) { encodeVec(w, data, encodeI32) })
}
func InsertVecI64Args(v []int64) bsatn.Serializable {
	return newArgs(v, func(w bsatn.Writer, data []int64) { encodeVec(w, data, encodeI64) })
}
func InsertVecI128Args(v []types.Int128) bsatn.Serializable {
	return newArgs(v, func(w bsatn.Writer, data []types.Int128) { encodeVec(w, data, encodeInt128) })
}
func InsertVecI256Args(v []types.Int256) bsatn.Serializable {
	return newArgs(v, func(w bsatn.Writer, data []types.Int256) { encodeVec(w, data, encodeInt256) })
}
func InsertVecBoolArgs(v []bool) bsatn.Serializable {
	return newArgs(v, func(w bsatn.Writer, data []bool) { encodeVec(w, data, encodeBool) })
}
func InsertVecF32Args(v []float32) bsatn.Serializable {
	return newArgs(v, func(w bsatn.Writer, data []float32) { encodeVec(w, data, encodeF32) })
}
func InsertVecF64Args(v []float64) bsatn.Serializable {
	return newArgs(v, func(w bsatn.Writer, data []float64) { encodeVec(w, data, encodeF64) })
}
func InsertVecStringArgs(v []string) bsatn.Serializable {
	return newArgs(v, func(w bsatn.Writer, data []string) { encodeVec(w, data, encodeString) })
}
func InsertVecIdentityArgs(v []types.Identity) bsatn.Serializable {
	return newArgs(v, func(w bsatn.Writer, data []types.Identity) { encodeVec(w, data, encodeIdentity) })
}
func InsertVecConnectionIdArgs(v []types.ConnectionId) bsatn.Serializable {
	return newArgs(v, func(w bsatn.Writer, data []types.ConnectionId) {
		encodeVec(w, data, encodeConnectionId)
	})
}
func InsertVecUuidArgs(v []types.Uuid) bsatn.Serializable {
	return newArgs(v, func(w bsatn.Writer, data []types.Uuid) { encodeVec(w, data, encodeUuid) })
}
func InsertVecTimestampArgs(v []types.Timestamp) bsatn.Serializable {
	return newArgs(v, func(w bsatn.Writer, data []types.Timestamp) { encodeVec(w, data, encodeTimestamp) })
}
func InsertVecSimpleEnumArgs(v []SimpleEnum) bsatn.Serializable {
	return newArgs(v, func(w bsatn.Writer, data []SimpleEnum) { encodeVec(w, data, encodeSimpleEnum) })
}
func InsertVecEnumWithPayloadArgs(v []EnumWithPayload) bsatn.Serializable {
	return newArgs(v, func(w bsatn.Writer, data []EnumWithPayload) { encodeVec(w, data, encodeEnumWithPayload) })
}
func InsertVecUnitStructArgs(v []*UnitStruct) bsatn.Serializable {
	return newArgs(v, func(w bsatn.Writer, data []*UnitStruct) {
		w.PutArrayLen(uint32(len(data)))
		for _, s := range data {
			s.WriteBsatn(w)
		}
	})
}
func InsertVecByteStructArgs(v []*ByteStruct) bsatn.Serializable {
	return newArgs(v, func(w bsatn.Writer, data []*ByteStruct) {
		w.PutArrayLen(uint32(len(data)))
		for _, s := range data {
			s.WriteBsatn(w)
		}
	})
}
func InsertVecEveryPrimitiveStructArgs(v []*EveryPrimitiveStruct) bsatn.Serializable {
	return newArgs(v, func(w bsatn.Writer, data []*EveryPrimitiveStruct) {
		w.PutArrayLen(uint32(len(data)))
		for _, s := range data {
			s.WriteBsatn(w)
		}
	})
}
func InsertVecEveryVecStructArgs(v []*EveryVecStruct) bsatn.Serializable {
	return newArgs(v, func(w bsatn.Writer, data []*EveryVecStruct) {
		w.PutArrayLen(uint32(len(data)))
		for _, s := range data {
			s.WriteBsatn(w)
		}
	})
}

// --- No-arg reducer args ---

func InsertCallerOneIdentityArgs() bsatn.Serializable { return &emptyArgs{} }
func InsertCallTimestampArgs() bsatn.Serializable     { return &emptyArgs{} }
func InsertCallUuidV4Args() bsatn.Serializable        { return &emptyArgs{} }

// --- Insert args for Unique* tables (key + data) ---

func insertKeyDataArgs[K any](key K, data int32, enc EncoderFunc[K]) bsatn.Serializable {
	return &multiArgs{data: encodeKeyDataBytes(key, data, enc)}
}

func encodeKeyDataBytes[K any](key K, data int32, enc EncoderFunc[K]) []byte {
	w := bsatn.NewWriter(64)
	enc(w, key)
	w.PutI32(data)
	return w.Bytes()
}

func InsertUniqueU8Args(key uint8, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeU8)
}
func InsertUniqueU16Args(key uint16, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeU16)
}
func InsertUniqueU32Args(key uint32, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeU32)
}
func InsertUniqueU64Args(key uint64, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeU64)
}
func InsertUniqueU128Args(key types.Uint128, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeUint128)
}
func InsertUniqueU256Args(key types.Uint256, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeUint256)
}
func InsertUniqueI8Args(key int8, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeI8)
}
func InsertUniqueI16Args(key int16, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeI16)
}
func InsertUniqueI32Args(key int32, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeI32)
}
func InsertUniqueI64Args(key int64, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeI64)
}
func InsertUniqueI128Args(key types.Int128, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeInt128)
}
func InsertUniqueI256Args(key types.Int256, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeInt256)
}
func InsertUniqueBoolArgs(key bool, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeBool)
}
func InsertUniqueStringArgs(key string, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeString)
}
func InsertUniqueIdentityArgs(key types.Identity, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeIdentity)
}
func InsertUniqueConnectionIdArgs(key types.ConnectionId, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeConnectionId)
}
func InsertUniqueUuidArgs(key types.Uuid, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeUuid)
}

// --- Delete args for Unique* tables (key only) ---

func DeleteUniqueU8Args(key uint8) bsatn.Serializable           { return newArgs(key, encodeU8) }
func DeleteUniqueU16Args(key uint16) bsatn.Serializable         { return newArgs(key, encodeU16) }
func DeleteUniqueU32Args(key uint32) bsatn.Serializable         { return newArgs(key, encodeU32) }
func DeleteUniqueU64Args(key uint64) bsatn.Serializable         { return newArgs(key, encodeU64) }
func DeleteUniqueU128Args(key types.Uint128) bsatn.Serializable { return newArgs(key, encodeUint128) }
func DeleteUniqueU256Args(key types.Uint256) bsatn.Serializable { return newArgs(key, encodeUint256) }
func DeleteUniqueI8Args(key int8) bsatn.Serializable            { return newArgs(key, encodeI8) }
func DeleteUniqueI16Args(key int16) bsatn.Serializable          { return newArgs(key, encodeI16) }
func DeleteUniqueI32Args(key int32) bsatn.Serializable          { return newArgs(key, encodeI32) }
func DeleteUniqueI64Args(key int64) bsatn.Serializable          { return newArgs(key, encodeI64) }
func DeleteUniqueI128Args(key types.Int128) bsatn.Serializable  { return newArgs(key, encodeInt128) }
func DeleteUniqueI256Args(key types.Int256) bsatn.Serializable  { return newArgs(key, encodeInt256) }
func DeleteUniqueBoolArgs(key bool) bsatn.Serializable          { return newArgs(key, encodeBool) }
func DeleteUniqueStringArgs(key string) bsatn.Serializable      { return newArgs(key, encodeString) }
func DeleteUniqueIdentityArgs(key types.Identity) bsatn.Serializable {
	return newArgs(key, encodeIdentity)
}
func DeleteUniqueConnectionIdArgs(key types.ConnectionId) bsatn.Serializable {
	return newArgs(key, encodeConnectionId)
}
func DeleteUniqueUuidArgs(key types.Uuid) bsatn.Serializable { return newArgs(key, encodeUuid) }

// --- Update args for Unique* tables (key + data) ---

func UpdateUniqueU8Args(key uint8, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeU8)
}
func UpdateUniqueU16Args(key uint16, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeU16)
}
func UpdateUniqueU32Args(key uint32, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeU32)
}
func UpdateUniqueU64Args(key uint64, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeU64)
}
func UpdateUniqueU128Args(key types.Uint128, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeUint128)
}
func UpdateUniqueU256Args(key types.Uint256, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeUint256)
}
func UpdateUniqueI8Args(key int8, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeI8)
}
func UpdateUniqueI16Args(key int16, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeI16)
}
func UpdateUniqueI32Args(key int32, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeI32)
}
func UpdateUniqueI64Args(key int64, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeI64)
}
func UpdateUniqueI128Args(key types.Int128, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeInt128)
}
func UpdateUniqueI256Args(key types.Int256, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeInt256)
}
func UpdateUniqueBoolArgs(key bool, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeBool)
}
func UpdateUniqueStringArgs(key string, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeString)
}
func UpdateUniqueIdentityArgs(key types.Identity, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeIdentity)
}
func UpdateUniqueConnectionIdArgs(key types.ConnectionId, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeConnectionId)
}
func UpdateUniqueUuidArgs(key types.Uuid, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeUuid)
}

// --- Insert args for Pk* tables (key + data) ---

func InsertPkU8Args(key uint8, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeU8)
}
func InsertPkU16Args(key uint16, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeU16)
}
func InsertPkU32Args(key uint32, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeU32)
}
func InsertPkU64Args(key uint64, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeU64)
}
func InsertPkU128Args(key types.Uint128, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeUint128)
}
func InsertPkU256Args(key types.Uint256, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeUint256)
}
func InsertPkI8Args(key int8, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeI8)
}
func InsertPkI16Args(key int16, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeI16)
}
func InsertPkI32Args(key int32, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeI32)
}
func InsertPkI64Args(key int64, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeI64)
}
func InsertPkI128Args(key types.Int128, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeInt128)
}
func InsertPkI256Args(key types.Int256, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeInt256)
}
func InsertPkBoolArgs(key bool, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeBool)
}
func InsertPkStringArgs(key string, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeString)
}
func InsertPkIdentityArgs(key types.Identity, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeIdentity)
}
func InsertPkConnectionIdArgs(key types.ConnectionId, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeConnectionId)
}
func InsertPkUuidArgs(key types.Uuid, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeUuid)
}
func InsertPkSimpleEnumArgs(key SimpleEnum, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeSimpleEnum)
}

// --- Delete args for Pk* tables (key only) ---

func DeletePkU8Args(key uint8) bsatn.Serializable                { return newArgs(key, encodeU8) }
func DeletePkU16Args(key uint16) bsatn.Serializable              { return newArgs(key, encodeU16) }
func DeletePkU32Args(key uint32) bsatn.Serializable              { return newArgs(key, encodeU32) }
func DeletePkU64Args(key uint64) bsatn.Serializable              { return newArgs(key, encodeU64) }
func DeletePkU128Args(key types.Uint128) bsatn.Serializable      { return newArgs(key, encodeUint128) }
func DeletePkU256Args(key types.Uint256) bsatn.Serializable      { return newArgs(key, encodeUint256) }
func DeletePkI8Args(key int8) bsatn.Serializable                 { return newArgs(key, encodeI8) }
func DeletePkI16Args(key int16) bsatn.Serializable               { return newArgs(key, encodeI16) }
func DeletePkI32Args(key int32) bsatn.Serializable               { return newArgs(key, encodeI32) }
func DeletePkI64Args(key int64) bsatn.Serializable               { return newArgs(key, encodeI64) }
func DeletePkI128Args(key types.Int128) bsatn.Serializable       { return newArgs(key, encodeInt128) }
func DeletePkI256Args(key types.Int256) bsatn.Serializable       { return newArgs(key, encodeInt256) }
func DeletePkBoolArgs(key bool) bsatn.Serializable               { return newArgs(key, encodeBool) }
func DeletePkStringArgs(key string) bsatn.Serializable           { return newArgs(key, encodeString) }
func DeletePkIdentityArgs(key types.Identity) bsatn.Serializable { return newArgs(key, encodeIdentity) }
func DeletePkConnectionIdArgs(key types.ConnectionId) bsatn.Serializable {
	return newArgs(key, encodeConnectionId)
}
func DeletePkUuidArgs(key types.Uuid) bsatn.Serializable       { return newArgs(key, encodeUuid) }
func DeletePkSimpleEnumArgs(key SimpleEnum) bsatn.Serializable { return newArgs(key, encodeSimpleEnum) }

// --- Update args for Pk* tables (key + data) ---

func UpdatePkU8Args(key uint8, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeU8)
}
func UpdatePkU16Args(key uint16, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeU16)
}
func UpdatePkU32Args(key uint32, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeU32)
}
func UpdatePkU64Args(key uint64, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeU64)
}
func UpdatePkU128Args(key types.Uint128, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeUint128)
}
func UpdatePkU256Args(key types.Uint256, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeUint256)
}
func UpdatePkI8Args(key int8, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeI8)
}
func UpdatePkI16Args(key int16, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeI16)
}
func UpdatePkI32Args(key int32, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeI32)
}
func UpdatePkI64Args(key int64, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeI64)
}
func UpdatePkI128Args(key types.Int128, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeInt128)
}
func UpdatePkI256Args(key types.Int256, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeInt256)
}
func UpdatePkBoolArgs(key bool, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeBool)
}
func UpdatePkStringArgs(key string, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeString)
}
func UpdatePkIdentityArgs(key types.Identity, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeIdentity)
}
func UpdatePkConnectionIdArgs(key types.ConnectionId, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeConnectionId)
}
func UpdatePkUuidArgs(key types.Uuid, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeUuid)
}
func UpdatePkSimpleEnumArgs(key SimpleEnum, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeSimpleEnum)
}

// --- Insert args for Option* tables ---

func InsertOptionI32Args(v *int32) bsatn.Serializable {
	return newArgs(v, func(w bsatn.Writer, val *int32) { encodeOption(w, val, encodeI32) })
}
func InsertOptionStringArgs(v *string) bsatn.Serializable {
	return newArgs(v, func(w bsatn.Writer, val *string) { encodeOption(w, val, encodeString) })
}
func InsertOptionIdentityArgs(v *types.Identity) bsatn.Serializable {
	return newArgs(v, func(w bsatn.Writer, val *types.Identity) { encodeOption(w, val, encodeIdentity) })
}
func InsertOptionUuidArgs(v *types.Uuid) bsatn.Serializable {
	return newArgs(v, func(w bsatn.Writer, val *types.Uuid) { encodeOption(w, val, encodeUuid) })
}
func InsertOptionSimpleEnumArgs(v *SimpleEnum) bsatn.Serializable {
	return newArgs(v, func(w bsatn.Writer, val *SimpleEnum) { encodeOption(w, val, encodeSimpleEnum) })
}
func InsertOptionEveryPrimitiveStructArgs(v *EveryPrimitiveStruct) bsatn.Serializable {
	return newArgs(v, func(w bsatn.Writer, val *EveryPrimitiveStruct) {
		if val != nil {
			w.PutSumTag(0)
			val.WriteBsatn(w)
		} else {
			w.PutSumTag(1)
		}
	})
}
func InsertOptionVecOptionI32Args(v *[]*int32) bsatn.Serializable {
	return newArgs(v, func(w bsatn.Writer, val *[]*int32) {
		if val != nil {
			w.PutSumTag(0)
			items := *val
			w.PutArrayLen(uint32(len(items)))
			for _, item := range items {
				encodeOption(w, item, encodeI32)
			}
		} else {
			w.PutSumTag(1)
		}
	})
}

// --- Insert args for special tables ---

func InsertLargeTableArgs(t *LargeTable) bsatn.Serializable {
	return newArgs(t, func(w bsatn.Writer, val *LargeTable) { val.WriteBsatn(w) })
}

func InsertTableHoldsTableArgs(a uint8, b uint8) bsatn.Serializable {
	return &multiArgs{data: func() []byte {
		w := bsatn.NewWriter(2)
		w.PutU8(a)
		w.PutU8(b)
		return w.Bytes()
	}()}
}

func InsertIndexedTableArgs(id uint32, key uint32, data int32) bsatn.Serializable {
	return &multiArgs{data: func() []byte {
		w := bsatn.NewWriter(12)
		w.PutU32(id)
		w.PutU32(key)
		w.PutI32(data)
		return w.Bytes()
	}()}
}

func InsertBTreeU32Args(key uint32, data int32) bsatn.Serializable {
	return insertKeyDataArgs(key, data, encodeU32)
}
