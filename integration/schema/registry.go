package schema

import (
	"go.digitalxero.dev/spacetimedb-client/client/cache"
)

// AllTableDefs returns all table definitions that do not have a primary key.
func AllTableDefs() []cache.TableDef {
	var defs []cache.TableDef
	defs = append(defs, PrimitiveTableDefs()...)
	defs = append(defs, VecTableDefs()...)
	defs = append(defs, OptionTableDefs()...)
	defs = append(defs, SpecialTableDefs()...)
	return defs
}

// AllTableDefsWithPK returns all table definitions that have a primary key.
func AllTableDefsWithPK() []cache.TableDefWithPK {
	var defs []cache.TableDefWithPK
	defs = append(defs, UniqueTableDefs()...)
	defs = append(defs, PkTableDefs()...)
	defs = append(defs, UsersDef, IndexedTableDef)
	return defs
}

// PrimitiveTableDefs returns the One* table definitions.
func PrimitiveTableDefs() []cache.TableDef {
	return []cache.TableDef{
		OneU8Def,
		OneU16Def,
		OneU32Def,
		OneU64Def,
		OneU128Def,
		OneU256Def,
		OneI8Def,
		OneI16Def,
		OneI32Def,
		OneI64Def,
		OneI128Def,
		OneI256Def,
		OneBoolDef,
		OneF32Def,
		OneF64Def,
		OneStringDef,
		OneIdentityDef,
		OneConnectionIdDef,
		OneUuidDef,
		OneTimestampDef,
		OneSimpleEnumDef,
		OneEnumWithPayloadDef,
		OneUnitStructDef,
		OneByteStructDef,
		OneEveryPrimitiveStructDef,
		OneEveryVecStructDef,
	}
}

// VecTableDefs returns the Vec* table definitions.
func VecTableDefs() []cache.TableDef {
	return []cache.TableDef{
		VecU8Def,
		VecU16Def,
		VecU32Def,
		VecU64Def,
		VecU128Def,
		VecU256Def,
		VecI8Def,
		VecI16Def,
		VecI32Def,
		VecI64Def,
		VecI128Def,
		VecI256Def,
		VecBoolDef,
		VecF32Def,
		VecF64Def,
		VecStringDef,
		VecIdentityDef,
		VecConnectionIdDef,
		VecUuidDef,
		VecTimestampDef,
		VecSimpleEnumDef,
		VecEnumWithPayloadDef,
		VecUnitStructDef,
		VecByteStructDef,
		VecEveryPrimitiveStructDef,
		VecEveryVecStructDef,
	}
}

// OptionTableDefs returns the Option* table definitions.
func OptionTableDefs() []cache.TableDef {
	return []cache.TableDef{
		OptionI32Def,
		OptionStringDef,
		OptionIdentityDef,
		OptionUuidDef,
		OptionSimpleEnumDef,
		OptionEveryPrimitiveStructDef,
		OptionVecOptionI32Def,
	}
}

// UniqueTableDefs returns the Unique* table definitions.
func UniqueTableDefs() []cache.TableDefWithPK {
	return []cache.TableDefWithPK{
		UniqueU8Def,
		UniqueU16Def,
		UniqueU32Def,
		UniqueU64Def,
		UniqueU128Def,
		UniqueU256Def,
		UniqueI8Def,
		UniqueI16Def,
		UniqueI32Def,
		UniqueI64Def,
		UniqueI128Def,
		UniqueI256Def,
		UniqueBoolDef,
		UniqueStringDef,
		UniqueIdentityDef,
		UniqueConnectionIdDef,
		UniqueUuidDef,
	}
}

// PkTableDefs returns the Pk* table definitions.
func PkTableDefs() []cache.TableDefWithPK {
	return []cache.TableDefWithPK{
		PkU8Def,
		PkU16Def,
		PkU32Def,
		PkU64Def,
		PkU128Def,
		PkU256Def,
		PkI8Def,
		PkI16Def,
		PkI32Def,
		PkI64Def,
		PkI128Def,
		PkI256Def,
		PkBoolDef,
		PkStringDef,
		PkIdentityDef,
		PkConnectionIdDef,
		PkUuidDef,
		PkSimpleEnumDef,
	}
}

// SpecialTableDefs returns special table definitions without primary keys.
func SpecialTableDefs() []cache.TableDef {
	return []cache.TableDef{
		LargeTableDef,
		BTreeU32Def,
		TableHoldsTableDef,
	}
}
