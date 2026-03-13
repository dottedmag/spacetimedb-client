package schema

import (
	"go.digitalxero.dev/spacetimedb-client/bsatn"
	"go.digitalxero.dev/spacetimedb-client/client/cache"
	"go.digitalxero.dev/spacetimedb-client/types"
)

// Option tables: each row has a pointer field (*T where nil = None).

var OptionI32Def cache.TableDef = newTableDef[Row[*int32]]("option_i_32",
	func(w bsatn.Writer, row *Row[*int32]) {
		encodeOption(w, row.Value, encodeI32)
	},
	func(r bsatn.Reader) (*Row[*int32], error) {
		v, err := decodeOption(r, decodeI32)
		if err != nil {
			return nil, err
		}
		return &Row[*int32]{Value: v}, nil
	},
)

var OptionStringDef cache.TableDef = newTableDef[Row[*string]]("option_string",
	func(w bsatn.Writer, row *Row[*string]) {
		encodeOption(w, row.Value, encodeString)
	},
	func(r bsatn.Reader) (*Row[*string], error) {
		v, err := decodeOption(r, decodeString)
		if err != nil {
			return nil, err
		}
		return &Row[*string]{Value: v}, nil
	},
)

var OptionIdentityDef cache.TableDef = newTableDef[Row[*types.Identity]]("option_identity",
	func(w bsatn.Writer, row *Row[*types.Identity]) {
		encodeOption(w, row.Value, encodeIdentity)
	},
	func(r bsatn.Reader) (*Row[*types.Identity], error) {
		v, err := decodeOption(r, decodeIdentity)
		if err != nil {
			return nil, err
		}
		return &Row[*types.Identity]{Value: v}, nil
	},
)

var OptionUuidDef cache.TableDef = newTableDef[Row[*types.Uuid]]("option_uuid",
	func(w bsatn.Writer, row *Row[*types.Uuid]) {
		encodeOption(w, row.Value, encodeUuid)
	},
	func(r bsatn.Reader) (*Row[*types.Uuid], error) {
		v, err := decodeOption(r, decodeUuid)
		if err != nil {
			return nil, err
		}
		return &Row[*types.Uuid]{Value: v}, nil
	},
)

var OptionSimpleEnumDef cache.TableDef = newTableDef[Row[*SimpleEnum]]("option_simple_enum",
	func(w bsatn.Writer, row *Row[*SimpleEnum]) {
		encodeOption(w, row.Value, encodeSimpleEnum)
	},
	func(r bsatn.Reader) (*Row[*SimpleEnum], error) {
		v, err := decodeOption(r, decodeSimpleEnum)
		if err != nil {
			return nil, err
		}
		return &Row[*SimpleEnum]{Value: v}, nil
	},
)

var OptionEveryPrimitiveStructDef cache.TableDef = newTableDef[Row[*EveryPrimitiveStruct]]("option_every_primitive_struct",
	func(w bsatn.Writer, row *Row[*EveryPrimitiveStruct]) {
		if row.Value != nil {
			w.PutSumTag(0)
			row.Value.WriteBsatn(w)
		} else {
			w.PutSumTag(1)
		}
	},
	func(r bsatn.Reader) (*Row[*EveryPrimitiveStruct], error) {
		tag, err := r.GetSumTag()
		if err != nil {
			return nil, err
		}
		switch tag {
		case 0:
			v, err := ReadEveryPrimitiveStruct(r)
			if err != nil {
				return nil, err
			}
			return &Row[*EveryPrimitiveStruct]{Value: v}, nil
		case 1:
			return &Row[*EveryPrimitiveStruct]{Value: nil}, nil
		default:
			return nil, &bsatn.ErrInvalidTag{Tag: tag, SumName: "Option"}
		}
	},
)

// OptionVecOptionI32 is Option<Vec<Option<i32>>>
// Represented as: *[]*int32
var OptionVecOptionI32Def cache.TableDef = newTableDef[Row[*[]*int32]]("option_vec_option_i_32",
	func(w bsatn.Writer, row *Row[*[]*int32]) {
		if row.Value != nil {
			w.PutSumTag(0)
			items := *row.Value
			w.PutArrayLen(uint32(len(items)))
			for _, item := range items {
				encodeOption(w, item, encodeI32)
			}
		} else {
			w.PutSumTag(1)
		}
	},
	func(r bsatn.Reader) (*Row[*[]*int32], error) {
		tag, err := r.GetSumTag()
		if err != nil {
			return nil, err
		}
		switch tag {
		case 0:
			n, err := r.GetArrayLen()
			if err != nil {
				return nil, err
			}
			items := make([]*int32, 0, n)
			for i := uint32(0); i < n; i++ {
				v, err := decodeOption(r, decodeI32)
				if err != nil {
					return nil, err
				}
				items = append(items, v)
			}
			return &Row[*[]*int32]{Value: &items}, nil
		case 1:
			return &Row[*[]*int32]{Value: nil}, nil
		default:
			return nil, &bsatn.ErrInvalidTag{Tag: tag, SumName: "Option"}
		}
	},
)
