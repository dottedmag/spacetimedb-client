package types

import (
	"go.digitalxero.dev/spacetimedb-client/bsatn"
)

// EnergyQuanta wraps a [Uint128] representing the energy budget consumed
// or remaining for a reducer invocation.
//
// In BSATN it is encoded as a u128 (16 bytes little-endian).
// [EnergyQuanta.String] returns a decimal string of the energy amount.
type EnergyQuanta interface {
	bsatn.Serializable
	Value() Uint128
	String() string
}

// NewEnergyQuanta creates an EnergyQuanta from a Uint128.
func NewEnergyQuanta(v Uint128) EnergyQuanta {
	return &energyQuanta{value: v}
}

// ReadEnergyQuanta reads an EnergyQuanta from a BSATN reader (u128).
func ReadEnergyQuanta(r bsatn.Reader) (EnergyQuanta, error) {
	v, err := ReadUint128(r)
	if err != nil {
		return nil, err
	}
	return &energyQuanta{value: v}, nil
}

type energyQuanta struct {
	value Uint128
}

func (e *energyQuanta) WriteBsatn(w bsatn.Writer) {
	e.value.WriteBsatn(w)
}

func (e *energyQuanta) Value() Uint128 { return e.value }

func (e *energyQuanta) String() string {
	return e.value.String()
}
