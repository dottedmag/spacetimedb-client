package client

import "fmt"

// pkKey normalizes a primary-key value into something usable as a map key when
// matching a transaction's deletes against its inserts (applyPKUpdate).
//
// A PK is only useful there if two independent decodes of the same row produce
// equal keys. That holds for the scalar PKs (uint64, string, …) but not for the
// SDK's boxed identifier types: types.Identity and types.ConnectionId are
// interfaces backed by pointers, so two decodes of the same identity compare
// unequal and every update on such a table degrades into an unmatched
// delete + insert. Callers then see a row vanish and reappear instead of an
// update — and, because the pair can arrive insert-first, whatever the delete
// callback tears down wins.
//
// So: prefer the value's own bytes, fall back to its string form, and otherwise
// use the value as-is (scalars already behave).
func pkKey(pk any) any {
	switch v := pk.(type) {
	case nil:
		return nil
	case interface{ Bytes() [32]byte }:
		return v.Bytes()
	case interface{ Bytes() [16]byte }:
		return v.Bytes()
	case interface{ String() string }:
		return "s:" + v.String()
	}
	// A non-comparable PK (slice/map/func-backed) would panic as a map key, so
	// fall back to its formatted form rather than taking the process down.
	if !comparableKey(pk) {
		return fmt.Sprintf("%v", pk)
	}
	return pk
}

// comparableKey reports whether v can be used as a map key without panicking.
func comparableKey(v any) (ok bool) {
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	m := map[any]struct{}{}
	m[v] = struct{}{}
	return true
}
