package types_test

import (
	"fmt"

	"go.digitalxero.dev/spacetimedb-client/types"
)

func ExampleNewIdentity() {
	var b [32]byte
	b[31] = 0x01
	id := types.NewIdentity(b)
	fmt.Println(id.String())
	// Output: 0100000000000000000000000000000000000000000000000000000000000000
}

func ExampleNewTimestamp() {
	ts := types.NewTimestamp(1700000000000000)
	fmt.Println(ts.String())
	fmt.Println(ts.Microseconds())
	// Output:
	// 2023-11-14T22:13:20.000000+00:00
	// 1700000000000000
}

func ExampleNewTimeDuration() {
	td := types.NewTimeDuration(1500000)
	fmt.Println(td.Duration())
	fmt.Println(td.String())
	// Output:
	// 1.5s
	// 1.500000
}

func ExampleNewUint128() {
	u := types.NewUint128(1, 0)
	fmt.Println(u.String())
	// Output: 1
}

func ExampleNewUuid() {
	var b [16]byte
	for i := range b {
		b[i] = byte(i + 1)
	}
	u := types.NewUuid(b)
	fmt.Println(u.String())
	// Output: 100f0e0d-0c0b-0a09-0807-060504030201
}
