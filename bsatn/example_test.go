package bsatn_test

import (
	"fmt"

	"go.digitalxero.dev/spacetimedb-client/bsatn"
)

// point implements bsatn.Serializable for use in examples.
type point struct {
	X int32
	Y int32
}

func (p point) WriteBsatn(w bsatn.Writer) {
	w.PutI32(p.X)
	w.PutI32(p.Y)
}

func readPoint(r bsatn.Reader) (point, error) {
	x, err := r.GetI32()
	if err != nil {
		return point{}, err
	}
	y, err := r.GetI32()
	if err != nil {
		return point{}, err
	}
	return point{X: x, Y: y}, nil
}

func ExampleNewWriter() {
	w := bsatn.NewWriter(16)
	w.PutU32(42)
	fmt.Println(w.Len())
	// Output:
	// 4
}

func ExampleNewReader() {
	w := bsatn.NewWriter(4)
	w.PutU32(42)

	r := bsatn.NewReader(w.Bytes())
	v, err := r.GetU32()
	if err != nil {
		panic(err)
	}
	fmt.Println(v)
	// Output:
	// 42
}

func ExampleEncode() {
	p := point{X: 10, Y: 20}
	data := bsatn.Encode(p)
	fmt.Println(len(data))
	// Output:
	// 8
}

func ExampleDecode() {
	p := point{X: 10, Y: 20}
	data := bsatn.Encode(p)

	decoded, err := bsatn.Decode(data, readPoint)
	if err != nil {
		panic(err)
	}
	fmt.Printf("(%d, %d)\n", decoded.X, decoded.Y)
	// Output:
	// (10, 20)
}

func ExampleWriteArray() {
	w := bsatn.NewWriter(64)
	items := []point{{X: 1, Y: 2}, {X: 3, Y: 4}}
	bsatn.WriteArray(w, items)
	fmt.Println(w.Len())
	// Output:
	// 20
}

func ExampleReadArray() {
	w := bsatn.NewWriter(64)
	items := []point{{X: 1, Y: 2}, {X: 3, Y: 4}}
	bsatn.WriteArray(w, items)

	r := bsatn.NewReader(w.Bytes())
	decoded, err := bsatn.ReadArray(r, readPoint)
	if err != nil {
		panic(err)
	}
	for _, p := range decoded {
		fmt.Printf("(%d, %d)\n", p.X, p.Y)
	}
	// Output:
	// (1, 2)
	// (3, 4)
}

func ExampleWriteOption() {
	w := bsatn.NewWriter(16)
	some := point{X: 5, Y: 10}
	bsatn.WriteOption(w, &some)
	fmt.Printf("some len: %d\n", w.Len())

	w.Reset()
	bsatn.WriteOption[point](w, nil)
	fmt.Printf("none len: %d\n", w.Len())
	// Output:
	// some len: 9
	// none len: 1
}

func ExampleReadOption() {
	w := bsatn.NewWriter(16)
	some := point{X: 5, Y: 10}
	bsatn.WriteOption(w, &some)

	r := bsatn.NewReader(w.Bytes())
	result, err := bsatn.ReadOption(r, readPoint)
	if err != nil {
		panic(err)
	}
	if result != nil {
		fmt.Printf("some: (%d, %d)\n", result.X, result.Y)
	}
	// Output:
	// some: (5, 10)
}

func ExampleWriteByteArray() {
	w := bsatn.NewWriter(16)
	bsatn.WriteByteArray(w, []byte{0xCA, 0xFE})
	fmt.Println(w.Len())
	// Output:
	// 6
}

func ExampleReadByteArray() {
	w := bsatn.NewWriter(16)
	bsatn.WriteByteArray(w, []byte{0xCA, 0xFE})

	r := bsatn.NewReader(w.Bytes())
	data, err := bsatn.ReadByteArray(r)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%X\n", data)
	// Output:
	// CAFE
}

func Example_roundTrip() {
	original := point{X: -100, Y: 200}
	data := bsatn.Encode(original)

	decoded, err := bsatn.Decode(data, readPoint)
	if err != nil {
		panic(err)
	}
	fmt.Printf("original: (%d, %d)\n", original.X, original.Y)
	fmt.Printf("decoded:  (%d, %d)\n", decoded.X, decoded.Y)
	// Output:
	// original: (-100, 200)
	// decoded:  (-100, 200)
}
