package protocol_test

import (
	"bytes"
	"compress/gzip"
	"testing"

	"github.com/stretchr/testify/require"
	"go.digitalxero.dev/spacetimedb-client/bsatn"
	"go.digitalxero.dev/spacetimedb-client/client/protocol"
)

func TestUntrustedArrayCounts(t *testing.T) {
	for _, tag := range []uint8{1, 4} {
		w := bsatn.NewWriter(32)
		w.PutSumTag(tag)
		if tag == 1 {
			w.PutU32(1)
			w.PutU32(1)
		}
		w.PutU32(^uint32(0))
		_, err := protocol.ReadServerMessage(bsatn.NewReader(w.Bytes()))
		require.Error(t, err)
	}
}

func TestInvalidRowBoundaries(t *testing.T) {
	for _, hint := range []protocol.RowSizeHint{
		protocol.FixedSizeHint{RowSize: 2},
		protocol.RowOffsetsHint{Offsets: []uint64{0, ^uint64(0)}},
		protocol.RowOffsetsHint{Offsets: []uint64{1}},
	} {
		list := &protocol.BsatnRowList{SizeHint: hint, RowsData: []byte{1, 2, 3}}
		require.Error(t, list.Validate())
		require.NotPanics(t, func() { list.Rows() })
	}
}

func TestDecompressionLimit(t *testing.T) {
	var buf bytes.Buffer
	buf.WriteByte(byte(protocol.CompressionGzip))
	w := gzip.NewWriter(&buf)
	_, err := w.Write(bytes.Repeat([]byte{0}, protocol.MaxMessageSize+1))
	require.NoError(t, err)
	require.NoError(t, w.Close())
	_, err = protocol.DecompressMessage(buf.Bytes())
	require.Error(t, err)
}
