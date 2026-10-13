package protocol

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"

	"github.com/andybalholm/brotli"
)

// Compression identifies the compression algorithm for server messages.
// Pass a Compression value to [client.DbConnectionBuilder.WithCompression]
// to request compressed responses from the server.
type Compression uint8

const (
	// MaxMessageSize bounds both compressed frames and decompressed payloads.
	MaxMessageSize = 33 * 1024 * 1024
	// CompressionNone means no compression.
	CompressionNone Compression = 0
	// CompressionBrotli means brotli compression.
	CompressionBrotli Compression = 1
	// CompressionGzip means gzip compression.
	CompressionGzip Compression = 2
)

// DecompressMessage reads the leading compression tag byte and
// decompresses the remaining payload accordingly.
func DecompressMessage(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("protocol: empty message")
	}

	tag := Compression(data[0])
	payload := data[1:]

	switch tag {
	case CompressionNone:
		if len(payload) > MaxMessageSize {
			return nil, fmt.Errorf("protocol: message exceeds %d bytes", MaxMessageSize)
		}
		return payload, nil
	case CompressionBrotli:
		r := brotli.NewReader(bytes.NewReader(payload))
		return readLimitedMessage(r)
	case CompressionGzip:
		gr, err := gzip.NewReader(bytes.NewReader(payload))
		if err != nil {
			return nil, fmt.Errorf("protocol: gzip init: %w", err)
		}
		defer func() { _ = gr.Close() }()
		return readLimitedMessage(gr)
	default:
		return nil, fmt.Errorf("protocol: unknown compression tag: %d", tag)
	}
}

func readLimitedMessage(r io.Reader) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(r, MaxMessageSize+1))
	if err != nil {
		return nil, fmt.Errorf("protocol: decompress: %w", err)
	}
	if len(data) > MaxMessageSize {
		return nil, fmt.Errorf("protocol: decompressed message exceeds %d bytes", MaxMessageSize)
	}
	return data, nil
}
