package ws

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"

	"github.com/coder/websocket"
)

// Connection wraps a WebSocket connection to a SpacetimeDB server.
// Send transmits a binary message, Recv blocks until a message arrives,
// and Close performs a graceful WebSocket close handshake.
//
// All methods accept a context for cancellation. If the context is
// cancelled, the operation returns the context error.
type Connection interface {
	Send(ctx context.Context, data []byte) error
	Recv(ctx context.Context) ([]byte, error)
	Close() error
}

// ConnectionBuilder configures and establishes a WebSocket connection
// to a SpacetimeDB server. Use [NewConnection] to create a builder.
//
// At minimum, WithUri and WithDatabaseName must be set before calling Build.
// The default protocol is "v2.bsatn.spacetimedb".
type ConnectionBuilder interface {
	WithUri(uri string) ConnectionBuilder
	WithDatabaseName(name string) ConnectionBuilder
	WithToken(token string) ConnectionBuilder
	WithProtocol(protocol string) ConnectionBuilder
	WithCompression(compression string) ConnectionBuilder
	Build(ctx context.Context) (Connection, error)
}

func NewConnection() ConnectionBuilder {
	return &connectionBuilder{
		protocol: "v2.bsatn.spacetimedb",
	}
}

type connectionBuilder struct {
	uri         string
	database    string
	token       string
	protocol    string
	compression string
}

func (b *connectionBuilder) WithUri(uri string) ConnectionBuilder {
	b.uri = uri
	return b
}

func (b *connectionBuilder) WithDatabaseName(name string) ConnectionBuilder {
	b.database = name
	return b
}

func (b *connectionBuilder) WithToken(token string) ConnectionBuilder {
	b.token = token
	return b
}

func (b *connectionBuilder) WithProtocol(protocol string) ConnectionBuilder {
	b.protocol = protocol
	return b
}

func (b *connectionBuilder) WithCompression(compression string) ConnectionBuilder {
	b.compression = compression
	return b
}

func (b *connectionBuilder) Build(ctx context.Context) (Connection, error) {
	// Build WebSocket URL: ws://{host}/v1/database/{database}/subscribe
	u, err := url.Parse(b.uri)
	if err != nil || u.Host == "" || u.User != nil || u.Fragment != "" {
		return nil, fmt.Errorf("ws: invalid server URI")
	}
	switch u.Scheme {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	case "ws", "wss":
	default:
		return nil, fmt.Errorf("ws: URI scheme must be http, https, ws, or wss")
	}
	if strings.TrimSpace(b.database) == "" || b.protocol == "" {
		return nil, fmt.Errorf("ws: database name and protocol are required")
	}
	basePath := strings.TrimRight(u.EscapedPath(), "/")
	u.RawPath = basePath + "/v1/database/" + url.PathEscape(b.database) + "/subscribe"
	u.Path, _ = url.PathUnescape(u.RawPath)
	q := u.Query()
	if b.compression != "" {
		switch strings.ToLower(b.compression) {
		case "none":
			q.Set("compression", "None")
		case "brotli":
			q.Set("compression", "Brotli")
		case "gzip":
			q.Set("compression", "Gzip")
		default:
			return nil, fmt.Errorf("ws: unsupported compression %q", b.compression)
		}
	}
	u.RawQuery = q.Encode()
	wsURL := u.String()

	// Set up headers
	headers := http.Header{}
	if b.token != "" {
		headers.Set("Authorization", fmt.Sprintf("Bearer %s", b.token))
	}

	// Dial with protocol negotiation
	conn, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{
		Subprotocols: []string{b.protocol},
		HTTPHeader:   headers,
	})
	if err != nil {
		return nil, fmt.Errorf("ws: dial failed: %w", err)
	}
	if conn.Subprotocol() != b.protocol {
		_ = conn.CloseNow()
		return nil, fmt.Errorf("ws: server did not negotiate protocol %q", b.protocol)
	}

	// Set reasonable read limit (33MB like Rust server)
	conn.SetReadLimit(33 * 1024 * 1024)

	return &connection{conn: conn}, nil
}

type connection struct {
	conn *websocket.Conn
}

func (c *connection) Send(ctx context.Context, data []byte) error {
	return c.conn.Write(ctx, websocket.MessageBinary, data)
}

func (c *connection) Recv(ctx context.Context) ([]byte, error) {
	typ, data, err := c.conn.Read(ctx)
	if err != nil {
		// DIAG: surface WHY the read failed — a 33 MiB read-limit breach, a server
		// close, a reset, etc. This error was previously swallowed by readLoop, which
		// made every failure look like a generic "connection lost".
		slog.Error("ws.Recv: read failed", "err", err, "closeStatus", websocket.CloseStatus(err))
		return nil, err
	}
	if typ != websocket.MessageBinary {
		return nil, fmt.Errorf("ws: expected a binary message")
	}
	if len(data) > 8*1024*1024 {
		slog.Warn("ws.Recv: large server message", "bytes", len(data), "limitBytes", 33*1024*1024)
	}
	return data, nil
}

func (c *connection) Close() error {
	return c.conn.Close(websocket.StatusNormalClosure, "client disconnect")
}
