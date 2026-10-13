package ws_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
	"go.digitalxero.dev/spacetimedb-client/client/ws"
)

func TestURLAndCompressionNegotiation(t *testing.T) {
	request := make(chan *http.Request, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request <- r
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{"v2.bsatn.spacetimedb"}})
		if err != nil {
			return
		}
		defer func() { _ = conn.CloseNow() }()
		_, _, _ = conn.Read(r.Context())
	}))
	defer srv.Close()
	conn, err := ws.NewConnection().WithUri(srv.URL + "/proxy/?confirmed=true").WithDatabaseName("db/with space").WithCompression("gzip").Build(context.Background())
	require.NoError(t, err)
	r := <-request
	require.Equal(t, "/proxy/v1/database/db%2Fwith%20space/subscribe", r.URL.EscapedPath())
	require.Equal(t, "Gzip", r.URL.Query().Get("compression"))
	require.Equal(t, "true", r.URL.Query().Get("confirmed"))
	_ = conn.Close()
}

func TestInvalidURIs(t *testing.T) {
	for _, uri := range []string{"", "localhost:3000", "ftp://host", "ws://user:pass@host", "ws://host/#fragment"} {
		_, err := ws.NewConnection().WithUri(uri).WithDatabaseName("db").Build(t.Context())
		require.Error(t, err)
	}
}
