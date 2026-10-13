# SpacetimeDB Go Client

```
go get go.digitalxero.dev/spacetimedb-client
```

A Go client library for [SpacetimeDB](https://spacetimedb.com) with real-time subscriptions, local row caching, and BSATN serialization over WebSocket. Targets **SpacetimeDB 2.11.0** and requires **Go 1.26.9+**.

## Production use

The default wire protocol is `v2.bsatn.spacetimedb`. Select `WithProtocol("v3.bsatn.spacetimedb")` for coalesced messages on current servers. Both protocols support no compression, Brotli, and gzip through `WithCompression`. HTTP/HTTPS server URLs are accepted alongside WS/WSS URLs; use HTTPS or WSS for remote deployments.

Call `Run` exactly once and register table definitions before starting it. Calls and subscriptions may be queued before `Run`; the command queue is bounded and returns an error when full. Connection shutdown releases outstanding queries and procedures, invalidates subscription handles, and reports transport or protocol failures through `Run` and `OnDisconnect`. `OnConnectError` reports connection establishment failures. The client does not automatically reconnect: rebuild the connection with the saved `Token()` and register subscriptions again.

Use `OneOffQueryContext` and `CallProcedure` with deadlines for request cancellation. These methods wait for the event loop to receive a response; call them from a separate goroutine when initiating work inside a callback. Cancellation stops waiting, and does not undo a request already sent to the server. `ReducerError.Value` retains structured BSATN errors, while string errors also populate `Message`.

Overlapping subscriptions retain each row until its final subscription releases it. Unsubscribing requests dropped rows so the cache is updated. Compressed frames are limited to 33 MiB after decompression, and invalid row boundaries or message lengths terminate the connection.

Validation commands:

```bash
go test -race ./...
go vet ./...
go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 ./...
# Requires a running 2.11.0 server, the spacetime CLI, and Rust's WASM target:
go test -tags integration -race -count=1 -timeout=15m ./integration/...
```

Integration setup or publish failures fail the test run. The upstream module fixture is pinned to `v2.11.0`; `SPACETIMEDB_REPO_REF` can override it for compatibility investigations.

For released tags, the Rust fixture is copied outside the upstream workspace and built against the matching published SDK version. This avoids resolving unrelated server and JavaScript build dependencies. Set `SPACETIMEDB_WORKSPACE_SDK=true` to explicitly build against the cloned workspace SDK instead. Subprocess output waits are bounded after a context deadline.

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "log"

    "go.digitalxero.dev/spacetimedb-client/bsatn"
    "go.digitalxero.dev/spacetimedb-client/client"
    "go.digitalxero.dev/spacetimedb-client/client/cache"
    "go.digitalxero.dev/spacetimedb-client/types"
)

// 1. Define your table struct and TableDef.

type Player struct {
    ID   uint64
    Name string
    HP   uint32
}

type playerDef struct{}

func (playerDef) TableName() string { return "Player" }

func (playerDef) DecodeRow(r bsatn.Reader) (any, error) {
    id, err := r.GetU64()
    if err != nil {
        return nil, err
    }
    name, err := r.GetString()
    if err != nil {
        return nil, err
    }
    hp, err := r.GetU32()
    if err != nil {
        return nil, err
    }
    return &Player{ID: id, Name: name, HP: hp}, nil
}

func (playerDef) EncodeRow(row any) []byte {
    p := row.(*Player)
    w := bsatn.NewWriter(32)
    w.PutU64(p.ID)
    w.PutString(p.Name)
    w.PutU32(p.HP)
    return w.Bytes()
}

// Implement PrimaryKey for update detection.
func (playerDef) PrimaryKey(row any) any { return row.(*Player).ID }

// 2. Define reducer arguments.

type createPlayerArgs struct {
    Name string
}

func (a *createPlayerArgs) WriteBsatn(w bsatn.Writer) {
    w.PutString(a.Name)
}

func main() {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // 3. Build the connection.
    conn, err := client.NewDbConnection().
        WithUri("ws://localhost:3000").
        WithDatabaseName("my_game").
        OnConnect(func(c client.DbConnection, id types.Identity, token string) {
            fmt.Println("Connected as", id)

            // 5. Subscribe to tables.
            c.Subscribe("SELECT * FROM Player").
                OnApplied(func() {
                    fmt.Println("Got", c.Cache().GetTable("Player").Count(), "players")
                }).
                Build()

            // 6. Call a reducer.
            c.CallReducer("create_player", &createPlayerArgs{Name: "Alice"})
        }).
        Build(ctx)
    if err != nil {
        log.Fatal(err)
    }

    // 4. Register tables before running.
    conn.RegisterTable(playerDef{})

    // Listen for inserts.
    conn.Cache().GetTable("Player").OnInsert(func(row any) {
        p := row.(*Player)
        fmt.Printf("New player: %s (HP=%d)\n", p.Name, p.HP)
    })

    // 7. Run the event loop (blocks until ctx is cancelled or Disconnect is called).
    if err := conn.Run(ctx); err != nil {
        log.Println("Disconnected:", err)
    }
}
```

## Packages

| Package | Description |
|---------|-------------|
| [`bsatn`](https://pkg.go.dev/go.digitalxero.dev/spacetimedb-client/bsatn) | BSATN binary serialization: `Writer`, `Reader`, `Serializable`, and generic encode/decode helpers |
| [`types`](https://pkg.go.dev/go.digitalxero.dev/spacetimedb-client/types) | SpacetimeDB domain types: `Identity`, `ConnectionId`, `Timestamp`, `Uint128`, `Uuid`, algebraic types |
| [`client`](https://pkg.go.dev/go.digitalxero.dev/spacetimedb-client/client) | Primary API: `DbConnection`, subscriptions, reducer calls, one-off queries |
| [`client/cache`](https://pkg.go.dev/go.digitalxero.dev/spacetimedb-client/client/cache) | Client-side row cache with insert/delete/update callbacks |
| [`client/protocol`](https://pkg.go.dev/go.digitalxero.dev/spacetimedb-client/client/protocol) | Wire protocol types (internal; most users only need `Compression` constants) |
| [`client/ws`](https://pkg.go.dev/go.digitalxero.dev/spacetimedb-client/client/ws) | Low-level WebSocket connection (internal; used indirectly through `client`) |

## Architecture

### Event Loop Model

`DbConnection.Run(ctx)` blocks on the calling goroutine and processes WebSocket messages, cache updates, and callbacks sequentially. Commands like `CallReducer` and `Subscribe` use a bounded queue consumed by the event loop. Identity access and cache reference counts use synchronization for concurrent readers.

### Builder Pattern

All public types use the builder pattern with private implementations:

```go
conn, err := client.NewDbConnection().
    WithUri("ws://localhost:3000").
    WithDatabaseName("my_db").
    OnConnect(handler).
    Build(ctx)
```

### BSATN Serialization

BSATN (Binary SpacetimeDB Algebraic Type Notation) is a deterministic, little-endian binary format. All data on the wire is BSATN-encoded. Key mappings:

| Go type | BSATN type | Wire size |
|---------|-----------|-----------|
| `bool` | bool | 1 byte |
| `uint32` | u32 | 4 bytes LE |
| `uint64` | u64 | 8 bytes LE |
| `string` | String | 4-byte len + UTF-8 |
| `[]T` | Array\<T\> | 4-byte count + elements |
| `*T` | Option\<T\> | 1-byte tag [+ value] |
| struct | Product | sequential fields |

Implement `bsatn.Serializable` for your types to use them as reducer arguments or table rows. See the [`bsatn` package docs](https://pkg.go.dev/go.digitalxero.dev/spacetimedb-client/bsatn) for the full type mapping and encoding rules.

## Full API Documentation

See [pkg.go.dev/go.digitalxero.dev/spacetimedb-client](https://pkg.go.dev/go.digitalxero.dev/spacetimedb-client) for complete API reference documentation.
