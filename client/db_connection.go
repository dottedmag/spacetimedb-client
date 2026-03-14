package client

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync/atomic"

	"go.digitalxero.dev/spacetimedb-client/bsatn"
	"go.digitalxero.dev/spacetimedb-client/client/cache"
	"go.digitalxero.dev/spacetimedb-client/client/protocol"
	"go.digitalxero.dev/spacetimedb-client/client/ws"
	"go.digitalxero.dev/spacetimedb-client/types"
)

// DbConnectionBuilder configures and establishes a connection to a
// SpacetimeDB database. Use [NewDbConnection] to create a builder,
// configure it with the With* and On* methods, then call Build to
// connect.
//
// At minimum, WithUri and WithDatabaseName must be called before Build.
// The builder methods can be chained:
//
//	conn, err := client.NewDbConnection().
//		WithUri("ws://localhost:3000").
//		WithDatabaseName("my_db").
//		OnConnect(func(conn client.DbConnection, id types.Identity, token string) {
//			fmt.Println("connected as", id)
//		}).
//		Build(ctx)
type DbConnectionBuilder interface {
	WithUri(uri string) DbConnectionBuilder
	WithDatabaseName(nameOrAddress string) DbConnectionBuilder
	WithToken(token string) DbConnectionBuilder
	WithCompression(c protocol.Compression) DbConnectionBuilder
	OnConnect(fn func(conn DbConnection, identity types.Identity, token string)) DbConnectionBuilder
	OnConnectError(fn func(err error)) DbConnectionBuilder
	OnDisconnect(fn func(conn DbConnection, err error)) DbConnectionBuilder
	Build(ctx context.Context) (DbConnection, error)
}

// DbConnection is the primary interface for interacting with a SpacetimeDB
// database. It provides methods for subscribing to queries, calling reducers,
// executing one-off queries, and accessing the local row cache.
//
// All interaction with the database happens through a single-goroutine event
// loop started by [DbConnection.Run]. Table registrations via RegisterTable
// must happen before calling Run. After Run is called, use Subscribe to
// register queries and CallReducer to invoke server-side functions.
//
// The connection remains active until Disconnect is called or the context
// passed to Run is cancelled.
type DbConnection interface {
	Identity() types.Identity
	ConnectionId() types.ConnectionId
	Token() string
	IsActive() bool
	Subscribe(queries ...string) SubscriptionBuilder
	CallReducer(reducer string, args bsatn.Serializable) error
	OneOffQuery(query string) ([][]byte, error)
	Disconnect() error
	Run(ctx context.Context) error
	RegisterTable(def cache.TableDef)
	Cache() cache.ClientCache
}

// NewDbConnection returns a new [DbConnectionBuilder] with default settings.
// Compression defaults to [protocol.CompressionNone].
func NewDbConnection() DbConnectionBuilder {
	return &dbConnectionBuilder{
		compression: protocol.CompressionNone,
	}
}

type dbConnectionBuilder struct {
	uri            string
	database       string
	token          string
	compression    protocol.Compression
	onConnect      func(DbConnection, types.Identity, string)
	onConnectError func(error)
	onDisconnect   func(DbConnection, error)
}

func (b *dbConnectionBuilder) WithUri(uri string) DbConnectionBuilder {
	b.uri = uri
	return b
}

func (b *dbConnectionBuilder) WithDatabaseName(name string) DbConnectionBuilder {
	b.database = name
	return b
}

func (b *dbConnectionBuilder) WithToken(token string) DbConnectionBuilder {
	b.token = token
	return b
}

func (b *dbConnectionBuilder) WithCompression(c protocol.Compression) DbConnectionBuilder {
	b.compression = c
	return b
}

func (b *dbConnectionBuilder) OnConnect(fn func(DbConnection, types.Identity, string)) DbConnectionBuilder {
	b.onConnect = fn
	return b
}

func (b *dbConnectionBuilder) OnConnectError(fn func(error)) DbConnectionBuilder {
	b.onConnectError = fn
	return b
}

func (b *dbConnectionBuilder) OnDisconnect(fn func(DbConnection, error)) DbConnectionBuilder {
	b.onDisconnect = fn
	return b
}

func (b *dbConnectionBuilder) Build(ctx context.Context) (DbConnection, error) {
	wsConn, err := ws.NewConnection().
		WithUri(b.uri).
		WithDatabaseName(b.database).
		WithToken(b.token).
		Build(ctx)
	if err != nil {
		if b.onConnectError != nil {
			b.onConnectError(err)
		}
		return nil, &ConnectionError{Message: "failed to connect", Err: err}
	}

	conn := &dbConnection{
		ws:             wsConn,
		cache:          cache.NewClientCache(),
		commands:       make(chan *command, 256),
		incoming:       make(chan []byte, 64),
		compression:    b.compression,
		onConnect:      b.onConnect,
		onConnectError: b.onConnectError,
		onDisconnect:   b.onDisconnect,
	}

	return conn, nil
}

// command types for the channel-based event loop.
type commandType int

const (
	cmdCallReducer commandType = iota
	cmdSubscribe
	cmdUnsubscribe
	cmdOneOffQuery
	cmdDisconnect
)

type command struct {
	typ        commandType
	reducer    string
	args       []byte
	queries    []string
	querySetID uint32
	onApplied  func()
	result     chan any
}

// oneOffResult is the internal result passed back through the one-off query result channel.
type oneOffResult struct {
	rows [][]byte
	err  error
}

type dbConnection struct {
	ws          ws.Connection
	cache       cache.ClientCache
	commands    chan *command
	incoming    chan []byte
	compression protocol.Compression

	identity     types.Identity
	connectionID types.ConnectionId
	token        string

	active         atomic.Bool
	nextRequestID  atomic.Uint32
	nextQuerySetID atomic.Uint32

	onConnect      func(DbConnection, types.Identity, string)
	onConnectError func(error)
	onDisconnect   func(DbConnection, error)

	// State maps only accessed from the Run() goroutine event loop.
	subscriptionCallbacks map[uint32]func()             // keyed by request_id
	oneOffCallbacks       map[uint64]chan any            // keyed by messageID (first 8 bytes as uint64)
}

func (c *dbConnection) Identity() types.Identity        { return c.identity }
func (c *dbConnection) ConnectionId() types.ConnectionId { return c.connectionID }
func (c *dbConnection) Token() string                    { return c.token }
func (c *dbConnection) IsActive() bool                   { return c.active.Load() }
func (c *dbConnection) Cache() cache.ClientCache         { return c.cache }

func (c *dbConnection) RegisterTable(def cache.TableDef) {
	c.cache.RegisterTable(def)
}

func (c *dbConnection) CallReducer(reducer string, args bsatn.Serializable) error {
	var encoded []byte
	if args != nil {
		w := bsatn.NewWriter(64)
		args.WriteBsatn(w)
		encoded = w.Bytes()
	}
	c.commands <- &command{
		typ:     cmdCallReducer,
		reducer: reducer,
		args:    encoded,
	}
	return nil
}

func (c *dbConnection) OneOffQuery(query string) ([][]byte, error) {
	result := make(chan any, 1)
	c.commands <- &command{
		typ:     cmdOneOffQuery,
		queries: []string{query},
		result:  result,
	}
	resp := <-result
	switch r := resp.(type) {
	case oneOffResult:
		return r.rows, r.err
	default:
		return nil, &ProtocolError{Message: "unexpected one-off query result type"}
	}
}

func (c *dbConnection) Subscribe(queries ...string) SubscriptionBuilder {
	return &subscriptionBuilder{
		conn:    c,
		queries: queries,
	}
}

func (c *dbConnection) Disconnect() error {
	c.commands <- &command{typ: cmdDisconnect}
	return nil
}

// Run starts the single-goroutine event loop that processes all WebSocket
// messages and commands. It blocks until the context is cancelled or
// [DbConnection.Disconnect] is called.
//
// All server messages (subscription updates, reducer results, identity
// tokens) are processed sequentially in this goroutine. Table cache
// mutations and callbacks are invoked here, so callback functions should
// not block for extended periods.
//
// Run must be called exactly once. Calling it again after it returns
// is undefined behavior.
func (c *dbConnection) Run(ctx context.Context) error {
	c.active.Store(true)
	defer c.active.Store(false)

	c.subscriptionCallbacks = make(map[uint32]func())
	c.oneOffCallbacks = make(map[uint64]chan any)

	// readCtx is derived from context.Background() rather than the caller's
	// ctx so that context cancellation does not kill the TCP connection before
	// the WebSocket close handshake can complete.
	readCtx, readCancel := context.WithCancel(context.Background())
	defer readCancel()

	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		c.readLoop(readCtx)
	}()

	shutdown := func(disconnectErr error) {
		_ = c.ws.Close()
		readCancel()
		<-readDone
		if c.onDisconnect != nil {
			c.onDisconnect(c, disconnectErr)
		}
	}

	for {
		select {
		case <-ctx.Done():
			shutdown(ctx.Err())
			return ctx.Err()

		case cmd := <-c.commands:
			if err := c.handleCommand(ctx, cmd); err != nil {
				return err
			}
			if cmd.typ == cmdDisconnect {
				shutdown(nil)
				return nil
			}

		case msg := <-c.incoming:
			c.handleIncoming(msg)

		case <-readDone:
			if c.onDisconnect != nil {
				c.onDisconnect(c, &ConnectionError{Message: "connection lost"})
			}
			return &ConnectionError{Message: "connection lost"}
		}
	}
}

func (c *dbConnection) readLoop(ctx context.Context) {
	for {
		data, err := c.ws.Recv(ctx)
		if err != nil {
			return
		}
		c.incoming <- data
	}
}

func (c *dbConnection) handleCommand(ctx context.Context, cmd *command) error {
	switch cmd.typ {
	case cmdCallReducer:
		reqID := c.nextRequestID.Add(1)
		msg := &protocol.CallReducer{
			Reducer:   cmd.reducer,
			Args:      cmd.args,
			RequestID: reqID,
			Flags:     0, // FullUpdate
		}
		return c.sendMessage(ctx, msg)

	case cmdSubscribe:
		reqID := c.nextRequestID.Add(1)
		if cmd.onApplied != nil {
			c.subscriptionCallbacks[reqID] = cmd.onApplied
		}
		// Use v1 Subscribe (tag 1) which replaces the entire subscription set.
		msg := &protocol.Subscribe{
			QueryStrings: cmd.queries,
			RequestID:    reqID,
		}
		return c.sendMessage(ctx, msg)

	case cmdUnsubscribe:
		// v1 doesn't have a direct unsubscribe for Subscribe (tag 1).
		// Would need to use UnsubscribeMulti for SubscribeMulti subscriptions.
		return nil

	case cmdOneOffQuery:
		reqID := c.nextRequestID.Add(1)
		// v1 OneOffQuery uses a message_id (byte array) instead of request_id.
		// We encode the request ID as 8 bytes.
		msgID := make([]byte, 8)
		binary.LittleEndian.PutUint64(msgID, uint64(reqID))
		if cmd.result != nil {
			c.oneOffCallbacks[uint64(reqID)] = cmd.result
		}
		msg := &protocol.OneOffQuery{
			MessageID:   msgID,
			QueryString: cmd.queries[0],
		}
		return c.sendMessage(ctx, msg)

	case cmdDisconnect:
		return nil
	}
	return nil
}

func (c *dbConnection) sendMessage(ctx context.Context, msg protocol.ClientMessage) error {
	data := bsatn.Encode(msg)
	return c.ws.Send(ctx, data)
}

func (c *dbConnection) handleIncoming(data []byte) {
	decompressed, err := protocol.DecompressMessage(data)
	if err != nil {
		if c.onConnectError != nil {
			c.onConnectError(&ProtocolError{Message: "decompress error: " + err.Error()})
		}
		return
	}

	r := bsatn.NewReader(decompressed)
	msg, err := protocol.ReadServerMessage(r)
	if err != nil {
		if c.onConnectError != nil {
			c.onConnectError(&ProtocolError{Message: fmt.Sprintf("read server message error (len=%d): %s", len(decompressed), err.Error())})
		}
		return
	}

	switch m := msg.(type) {
	case *protocol.IdentityToken:
		c.identity = m.Identity
		c.connectionID = m.ConnectionID
		c.token = m.Token
		if c.onConnect != nil {
			c.onConnect(c, m.Identity, m.Token)
		}

	case *protocol.InitialSubscription:
		// InitialSubscription is the response to a Subscribe (tag 1) message.
		// Apply all rows from the database update to the cache.
		c.applyV1DatabaseUpdate(&m.DatabaseUpdate)
		// Fire the onApplied callback for this request.
		if cb, ok := c.subscriptionCallbacks[m.RequestID]; ok {
			cb()
			delete(c.subscriptionCallbacks, m.RequestID)
		}

	case *protocol.SubscribeMultiApplied:
		// SubscribeMultiApplied is the response to SubscribeMulti (tag 4).
		c.applyV1DatabaseUpdate(&m.Update)
		if cb, ok := c.subscriptionCallbacks[m.RequestID]; ok {
			cb()
			delete(c.subscriptionCallbacks, m.RequestID)
		}

	case *protocol.V1TransactionUpdate:
		// Full transaction update from a reducer run.
		switch status := m.Status.(type) {
		case *protocol.V1Committed:
			c.applyV1DatabaseUpdate(&status.DatabaseUpdate)
		}

	case *protocol.V1TransactionUpdateLight:
		// Light transaction update (just the database changes).
		c.applyV1DatabaseUpdate(&m.DatabaseUpdate)

	case *protocol.V1SubscriptionError:
		// Remove subscription callback on error.
		if m.RequestID != nil {
			delete(c.subscriptionCallbacks, *m.RequestID)
		}

	case *protocol.OneOffQueryResponse:
		// Match by message_id (first 8 bytes as uint64).
		if len(m.MessageID) >= 8 {
			key := binary.LittleEndian.Uint64(m.MessageID[:8])
			if ch, ok := c.oneOffCallbacks[key]; ok {
				if m.Error != nil {
					ch <- oneOffResult{err: &ProtocolError{Message: *m.Error}}
				} else {
					var rows [][]byte
					for _, table := range m.Tables {
						if table.Rows != nil {
							rows = append(rows, table.Rows.Rows()...)
						}
					}
					ch <- oneOffResult{rows: rows}
				}
				delete(c.oneOffCallbacks, key)
			}
		}
	}
}

// applyV1DatabaseUpdate applies a v1 DatabaseUpdate to the cache.
func (c *dbConnection) applyV1DatabaseUpdate(update *protocol.V1DatabaseUpdate) {
	if update == nil {
		return
	}
	for _, tableUpdate := range update.Tables {
		tc := c.cache.GetTable(tableUpdate.TableName)
		if tc == nil {
			continue
		}
		for _, cqu := range tableUpdate.Updates {
			qu, err := protocol.DecompressV1QueryUpdate(cqu)
			if err != nil {
				continue
			}
			if qu == nil {
				continue
			}

			// Check if this table supports PK-based update detection
			pkDef, hasPK := c.cache.GetTableDef(tableUpdate.TableName)

			if hasPK && qu.Deletes != nil && qu.Inserts != nil &&
				qu.Deletes.Len() > 0 && qu.Inserts.Len() > 0 {
				c.applyPKUpdate(tc, pkDef, qu)
			} else {
				// Simple delete-then-insert
				if qu.Deletes != nil {
					for _, rowData := range qu.Deletes.Rows() {
						reader := bsatn.NewReader(rowData)
						row, err := c.cache.DecodeRow(tableUpdate.TableName, reader)
						if err != nil {
							continue
						}
						tc.ApplyDelete(rowData, row)
					}
				}
				if qu.Inserts != nil {
					for _, rowData := range qu.Inserts.Rows() {
						reader := bsatn.NewReader(rowData)
						row, err := c.cache.DecodeRow(tableUpdate.TableName, reader)
						if err != nil {
							continue
						}
						tc.ApplyInsert(rowData, row)
					}
				}
			}
		}
	}
}

// applyPKUpdate handles PK-based update detection for tables with primary keys.
func (c *dbConnection) applyPKUpdate(tc cache.TableCache, pkDef cache.TableDefWithPK, qu *protocol.V1Uncompressed) {
	type deleteEntry struct {
		rowBytes []byte
		row      any
	}
	deletedByPK := map[any]deleteEntry{}
	for _, rowData := range qu.Deletes.Rows() {
		reader := bsatn.NewReader(rowData)
		row, err := pkDef.DecodeRow(reader)
		if err != nil {
			continue
		}
		pk := pkDef.PrimaryKey(row)
		deletedByPK[pk] = deleteEntry{rowData, row}
	}
	for _, rowData := range qu.Inserts.Rows() {
		reader := bsatn.NewReader(rowData)
		row, err := pkDef.DecodeRow(reader)
		if err != nil {
			continue
		}
		pk := pkDef.PrimaryKey(row)
		if old, isUpdate := deletedByPK[pk]; isUpdate {
			tc.ApplyUpdate(old.rowBytes, old.row, rowData, row)
			delete(deletedByPK, pk)
		} else {
			tc.ApplyInsert(rowData, row)
		}
	}
	for _, old := range deletedByPK {
		tc.ApplyDelete(old.rowBytes, old.row)
	}
}

// subscriptionBuilder implements SubscriptionBuilder.
type subscriptionBuilder struct {
	conn      *dbConnection
	queries   []string
	onApplied func()
}

func (sb *subscriptionBuilder) OnApplied(fn func()) SubscriptionBuilder {
	sb.onApplied = fn
	return sb
}

func (sb *subscriptionBuilder) Build() (SubscriptionHandle, error) {
	qsID := sb.conn.nextQuerySetID.Add(1)

	sb.conn.commands <- &command{
		typ:        cmdSubscribe,
		queries:    sb.queries,
		querySetID: qsID,
		onApplied:  sb.onApplied,
	}

	return &subscriptionHandle{
		conn:       sb.conn,
		querySetID: qsID,
		active:     true,
	}, nil
}

// subscriptionHandle implements SubscriptionHandle.
type subscriptionHandle struct {
	conn       *dbConnection
	querySetID uint32
	active     bool
}

func (sh *subscriptionHandle) Unsubscribe() error {
	sh.active = false
	sh.conn.commands <- &command{
		typ:        cmdUnsubscribe,
		querySetID: sh.querySetID,
	}
	return nil
}

func (sh *subscriptionHandle) IsActive() bool {
	return sh.active
}

// Ensure interfaces are satisfied at compile time.
var (
	_ DbConnectionBuilder = (*dbConnectionBuilder)(nil)
	_ DbConnection        = (*dbConnection)(nil)
	_ SubscriptionBuilder = (*subscriptionBuilder)(nil)
	_ SubscriptionHandle  = (*subscriptionHandle)(nil)
	_ error               = (*ConnectionError)(nil)
	_ error               = (*ReducerError)(nil)
	_ error               = (*SubscriptionError)(nil)
	_ error               = (*ProtocolError)(nil)
)
