package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

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
	// WithProtocol selects v2.bsatn.spacetimedb (default) or v3.bsatn.spacetimedb.
	WithProtocol(protocol string) DbConnectionBuilder
	WithUri(uri string) DbConnectionBuilder
	WithDatabaseName(nameOrAddress string) DbConnectionBuilder
	WithToken(token string) DbConnectionBuilder
	// WithAutomaticReconnect preserves subscriptions and cache across transport loss.
	WithAutomaticReconnect() DbConnectionBuilder
	// WithTokenProvider obtains credentials before every connection attempt.
	WithTokenProvider(fn func(context.Context) (string, error)) DbConnectionBuilder
	WithCompression(c protocol.Compression) DbConnectionBuilder
	OnConnect(fn func(conn DbConnection, identity types.Identity, token string)) DbConnectionBuilder
	OnConnectError(fn func(err error)) DbConnectionBuilder
	OnDisconnect(fn func(conn DbConnection, err error)) DbConnectionBuilder
	// OnReducerError registers a handler invoked when a reducer call (which is
	// fire-and-forget) fails on the server — a structured error return or a host
	// internal error such as "no such reducer". This is distinct from
	// OnConnectError: a reducer failure does not affect the connection, so the
	// handler should not treat it as fatal. The error is a [*ReducerError].
	OnReducerError(fn func(err error)) DbConnectionBuilder
	// OnTxStart runs before a complete cache update batch and its row and
	// subscription callbacks. It also covers initial snapshots, reconnect
	// reconciliation, and rows removed by unsubscribe or subscription failure.
	OnTxStart(fn func()) DbConnectionBuilder
	// OnTxFinish runs after the batch, including when row decoding fails.
	// A nonnil error means the cache may be partially updated and Run will stop.
	// Both hooks run synchronously in Run; they must not wait for SDK responses.
	OnTxFinish(fn func(err error)) DbConnectionBuilder
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
	CallProcedure(ctx context.Context, procedure string, args bsatn.Serializable) ([]byte, error)
	OneOffQuery(query string) ([][]byte, error)
	OneOffQueryContext(ctx context.Context, query string) ([][]byte, error)
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
	automaticReconnect bool
	tokenProvider      func(context.Context) (string, error)
	protocol           string
	uri                string
	database           string
	token              string
	compression        protocol.Compression
	onConnect          func(DbConnection, types.Identity, string)
	onConnectError     func(error)
	onDisconnect       func(DbConnection, error)
	onReducerError     func(error)
	onTxStart          func()
	onTxFinish         func(error)
}

func (b *dbConnectionBuilder) WithAutomaticReconnect() DbConnectionBuilder {
	b.automaticReconnect = true
	return b
}

func (b *dbConnectionBuilder) WithTokenProvider(fn func(context.Context) (string, error)) DbConnectionBuilder {
	b.tokenProvider = fn
	return b
}

func (b *dbConnectionBuilder) WithUri(uri string) DbConnectionBuilder {
	b.uri = uri
	return b
}

func (b *dbConnectionBuilder) WithProtocol(protocol string) DbConnectionBuilder {
	b.protocol = protocol
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

func (b *dbConnectionBuilder) OnReducerError(fn func(error)) DbConnectionBuilder {
	b.onReducerError = fn
	return b
}

func (b *dbConnectionBuilder) OnTxStart(fn func()) DbConnectionBuilder {
	b.onTxStart = fn
	return b
}

func (b *dbConnectionBuilder) OnTxFinish(fn func(error)) DbConnectionBuilder {
	b.onTxFinish = fn
	return b
}

func (b *dbConnectionBuilder) Build(ctx context.Context) (DbConnection, error) {
	// A built connection owns its settings even if the builder is reused.
	settings := *b
	b = &settings
	if b.protocol == "" {
		b.protocol = "v2.bsatn.spacetimedb"
	}
	if b.protocol != "v2.bsatn.spacetimedb" && b.protocol != "v3.bsatn.spacetimedb" {
		return nil, &ConnectionError{Message: "unsupported protocol"}
	}
	compression := "none"
	switch b.compression {
	case protocol.CompressionNone:
	case protocol.CompressionBrotli:
		compression = "brotli"
	case protocol.CompressionGzip:
		compression = "gzip"
	default:
		return nil, &ConnectionError{Message: "unsupported compression"}
	}
	dial := func(ctx context.Context, token string) (ws.Connection, error) {
		if b.tokenProvider != nil {
			var err error
			token, err = b.tokenProvider(ctx)
			if err != nil {
				return nil, fmt.Errorf("token provider: %w", err)
			}
		}
		return ws.NewConnection().
			WithUri(b.uri).
			WithDatabaseName(b.database).
			WithToken(token).
			WithCompression(compression).
			WithProtocol(b.protocol).
			Build(ctx)
	}
	wsConn, err := dial(ctx, b.token)
	if err != nil {
		if b.onConnectError != nil {
			b.onConnectError(err)
		}
		return nil, &ConnectionError{Message: "failed to connect", Err: err}
	}

	conn := &dbConnection{
		automaticReconnect: b.automaticReconnect,
		dial:               dial,
		token:              b.token,
		protocol:           b.protocol,
		ws:                 wsConn,
		cache:              cache.NewClientCache(),
		commands:           make(chan *command, 256),
		incoming:           make(chan []byte, 64),
		compression:        b.compression,
		onConnect:          b.onConnect,
		onConnectError:     b.onConnectError,
		onDisconnect:       b.onDisconnect,
		onReducerError:     b.onReducerError,
		onTxStart:          b.onTxStart,
		onTxFinish:         b.onTxFinish,
	}

	return conn, nil
}

// command types for the channel-based event loop.
type commandType int

const (
	cmdCallReducer commandType = iota
	cmdCallProcedure
	cmdSubscribe
	cmdUnsubscribe
	cmdOneOffQuery
	cmdDisconnect
)

type command struct {
	handle     *subscriptionHandle
	ctx        context.Context
	typ        commandType
	reducer    string
	args       []byte
	queries    []string
	querySetID uint32
	onApplied  func()
	onError    func(error)
	result     chan any
}

// activeSubscription tracks a single independent subscription registered via
// [SubscriptionBuilder.Build]. It is keyed by its server-side query_id and is
// only ever accessed from the Run() goroutine event loop.
type activeSubscription struct {
	replaying bool
	rows      map[string]map[string][]byte
	handle    *subscriptionHandle
	queryID   uint32
	queries   []string
	onApplied func()
	onError   func(error)
	applied   bool
}

// oneOffResult is the internal result passed back through the one-off query result channel.
type oneOffResult struct {
	rows [][]byte
	err  error
}

// procedureResult is the internal result passed back through the procedure call
// result channel. value holds the BSATN-encoded return value on success.
type procedureResult struct {
	value []byte
	err   error
}

type dbConnection struct {
	disconnectSignal   chan struct{}
	disconnectOnce     sync.Once
	established        bool
	automaticReconnect bool
	dial               func(context.Context, string) (ws.Connection, error)
	reconnecting       bool
	rowDecodeErr       error // accessed only by the event loop
	protocol           string
	stateMu            sync.RWMutex
	doneOnce           sync.Once
	done               chan struct{}
	started            atomic.Bool
	closed             atomic.Bool
	ws                 ws.Connection
	cache              cache.ClientCache
	commands           chan *command
	incoming           chan []byte
	compression        protocol.Compression

	identity     types.Identity
	connectionID types.ConnectionId
	token        string

	active         atomic.Bool
	nextRequestID  atomic.Uint32
	nextQuerySetID atomic.Uint32

	onConnect      func(DbConnection, types.Identity, string)
	onConnectError func(error)
	onDisconnect   func(DbConnection, error)
	onReducerError func(error)
	onTxStart      func()
	onTxFinish     func(error)

	// State maps only accessed from the Run() goroutine event loop, so they
	// need no synchronization: there is exactly one writer goroutine.
	subscriptions      map[uint32]*activeSubscription // keyed by query_set_id
	oneOffCallbacks    map[uint32]chan any            // keyed by request_id
	procedureCallbacks map[uint32]chan any            // keyed by request_id, for routing procedure results
	procedureContexts  map[uint32]context.Context
	queryContexts      map[uint32]context.Context
	reducerNames       map[uint32]string // request_id -> reducer name, for reducer-result errors
}

func (c *dbConnection) Identity() types.Identity {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	return c.identity
}
func (c *dbConnection) ConnectionId() types.ConnectionId {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	return c.connectionID
}
func (c *dbConnection) Token() string            { c.stateMu.RLock(); defer c.stateMu.RUnlock(); return c.token }
func (c *dbConnection) IsActive() bool           { return c.active.Load() }
func (c *dbConnection) Cache() cache.ClientCache { return c.cache }

func (c *dbConnection) RegisterTable(def cache.TableDef) {
	c.cache.RegisterTable(def)
}

func (c *dbConnection) doneChannel() <-chan struct{} {
	c.doneOnce.Do(func() { c.done = make(chan struct{}); c.disconnectSignal = make(chan struct{}) })
	return c.done
}

// enqueue never blocks the event loop when called from a callback.
func (c *dbConnection) enqueue(cmd *command) error {
	if c.closed.Load() {
		return &ConnectionError{Message: "connection is closed"}
	}
	select {
	case <-c.doneChannel():
		return &ConnectionError{Message: "connection is closed"}
	case c.commands <- cmd:
		return nil
	default:
		return &ConnectionError{Message: "command queue is full"}
	}
}

func (c *dbConnection) CallReducer(reducer string, args bsatn.Serializable) error {
	var encoded []byte
	if args != nil {
		w := bsatn.NewWriter(64)
		args.WriteBsatn(w)
		encoded = w.Bytes()
	}
	return c.enqueue(&command{
		typ:     cmdCallReducer,
		reducer: reducer,
		args:    encoded,
	})
}

// CallProcedure invokes a procedure on the server and blocks until its result
// arrives. Unlike CallReducer (which is fire-and-forget), a procedure returns a
// value: the returned bytes are the BSATN encoding of the procedure's declared
// return type, which generated callers decode into a typed value.
//
// The call blocks until the server sends the matching ProcedureResult, ctx is
// cancelled, or the connection drops. A non-success status (host/internal error
// or out-of-energy) is returned as a *ProcedureError.
//
// CallProcedure must not be called from within the Run() goroutine (e.g. from a
// subscription callback), since it blocks waiting for that goroutine to process
// the response.
func (c *dbConnection) CallProcedure(ctx context.Context, procedure string, args bsatn.Serializable) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var encoded []byte
	if args != nil {
		w := bsatn.NewWriter(64)
		args.WriteBsatn(w)
		encoded = w.Bytes()
	}

	result := make(chan any, 1)
	cmd := &command{
		ctx:     ctx,
		typ:     cmdCallProcedure,
		reducer: procedure,
		args:    encoded,
		result:  result,
	}

	if err := c.enqueue(cmd); err != nil {
		return nil, err
	}

	select {
	case resp := <-result:
		r, ok := resp.(procedureResult)
		if !ok {
			return nil, &ProtocolError{Message: "unexpected procedure result type"}
		}
		if pe, ok := r.err.(*ProcedureError); ok && pe.ProcedureName == "" {
			pe.ProcedureName = procedure
		}
		return r.value, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.doneChannel():
		return nil, &ConnectionError{Message: "connection is closed"}
	}
}

func (c *dbConnection) OneOffQuery(query string) ([][]byte, error) {
	return c.OneOffQueryContext(context.Background(), query)
}

// OneOffQueryContext performs a query with an independently cancellable wait.
func (c *dbConnection) OneOffQueryContext(ctx context.Context, query string) ([][]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	result := make(chan any, 1)
	if err := c.enqueue(&command{
		typ:     cmdOneOffQuery,
		ctx:     ctx,
		queries: []string{query},
		result:  result,
	}); err != nil {
		return nil, err
	}
	var resp any
	select {
	case resp = <-result:
	case <-c.doneChannel():
		return nil, &ConnectionError{Message: "connection is closed"}
	case <-ctx.Done():
		return nil, ctx.Err()
	}
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
	if c.closed.Load() {
		return nil
	}
	if !c.started.Load() {
		c.doneChannel()
		if c.closed.CompareAndSwap(false, true) {
			close(c.done)
			return c.ws.Close()
		}
		return nil
	}
	c.doneChannel()
	c.disconnectOnce.Do(func() { close(c.disconnectSignal) })
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
// returns a ConnectionError.
func (c *dbConnection) Run(ctx context.Context) (runErr error) {
	if !c.started.CompareAndSwap(false, true) || c.closed.Load() {
		return &ConnectionError{Message: "Run may only be called once on an open connection"}
	}
	c.doneChannel()
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-c.disconnectSignal:
			cancel()
		case <-runCtx.Done():
		}
	}()
	c.active.Store(true)
	defer c.active.Store(false)

	c.subscriptions = make(map[uint32]*activeSubscription)
	c.oneOffCallbacks = make(map[uint32]chan any)
	c.procedureCallbacks = make(map[uint32]chan any)
	c.procedureContexts = make(map[uint32]context.Context)
	c.queryContexts = make(map[uint32]context.Context)
	c.reducerNames = make(map[uint32]string)

	defer func() {
		c.active.Store(false)
		if c.closed.CompareAndSwap(false, true) {
			close(c.done)
		}
		_ = c.ws.Close()
		pendingErr := runErr
		if pendingErr == nil {
			pendingErr = &ConnectionError{Message: "connection is closed"}
		}
		for _, ch := range c.oneOffCallbacks {
			ch <- oneOffResult{err: pendingErr}
		}
		for _, ch := range c.procedureCallbacks {
			ch <- procedureResult{err: pendingErr}
		}
		clear(c.oneOffCallbacks)
		clear(c.procedureCallbacks)
		clear(c.procedureContexts)
		clear(c.queryContexts)
		clear(c.reducerNames)
		for _, sub := range c.subscriptions {
			if sub.handle != nil {
				sub.handle.active.Store(false)
			}
		}
		clear(c.subscriptions)
		if c.onDisconnect != nil {
			c.onDisconnect(c, runErr)
		}
	}()
	for {
		err := c.runSession(runCtx)
		select {
		case <-c.disconnectSignal:
			return nil
		default:
		}
		var transport *ConnectionError
		if !c.automaticReconnect || !errors.As(err, &transport) || ctx.Err() != nil {
			return err
		}
		c.active.Store(false)
		c.failPending(err)
		if c.onDisconnect != nil {
			c.onDisconnect(c, err)
		}
		if err := c.reconnect(runCtx); err != nil {
			select {
			case <-c.disconnectSignal:
				return nil
			default:
			}
			return err
		}
	}
}

func (c *dbConnection) runSession(ctx context.Context) error {
	readCtx, readCancel := context.WithCancel(ctx)
	readDone := make(chan struct{})
	var readErr error
	go func() {
		defer close(readDone)
		readErr = c.readLoop(readCtx)
	}()
	defer func() {
		readCancel()
		_ = c.ws.Close()
		<-readDone
	}()
	cleanupTicker := time.NewTicker(time.Second)
	defer cleanupTicker.Stop()

	for {
		commands := c.commands
		if c.reconnecting {
			commands = nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-cleanupTicker.C:
			for id, pendingCtx := range c.queryContexts {
				if pendingCtx.Err() != nil {
					delete(c.oneOffCallbacks, id)
					delete(c.queryContexts, id)
				}
			}
			for id, pendingCtx := range c.procedureContexts {
				if pendingCtx.Err() != nil {
					delete(c.procedureCallbacks, id)
					delete(c.procedureContexts, id)
				}
			}

		case cmd := <-commands:
			if err := c.handleCommand(ctx, cmd); err != nil {
				return err
			}
			if cmd.typ == cmdDisconnect {
				return nil
			}

		case msg := <-c.incoming:
			if err := c.handleIncoming(msg); err != nil {
				return err
			}

		case <-readDone:
			for len(c.incoming) > 0 {
				if err := c.handleIncoming(<-c.incoming); err != nil {
					return err
				}
			}
			return &ConnectionError{Message: "connection lost", Err: readErr}
		}
	}
}

func (c *dbConnection) readLoop(ctx context.Context) error {
	for {
		data, err := c.ws.Recv(ctx)
		if err != nil {
			// DIAG: the reason the read loop is ending (and thus why Run reports the
			// generic "connection lost"). ctx.Err() distinguishes an intentional
			// shutdown from a real socket failure.
			slog.Error("dbConnection.readLoop: Recv failed, ending net loop", "err", err, "ctxErr", ctx.Err())
			return err
		}
		select {
		case c.incoming <- data:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (c *dbConnection) handleCommand(ctx context.Context, cmd *command) error {
	switch cmd.typ {
	case cmdCallReducer:
		reqID := c.nextRequestID.Add(1)
		// Remember the reducer name so a failing ReducerResult (which only
		// carries request_id) can report which reducer failed.
		c.reducerNames[reqID] = cmd.reducer
		msg := &protocol.CallReducer{
			RequestID: reqID,
			Flags:     0,
			Reducer:   cmd.reducer,
			Args:      cmd.args,
		}
		return c.sendMessage(ctx, msg)

	case cmdCallProcedure:
		if cmd.ctx != nil && cmd.ctx.Err() != nil {
			return nil
		}
		reqID := c.nextRequestID.Add(1)
		if cmd.result != nil {
			c.procedureCallbacks[reqID] = cmd.result
			if cmd.ctx != nil {
				c.procedureContexts[reqID] = cmd.ctx
			}
		}
		msg := &protocol.CallProcedure{
			RequestID: reqID,
			Flags:     0,
			Procedure: cmd.reducer,
			Args:      cmd.args,
		}
		return c.sendMessage(ctx, msg)

	case cmdSubscribe:
		reqID := c.nextRequestID.Add(1)
		// Register the subscription keyed by its query_set_id so it can be tracked,
		// unsubscribed, and replayed independently of any other subscription.
		c.subscriptions[cmd.querySetID] = &activeSubscription{
			queryID:   cmd.querySetID,
			queries:   cmd.queries,
			onApplied: cmd.onApplied,
			onError:   cmd.onError,
			handle:    cmd.handle,
		}
		msg := &protocol.Subscribe{
			RequestID:    reqID,
			QuerySetID:   cmd.querySetID,
			QueryStrings: cmd.queries,
		}
		return c.sendMessage(ctx, msg)

	case cmdUnsubscribe:
		reqID := c.nextRequestID.Add(1)
		// The registry entry is removed when the server confirms with
		// UnsubscribeApplied (see dispatchServerMessage).
		msg := &protocol.Unsubscribe{
			RequestID:  reqID,
			QuerySetID: cmd.querySetID,
			Flags:      protocol.UnsubscribeSendDroppedRows,
		}
		return c.sendMessage(ctx, msg)

	case cmdOneOffQuery:
		if cmd.ctx != nil && cmd.ctx.Err() != nil {
			return nil
		}
		reqID := c.nextRequestID.Add(1)
		if cmd.result != nil {
			c.oneOffCallbacks[reqID] = cmd.result
			if cmd.ctx != nil {
				c.queryContexts[reqID] = cmd.ctx
			}
		}
		msg := &protocol.OneOffQuery{
			RequestID:   reqID,
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
	if err := c.ws.Send(ctx, data); err != nil {
		return &ConnectionError{Message: "send failed", Err: err}
	}
	return nil
}

func (c *dbConnection) handleIncoming(data []byte) error {
	c.rowDecodeErr = nil
	decompressed, err := protocol.DecompressMessage(data)
	if err != nil {
		return &ProtocolError{Message: "decompress error", Err: err}
	}

	r := bsatn.NewReader(decompressed)
	if c.protocol == "v3.bsatn.spacetimedb" {
		if r.Remaining() == 0 {
			return &ProtocolError{Message: "empty v3 payload"}
		}
		for r.Remaining() > 0 {
			msg, err := protocol.ReadServerMessage(r)
			if err != nil {
				return &ProtocolError{Message: "read v3 server message", Err: err}
			}
			c.dispatchServerMessage(msg)
			if c.rowDecodeErr != nil {
				return c.rowDecodeErr
			}
		}
		return nil
	}
	msg, err := protocol.ReadServerMessage(r)
	if err != nil {
		return &ProtocolError{Message: fmt.Sprintf("read server message error (len=%d)", len(decompressed)), Err: err}
	}
	if r.Remaining() != 0 {
		return &ProtocolError{Message: "trailing bytes in v2 server message"}
	}

	c.dispatchServerMessage(msg)
	return c.rowDecodeErr
}

// dispatchServerMessage applies a decoded server message to connection state,
// the row cache, and any registered subscription callbacks. It is only called
// from the Run() goroutine event loop.
func (c *dbConnection) dispatchServerMessage(msg protocol.ServerMessage) {
	switch m := msg.(type) {
	case *protocol.TransactionUpdate, *protocol.SubscribeApplied, *protocol.SubscribeBatchApplied,
		*protocol.UnsubscribeApplied, *protocol.SubscriptionError:
	case *protocol.ReducerResult:
		if _, ok := m.Outcome.(*protocol.ReducerOk); !ok {
			c.dispatchMessage(msg)
			return
		}
	default:
		c.dispatchMessage(msg)
		return
	}
	if c.onTxStart != nil {
		c.onTxStart()
	}
	defer func() {
		if c.onTxFinish != nil {
			c.onTxFinish(c.rowDecodeErr)
		}
	}()
	c.dispatchMessage(msg)
}

// Batch members use this dispatcher so their shared boundary is not nested.
func (c *dbConnection) dispatchMessage(msg protocol.ServerMessage) {
	switch m := msg.(type) {
	case *protocol.SubscribeBatchApplied:
		for _, result := range m.Results {
			if result.Rows != nil {
				c.dispatchMessage(&protocol.SubscribeApplied{RequestID: m.RequestID, QuerySetID: result.QuerySetID, Rows: *result.Rows})
			}
			if result.Error != nil {
				c.dispatchMessage(&protocol.SubscriptionError{RequestID: &m.RequestID, QuerySetID: result.QuerySetID, Error: *result.Error})
			}
			if c.rowDecodeErr != nil {
				return
			}
		}
	case *protocol.InitialConnection:
		if c.reconnecting && c.established && c.Identity().Bytes() != m.Identity.Bytes() {
			c.rowDecodeErr = &ProtocolError{Message: "identity changed during reconnect"}
			return
		}
		c.stateMu.Lock()
		c.identity = m.Identity
		c.connectionID = m.ConnectionID
		c.token = m.Token
		c.stateMu.Unlock()
		c.established = true
		if c.reconnecting {
			replayCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			err := c.resubscribeAll(replayCtx)
			cancel()
			if err != nil {
				c.rowDecodeErr = err
				return
			}
			c.reconnecting = false
		}
		c.active.Store(true)
		if c.onConnect != nil {
			c.onConnect(c, m.Identity, m.Token)
		}

	case *protocol.SubscribeApplied:
		if sub := c.subscriptions[m.QuerySetID]; sub != nil && sub.replaying {
			c.reconcileSubscription(sub, &m.Rows)
			if c.rowDecodeErr != nil {
				return
			}
			sub.replaying = false
			sub.applied = true
			if sub.onApplied != nil {
				sub.onApplied()
			}
			return
		}
		// Initial matching rows for a newly-applied query set: apply them as
		// inserts and fire onApplied for the subscription keyed by query_set_id.
		c.applyQueryRows(&m.Rows)
		if c.rowDecodeErr != nil {
			return
		}
		if sub, ok := c.subscriptions[m.QuerySetID]; ok {
			for _, table := range m.Rows.Tables {
				c.trackSubscriptionRows(sub, table.TableName, table.Rows, nil)
			}
			sub.applied = true
			if sub.onApplied != nil {
				sub.onApplied()
			}
		}

	case *protocol.UnsubscribeApplied:
		// Confirms removal of a query set. The optional rows (sent only when the
		// Unsubscribe requested dropped rows) are removed from the cache.
		if m.Rows != nil {
			c.applyQueryRowsAsDeletes(m.Rows)
		}
		if sub := c.subscriptions[m.QuerySetID]; sub != nil && sub.handle != nil {
			sub.handle.active.Store(false)
		}
		delete(c.subscriptions, m.QuerySetID)

	case *protocol.TransactionUpdate:
		// Broadcast row changes for the client's subscribed query sets.
		for i := range m.QuerySets {
			c.applyQuerySetUpdate(&m.QuerySets[i])
			if c.rowDecodeErr != nil {
				return
			}
		}

	case *protocol.ReducerResult:
		// Response to this client's CallReducer. CallReducer is fire-and-forget,
		// so on a committed run we apply the caller's embedded transaction update,
		// and on a failed run we surface the error via onReducerError — never via
		// onConnectError, because a reducer failure does not affect the connection.
		name := c.reducerNames[m.RequestID]
		delete(c.reducerNames, m.RequestID)
		switch outcome := m.Outcome.(type) {
		case *protocol.ReducerOk:
			for i := range outcome.TransactionUpdate.QuerySets {
				c.applyQuerySetUpdate(&outcome.TransactionUpdate.QuerySets[i])
				if c.rowDecodeErr != nil {
					return
				}
			}
		case *protocol.ReducerErr:
			// Value carries the reducer's own reason — a BSATN string holding the
			// returned error's Error() text, which for modules that return coded
			// errors is a stable code such as "conflict" or "rate_limited".
			// Surfacing it lets a client tell the user what actually went wrong
			// instead of a fixed sentence.
			if c.onReducerError != nil {
				c.onReducerError(&ReducerError{ReducerName: name, Message: reducerErrMessage(outcome.Value), Value: append([]byte(nil), outcome.Value...)})
			}
		case *protocol.ReducerInternalError:
			if c.onReducerError != nil {
				c.onReducerError(&ReducerError{ReducerName: name, Message: outcome.Message})
			}
		}

	case *protocol.SubscriptionError:
		// Resolve the failed subscription by query_set_id, fire onError, drop it.
		if sub, ok := c.subscriptions[m.QuerySetID]; ok {
			if sub.handle != nil {
				sub.handle.active.Store(false)
			}
			for tableName, rows := range sub.rows {
				tc := c.cache.GetTable(tableName)
				if tc == nil {
					continue
				}
				for _, data := range rows {
					row, err := c.decodeCacheRow(tableName, bsatn.NewReader(data))
					if err != nil {
						return
					}
					tc.ApplyDelete(data, row)
				}
			}
			if sub.onError != nil {
				sub.onError(&SubscriptionError{QuerySetID: m.QuerySetID, Message: m.Error})
			}
			delete(c.subscriptions, m.QuerySetID)
		}

	case *protocol.ProcedureResult:
		// Match by request_id and deliver the procedure result to the blocked
		// CallProcedure caller. The result channel is buffered (cap 1), so a late
		// result (after the caller's ctx was cancelled) never blocks this loop.
		if ch, ok := c.procedureCallbacks[m.RequestID]; ok {
			switch status := m.Status.(type) {
			case *protocol.ProcedureReturned:
				ch <- procedureResult{value: status.Value}
			case *protocol.ProcedureInternalError:
				ch <- procedureResult{err: &ProcedureError{Message: status.Message}}
			default:
				ch <- procedureResult{err: &ProtocolError{Message: "unknown procedure status"}}
			}
			delete(c.procedureCallbacks, m.RequestID)
			delete(c.procedureContexts, m.RequestID)
		}

	case *protocol.OneOffQueryResult:
		// Match by request_id.
		if ch, ok := c.oneOffCallbacks[m.RequestID]; ok {
			if m.Error != nil {
				ch <- oneOffResult{err: &ProtocolError{Message: *m.Error}}
			} else {
				var rows [][]byte
				if m.Rows != nil {
					for _, table := range m.Rows.Tables {
						if table.Rows != nil {
							rows = append(rows, table.Rows.Rows()...)
						}
					}
				}
				ch <- oneOffResult{rows: rows}
			}
			delete(c.oneOffCallbacks, m.RequestID)
			delete(c.queryContexts, m.RequestID)
		}
	}
}

func (c *dbConnection) decodeCacheRow(table string, r bsatn.Reader) (any, error) {
	row, err := c.cache.DecodeRow(table, r)
	if err == nil && r.Remaining() != 0 {
		err = fmt.Errorf("unexpected trailing row bytes")
	}
	if err != nil {
		c.rowDecodeErr = &ProtocolError{Message: "decode row for " + table, Err: err}
	}
	return row, err
}

// applyQueryRows applies the initial rows of a SubscribeApplied as inserts.
func (c *dbConnection) applyQueryRows(qr *protocol.QueryRows) {
	if qr == nil {
		return
	}
	for _, table := range qr.Tables {
		tc := c.cache.GetTable(table.TableName)
		if tc == nil || table.Rows == nil {
			continue
		}
		for _, rowData := range table.Rows.Rows() {
			reader := bsatn.NewReader(rowData)
			row, err := c.decodeCacheRow(table.TableName, reader)
			if err != nil {
				return
			}
			tc.ApplyInsert(rowData, row)
		}
	}
}

// applyQueryRowsAsDeletes removes the rows of a QueryRows (e.g. the dropped rows
// returned by an UnsubscribeApplied) from the cache.
func (c *dbConnection) applyQueryRowsAsDeletes(qr *protocol.QueryRows) {
	if qr == nil {
		return
	}
	for _, table := range qr.Tables {
		tc := c.cache.GetTable(table.TableName)
		if tc == nil || table.Rows == nil {
			continue
		}
		for _, rowData := range table.Rows.Rows() {
			reader := bsatn.NewReader(rowData)
			row, err := c.decodeCacheRow(table.TableName, reader)
			if err != nil {
				return
			}
			tc.ApplyDelete(rowData, row)
		}
	}
}

// applyQuerySetUpdate applies the per-table row changes of one QuerySetUpdate to
// the cache.
func (c *dbConnection) applyQuerySetUpdate(qs *protocol.QuerySetUpdate) {
	for ti := range qs.Tables {
		tu := &qs.Tables[ti]
		tc := c.cache.GetTable(tu.TableName)
		if tc == nil {
			continue
		}
		for _, rows := range tu.Rows {
			switch r := rows.(type) {
			case *protocol.PersistentTableRows:
				if sub := c.subscriptions[qs.QuerySetID]; sub != nil {
					c.trackSubscriptionRows(sub, tu.TableName, r.Inserts, r.Deletes)
				}
				c.applyPersistentRows(tu.TableName, tc, r)
			case *protocol.EventTableRows:
				c.applyEventRows(tu.TableName, tc, r)
			}
		}
	}
}

func (c *dbConnection) trackSubscriptionRows(sub *activeSubscription, table string, inserts, deletes *protocol.BsatnRowList) {
	if c.cache.GetTable(table) == nil {
		return
	}
	if sub.rows == nil {
		sub.rows = make(map[string]map[string][]byte)
	}
	rows := sub.rows[table]
	if rows == nil {
		rows = make(map[string][]byte)
		sub.rows[table] = rows
	}
	for _, data := range deletes.Rows() {
		delete(rows, string(data))
	}
	for _, data := range inserts.Rows() {
		key := string(data)
		rows[key] = []byte(key)
	}
}

// applyPersistentRows applies inserts/deletes for a persistent table, using
// primary-key based update detection when the table has a primary key.
func (c *dbConnection) applyPersistentRows(tableName string, tc cache.TableCache, rows *protocol.PersistentTableRows) {
	pkDef, hasPK := c.cache.GetTableDef(tableName)
	if hasPK && rows.Deletes != nil && rows.Inserts != nil &&
		rows.Deletes.Len() > 0 && rows.Inserts.Len() > 0 {
		c.applyPKUpdate(tc, pkDef, rows.Inserts, rows.Deletes)
		return
	}
	if rows.Deletes != nil {
		for _, rowData := range rows.Deletes.Rows() {
			reader := bsatn.NewReader(rowData)
			row, err := c.decodeCacheRow(tableName, reader)
			if err != nil {
				return
			}
			tc.ApplyDelete(rowData, row)
		}
	}
	if rows.Inserts != nil {
		for _, rowData := range rows.Inserts.Rows() {
			reader := bsatn.NewReader(rowData)
			row, err := c.decodeCacheRow(tableName, reader)
			if err != nil {
				return
			}
			tc.ApplyInsert(rowData, row)
		}
	}
}

// applyEventRows delivers ephemeral event-table rows via OnEvent without
// retaining them in the cache.
func (c *dbConnection) applyEventRows(tableName string, tc cache.TableCache, rows *protocol.EventTableRows) {
	if rows.Events == nil {
		return
	}
	for _, rowData := range rows.Events.Rows() {
		reader := bsatn.NewReader(rowData)
		row, err := c.decodeCacheRow(tableName, reader)
		if err != nil {
			return
		}
		tc.ApplyEvent(rowData, row)
	}
}

// applyPKUpdate handles PK-based update detection for tables with primary keys,
// matching delete+insert pairs that share a primary key into a single update.
func (c *dbConnection) applyPKUpdate(tc cache.TableCache, pkDef cache.TableDefWithPK, inserts, deletes *protocol.BsatnRowList) {
	type deleteEntry struct {
		rowBytes []byte
		row      any
	}
	deletedByPK := map[any]deleteEntry{}
	for _, rowData := range deletes.Rows() {
		reader := bsatn.NewReader(rowData)
		row, err := c.decodeCacheRow(pkDef.TableName(), reader)
		if err != nil {
			return
		}
		deletedByPK[pkKey(pkDef.PrimaryKey(row))] = deleteEntry{rowData, row}
	}
	for _, rowData := range inserts.Rows() {
		reader := bsatn.NewReader(rowData)
		row, err := c.decodeCacheRow(pkDef.TableName(), reader)
		if err != nil {
			return
		}
		pk := pkKey(pkDef.PrimaryKey(row))
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

// resubscribeAll re-sends a Subscribe for every tracked subscription, reusing
// each subscription's existing query_set_id. It is intended to be invoked after
// a connection is (re)established so the server's subscription set is restored.
func (c *dbConnection) resubscribeAll(ctx context.Context) error {
	for qid, sub := range c.subscriptions {
		sub.replaying = true
		reqID := c.nextRequestID.Add(1)
		sub.applied = false
		msg := &protocol.Subscribe{
			RequestID:    reqID,
			QuerySetID:   qid,
			QueryStrings: sub.queries,
		}
		if err := c.sendMessage(ctx, msg); err != nil {
			return err
		}
	}
	return nil
}

// subscriptionBuilder implements SubscriptionBuilder.
type subscriptionBuilder struct {
	conn      *dbConnection
	queries   []string
	onApplied func()
	onError   func(error)
}

func (sb *subscriptionBuilder) OnApplied(fn func()) SubscriptionBuilder {
	sb.onApplied = fn
	return sb
}

func (sb *subscriptionBuilder) OnError(fn func(error)) SubscriptionBuilder {
	sb.onError = fn
	return sb
}

func (sb *subscriptionBuilder) Build() (SubscriptionHandle, error) {
	if len(sb.queries) == 0 {
		return nil, &SubscriptionError{Message: "at least one query is required"}
	}
	qsID := sb.conn.nextQuerySetID.Add(1)
	handle := &subscriptionHandle{conn: sb.conn, querySetID: qsID}
	handle.active.Store(true)

	if err := sb.conn.enqueue(&command{
		handle:     handle,
		typ:        cmdSubscribe,
		queries:    append([]string(nil), sb.queries...),
		querySetID: qsID,
		onApplied:  sb.onApplied,
		onError:    sb.onError,
	}); err != nil {
		return nil, err
	}
	return handle, nil
}

// subscriptionHandle implements SubscriptionHandle.
type subscriptionHandle struct {
	conn       *dbConnection
	querySetID uint32
	active     atomic.Bool
}

func (sh *subscriptionHandle) Unsubscribe() error {
	if !sh.active.CompareAndSwap(true, false) {
		return nil
	}
	if err := sh.conn.enqueue(&command{
		typ:        cmdUnsubscribe,
		querySetID: sh.querySetID,
	}); err != nil {
		if !sh.conn.closed.Load() {
			sh.active.Store(true)
		}
		return err
	}
	return nil
}

func (sh *subscriptionHandle) IsActive() bool {
	return sh.active.Load() && !sh.conn.closed.Load()
}

func (sh *subscriptionHandle) QueryID() uint32 {
	return sh.querySetID
}

// Ensure interfaces are satisfied at compile time.
var (
	_ DbConnectionBuilder = (*dbConnectionBuilder)(nil)
	_ DbConnection        = (*dbConnection)(nil)
	_ SubscriptionBuilder = (*subscriptionBuilder)(nil)
	_ SubscriptionHandle  = (*subscriptionHandle)(nil)
	_ error               = (*ConnectionError)(nil)
	_ error               = (*ReducerError)(nil)
	_ error               = (*ProcedureError)(nil)
	_ error               = (*SubscriptionError)(nil)
	_ error               = (*ProtocolError)(nil)
)
