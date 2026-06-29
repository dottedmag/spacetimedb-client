package client

import (
	"context"
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
	// OnReducerError registers a handler invoked when a reducer call (which is
	// fire-and-forget) fails on the server — a structured error return or a host
	// internal error such as "no such reducer". This is distinct from
	// OnConnectError: a reducer failure does not affect the connection, so the
	// handler should not treat it as fatal. The error is a [*ReducerError].
	OnReducerError(fn func(err error)) DbConnectionBuilder
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
	onReducerError func(error)
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

func (b *dbConnectionBuilder) OnReducerError(fn func(error)) DbConnectionBuilder {
	b.onReducerError = fn
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
		onReducerError: b.onReducerError,
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
	onReducerError func(error)

	// State maps only accessed from the Run() goroutine event loop, so they
	// need no synchronization: there is exactly one writer goroutine.
	subscriptions      map[uint32]*activeSubscription // keyed by query_set_id
	oneOffCallbacks    map[uint32]chan any            // keyed by request_id
	procedureCallbacks map[uint32]chan any            // keyed by request_id, for routing procedure results
	reducerNames       map[uint32]string              // request_id -> reducer name, for reducer-result errors
}

func (c *dbConnection) Identity() types.Identity         { return c.identity }
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
	var encoded []byte
	if args != nil {
		w := bsatn.NewWriter(64)
		args.WriteBsatn(w)
		encoded = w.Bytes()
	}

	result := make(chan any, 1)
	cmd := &command{
		typ:     cmdCallProcedure,
		reducer: procedure,
		args:    encoded,
		result:  result,
	}

	select {
	case c.commands <- cmd:
	case <-ctx.Done():
		return nil, ctx.Err()
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
	}
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

	c.subscriptions = make(map[uint32]*activeSubscription)
	c.oneOffCallbacks = make(map[uint32]chan any)
	c.procedureCallbacks = make(map[uint32]chan any)
	c.reducerNames = make(map[uint32]string)

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
		reqID := c.nextRequestID.Add(1)
		if cmd.result != nil {
			c.procedureCallbacks[reqID] = cmd.result
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
			Flags:      protocol.UnsubscribeDefault,
		}
		return c.sendMessage(ctx, msg)

	case cmdOneOffQuery:
		reqID := c.nextRequestID.Add(1)
		if cmd.result != nil {
			c.oneOffCallbacks[reqID] = cmd.result
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

	c.dispatchServerMessage(msg)
}

// dispatchServerMessage applies a decoded server message to connection state,
// the row cache, and any registered subscription callbacks. It is only called
// from the Run() goroutine event loop.
func (c *dbConnection) dispatchServerMessage(msg protocol.ServerMessage) {
	switch m := msg.(type) {
	case *protocol.InitialConnection:
		c.identity = m.Identity
		c.connectionID = m.ConnectionID
		c.token = m.Token
		if c.onConnect != nil {
			c.onConnect(c, m.Identity, m.Token)
		}

	case *protocol.SubscribeApplied:
		// Initial matching rows for a newly-applied query set: apply them as
		// inserts and fire onApplied for the subscription keyed by query_set_id.
		c.applyQueryRows(&m.Rows)
		if sub, ok := c.subscriptions[m.QuerySetID]; ok {
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
		delete(c.subscriptions, m.QuerySetID)

	case *protocol.TransactionUpdate:
		// Broadcast row changes for the client's subscribed query sets.
		for i := range m.QuerySets {
			c.applyQuerySetUpdate(&m.QuerySets[i])
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
			}
		case *protocol.ReducerErr:
			if c.onReducerError != nil {
				c.onReducerError(&ReducerError{ReducerName: name, Message: "reducer returned an error"})
			}
		case *protocol.ReducerInternalError:
			if c.onReducerError != nil {
				c.onReducerError(&ReducerError{ReducerName: name, Message: outcome.Message})
			}
		}

	case *protocol.SubscriptionError:
		// Resolve the failed subscription by query_set_id, fire onError, drop it.
		if sub, ok := c.subscriptions[m.QuerySetID]; ok {
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
		}
	}
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
			row, err := c.cache.DecodeRow(table.TableName, reader)
			if err != nil {
				continue
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
			row, err := c.cache.DecodeRow(table.TableName, reader)
			if err != nil {
				continue
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
				c.applyPersistentRows(tu.TableName, tc, r)
			case *protocol.EventTableRows:
				c.applyEventRows(tu.TableName, tc, r)
			}
		}
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
			row, err := c.cache.DecodeRow(tableName, reader)
			if err != nil {
				continue
			}
			tc.ApplyDelete(rowData, row)
		}
	}
	if rows.Inserts != nil {
		for _, rowData := range rows.Inserts.Rows() {
			reader := bsatn.NewReader(rowData)
			row, err := c.cache.DecodeRow(tableName, reader)
			if err != nil {
				continue
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
		row, err := c.cache.DecodeRow(tableName, reader)
		if err != nil {
			continue
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
		row, err := pkDef.DecodeRow(reader)
		if err != nil {
			continue
		}
		pk := pkDef.PrimaryKey(row)
		deletedByPK[pk] = deleteEntry{rowData, row}
	}
	for _, rowData := range inserts.Rows() {
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

// resubscribeAll re-sends a Subscribe for every tracked subscription, reusing
// each subscription's existing query_set_id. It is intended to be invoked after
// a connection is (re)established so the server's subscription set is restored.
//
// NOTE: the client currently has no internal auto-reconnect loop — Run returns
// when the connection is lost and the caller must rebuild. This method is wired
// and ready for that future path; it is only meaningful once a reconnect
// mechanism calls it.
func (c *dbConnection) resubscribeAll(ctx context.Context) error {
	for qid, sub := range c.subscriptions {
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
	qsID := sb.conn.nextQuerySetID.Add(1)

	sb.conn.commands <- &command{
		typ:        cmdSubscribe,
		queries:    sb.queries,
		querySetID: qsID,
		onApplied:  sb.onApplied,
		onError:    sb.onError,
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
