package client_test

import (
	"context"
	"fmt"

	"go.digitalxero.dev/spacetimedb-client/bsatn"
	"go.digitalxero.dev/spacetimedb-client/client"
	"go.digitalxero.dev/spacetimedb-client/client/cache"
	"go.digitalxero.dev/spacetimedb-client/types"
)

func ExampleNewDbConnection() {
	ctx := context.Background()
	conn, err := client.NewDbConnection().
		WithUri("ws://localhost:3000").
		WithDatabaseName("my_database").
		WithToken("").
		OnConnect(func(conn client.DbConnection, identity types.Identity, token string) {
			fmt.Println("connected as", identity)
		}).
		OnDisconnect(func(conn client.DbConnection, err error) {
			fmt.Println("disconnected:", err)
		}).
		Build(ctx)
	if err != nil {
		fmt.Println("connection failed:", err)
		return
	}
	_ = conn
}

// placeholderTableDef is a minimal TableDef for demonstration purposes.
type placeholderTableDef struct{}

func (d *placeholderTableDef) TableName() string { return "messages" }
func (d *placeholderTableDef) DecodeRow(r bsatn.Reader) (any, error) {
	text, err := r.GetString()
	if err != nil {
		return nil, err
	}
	return &struct{ Text string }{Text: text}, nil
}
func (d *placeholderTableDef) EncodeRow(row any) []byte {
	w := bsatn.NewWriter(64)
	w.PutString(row.(*struct{ Text string }).Text)
	return w.Bytes()
}

func Example_lifecycle() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Build the connection.
	conn, err := client.NewDbConnection().
		WithUri("ws://localhost:3000").
		WithDatabaseName("chat").
		WithToken("").
		OnConnect(func(conn client.DbConnection, identity types.Identity, token string) {
			fmt.Println("connected as", identity)

			// 3. Subscribe to queries after connecting.
			_, subErr := conn.Subscribe("SELECT * FROM messages").
				OnApplied(func() {
					fmt.Println("initial rows received")
				}).
				Build()
			if subErr != nil {
				fmt.Println("subscribe failed:", subErr)
			}

			// 4. Call a reducer.
			args := &sendMessageArgs{Text: "hello from Go"}
			if callErr := conn.CallReducer("send_message", args); callErr != nil {
				fmt.Println("call reducer failed:", callErr)
			}
		}).
		OnDisconnect(func(conn client.DbConnection, err error) {
			fmt.Println("disconnected:", err)
		}).
		Build(ctx)
	if err != nil {
		fmt.Println("connection failed:", err)
		return
	}

	// 2. Register tables before calling Run.
	conn.RegisterTable(&placeholderTableDef{})

	// Register a callback on the table cache.
	tc := conn.Cache().GetTable("messages")
	if tc != nil {
		tc.OnInsert(func(row any) {
			fmt.Println("new message inserted")
		})
	}

	// Run the event loop in a goroutine.
	go func() {
		if runErr := conn.Run(ctx); runErr != nil {
			fmt.Println("run error:", runErr)
		}
	}()

	// 5. Disconnect when done.
	_ = conn.Disconnect()
}

// sendMessageArgs implements bsatn.Serializable for the "send_message" reducer.
type sendMessageArgs struct {
	Text string
}

func (a *sendMessageArgs) WriteBsatn(w bsatn.Writer) {
	w.PutString(a.Text)
}

func Example_reducerArgs() {
	// This shows how to create reducer arguments.
	// In practice, pass to conn.CallReducer("send_message", args).
	args := &sendMessageArgs{Text: "hello world"}
	data := bsatn.Encode(args)
	fmt.Println("encoded", len(data), "bytes")
}

// Ensure imports are used.
var _ cache.TableDef = (*placeholderTableDef)(nil)
