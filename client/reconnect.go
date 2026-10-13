package client

import (
	"context"
	"time"

	"go.digitalxero.dev/spacetimedb-client/client/protocol"
)

// reconnect runs on the event loop, after the old reader has stopped. Only
// subscriptions are replayed: retrying a mutation could execute it twice.
func (c *dbConnection) reconnect(ctx context.Context) error {
	delay := 100 * time.Millisecond
	for {
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		conn, err := c.dial(ctx, c.Token())
		if err == nil {
			c.ws = conn
			c.reconnecting = true
			return nil
		}
		if c.onConnectError != nil {
			c.onConnectError(err)
		}
		delay = min(delay*2, 10*time.Second)
	}
}

func (c *dbConnection) failPending(err error) {
	for _, ch := range c.oneOffCallbacks {
		ch <- oneOffResult{err: err}
	}
	for _, ch := range c.procedureCallbacks {
		ch <- procedureResult{err: err}
	}
	clear(c.oneOffCallbacks)
	clear(c.procedureCallbacks)
	clear(c.procedureContexts)
	clear(c.queryContexts)
	for id, name := range c.reducerNames {
		if c.onReducerError != nil {
			c.onReducerError(&ReducerError{ReducerName: name, Message: "connection lost; execution outcome unknown"})
		}
		delete(c.reducerNames, id)
	}
}

// Reconcile each subscription's old row contribution with its fresh snapshot.
// Unchanged rows retain references without generating spurious callbacks.
func (c *dbConnection) reconcileSubscription(sub *activeSubscription, snapshot *protocol.QueryRows) {
	fresh := &activeSubscription{}
	for _, table := range snapshot.Tables {
		c.trackSubscriptionRows(fresh, table.TableName, table.Rows, nil)
	}
	tables := make(map[string]bool)
	for name := range sub.rows {
		tables[name] = true
	}
	for name := range fresh.rows {
		tables[name] = true
	}
	for name := range tables {
		tc := c.cache.GetTable(name)
		if tc == nil {
			continue
		}
		var inserts, deletes [][]byte
		for key, data := range sub.rows[name] {
			if _, ok := fresh.rows[name][key]; !ok {
				deletes = append(deletes, data)
			}
		}
		for key, data := range fresh.rows[name] {
			if _, ok := sub.rows[name][key]; !ok {
				inserts = append(inserts, data)
			}
		}
		c.applyPersistentRows(name, tc, &protocol.PersistentTableRows{
			Inserts: snapshotRowList(inserts), Deletes: snapshotRowList(deletes),
		})
		if c.rowDecodeErr != nil {
			return
		}
	}
	sub.rows = fresh.rows
}

func snapshotRowList(rows [][]byte) *protocol.BsatnRowList {
	list := &protocol.BsatnRowList{}
	var offsets []uint64
	for _, row := range rows {
		offsets = append(offsets, uint64(len(list.RowsData)))
		list.RowsData = append(list.RowsData, row...)
	}
	list.SizeHint = protocol.RowOffsetsHint{Offsets: offsets}
	return list
}
