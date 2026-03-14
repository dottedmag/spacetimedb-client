package client

// SubscriptionBuilder configures and creates a subscription to one or more
// SQL queries. Create a SubscriptionBuilder by calling [DbConnection.Subscribe]
// with the desired queries.
//
// Use OnApplied to register a callback that fires once the initial matching
// rows have been delivered and applied to the local cache. Call Build to
// send the subscription to the server and obtain a [SubscriptionHandle].
type SubscriptionBuilder interface {
	OnApplied(fn func()) SubscriptionBuilder
	Build() (SubscriptionHandle, error)
}

// SubscriptionHandle represents an active subscription returned by
// [SubscriptionBuilder.Build]. Use Unsubscribe to cancel the subscription
// and IsActive to check whether it is still active.
type SubscriptionHandle interface {
	Unsubscribe() error
	IsActive() bool
}

// CallbackID is an opaque identifier returned when registering a callback.
// Pass it to RemoveCallback on a [cache.TableCache] to unregister.
type CallbackID uint64
