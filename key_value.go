package natsutil

import "github.com/nats-io/nats.go"

// KeyValue provides a generic interface for nats.KeyValue.
type KeyValue[T any] interface {
	// Delegate returns the underlying nats.KeyValue instance.
	Delegate() nats.KeyValue
	// Encoder returns the codec used for marshalling to and from bytes.
	Encoder() nats.Encoder
	// Get returns the latest value for the key.
	Get(key string) (entry KeyValueEntry[T], err error)
	// GetRevision returns a specific revision value for the key.
	GetRevision(key string, revision uint64) (entry KeyValueEntry[T], err error)
	// Put will place the new value for the key into the store.
	Put(key string, value T) (revision uint64, err error)
	// Create will add the key/value pair iff it does not exist.
	Create(key string, value T) (revision uint64, err error)
	// Update will update the value iff the latest revision matches.
	Update(key string, value T, last uint64) (revision uint64, err error)
	// Watch for any updates to keys that match the keys argument which could include wildcards.
	// Watch will send a nil entry when it has received all initial values.
	Watch(keys string, opts ...nats.WatchOpt) (KeyWatcher[T], error)
	// WatchAll will invoke the callback for all updates.
	WatchAll(opts ...nats.WatchOpt) (KeyWatcher[T], error)
	// History will return all historical values for the key.
	History(key string, opts ...nats.WatchOpt) ([]KeyValueEntry[T], error)
	// Bucket returns the current bucket name.
	Bucket() string
}

type kv[T any] struct {
	encoder  nats.Encoder
	delegate nats.KeyValue
}

func (k *kv[T]) Delegate() nats.KeyValue {
	return k.delegate
}

func (k *kv[T]) Encoder() nats.Encoder {
	return k.encoder
}

func (k *kv[T]) Get(key string) (entry KeyValueEntry[T], err error) {
	delegate, err := k.delegate.Get(key)
	if err != nil {
		return nil, err
	}
	return &kve[T]{delegate: delegate, encoder: k.encoder}, nil
}

func (k *kv[T]) GetRevision(key string, revision uint64) (entry KeyValueEntry[T], err error) {
	delegate, err := k.delegate.GetRevision(key, revision)
	if err != nil {
		return nil, err
	}
	return &kve[T]{delegate: delegate, encoder: k.encoder}, nil
}

func (k *kv[T]) Put(key string, value T) (revision uint64, err error) {
	bytes, err := k.encoder.Encode("", value)
	if err != nil {
		return 0, err
	}
	return k.delegate.Put(key, bytes)
}

func (k *kv[T]) Create(key string, value T) (revision uint64, err error) {
	bytes, err := k.encoder.Encode("", value)
	if err != nil {
		return 0, err
	}
	return k.delegate.Create(key, bytes)
}

func (k *kv[T]) Update(key string, value T, last uint64) (revision uint64, err error) {
	bytes, err := k.encoder.Encode("", value)
	if err != nil {
		return 0, err
	}
	return k.delegate.Update(key, bytes, last)
}

func (k *kv[T]) Watch(keys string, opts ...nats.WatchOpt) (KeyWatcher[T], error) {
	kw, err := k.delegate.Watch(keys, opts...)
	if err != nil {
		return nil, err
	}
	return NewKeyWatcher[T](kw, k.encoder), nil
}

func (k *kv[T]) WatchAll(opts ...nats.WatchOpt) (KeyWatcher[T], error) {
	kw, err := k.delegate.WatchAll(opts...)
	if err != nil {
		return nil, err
	}
	return NewKeyWatcher[T](kw, k.encoder), nil
}

func (k *kv[T]) History(key string, opts ...nats.WatchOpt) ([]KeyValueEntry[T], error) {
	entries, err := k.delegate.History(key, opts...)
	if err != nil {
		return nil, err
	}

	// convert into typed entries
	typedEntries := make([]KeyValueEntry[T], len(entries))
	for idx, delegate := range entries {
		typedEntries[idx] = &kve[T]{delegate: delegate, encoder: k.encoder}
	}

	return typedEntries, nil
}

func (k *kv[T]) Bucket() string {
	return k.delegate.Bucket()
}

func NewKeyValue[T any](delegate nats.KeyValue, encoder nats.Encoder) KeyValue[T] {
	return &kv[T]{delegate: delegate, encoder: encoder}
}
