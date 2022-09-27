package natsutil

import (
	"context"

	"github.com/nats-io/nats.go"
)

// KeyWatcher provides a generic interface for nats.KeyWatcher.
type KeyWatcher[T any] interface {
	nats.KeyWatcher
	// UpdatesUnmarshalled provides a decoded view of the Updates() channel.
	UpdatesUnmarshalled() <-chan KeyValueEntry[T]
}

type kw[T any] struct {
	// codec defines how to decode update values into type T.
	codec Codec[T]
	// delegate is the underlying nats.KeyWatcher returned from the nats library.
	delegate nats.KeyWatcher
}

func (k *kw[T]) Context() context.Context {
	return k.delegate.Context()
}

func (k *kw[T]) Stop() error {
	return k.delegate.Stop()
}

func (k *kw[T]) Updates() <-chan nats.KeyValueEntry {
	return k.delegate.Updates()
}

func (k *kw[T]) UpdatesUnmarshalled() <-chan KeyValueEntry[T] {
	updates := k.Updates()

	// size of this channel matches the underlying channel size
	ch := make(chan KeyValueEntry[T], 256)

	// start a routine to read from the underlying channel and wrap nats.KeyValueEntry
	go func() {
		// close channel upon completion
		defer close(ch)
		for delegate := range updates {
			var entry KeyValueEntry[T]
			// TODO why do we seem to get an initial nil entry when a key doesn't exist yet?
			if delegate != nil {
				entry = &kve[T]{delegate: delegate, codec: k.codec}
			}
			ch <- entry
		}
	}()

	return ch
}

func NewKeyWatcher[T any](watcher nats.KeyWatcher, codec Codec[T]) KeyWatcher[T] {
	return &kw[T]{delegate: watcher, codec: codec}
}
