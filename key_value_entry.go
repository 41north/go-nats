package natsutil

import (
	"sync/atomic"
	"time"

	"github.com/41north/go-async"
	"github.com/nats-io/nats.go"
)

// KeyValueEntry provides a generic interface for nats.KeyValueEntry.
type KeyValueEntry[T any] interface {
	nats.KeyValueEntry
	// UnmarshalValue decodes and returns the retrieve value.
	UnmarshalValue() (T, error)
}

// kve is a generic implementation of nats.KeyValueEntry.
type kve[T any] struct {
	// encoder defines how to encode T.
	encoder nats.Encoder
	// value represents the decoded return value.
	value atomic.Pointer[async.Result[T]]
	// delegate is the underlying nats.KeyValueEntry returned from the nats library.
	delegate nats.KeyValueEntry
}

func (e *kve[T]) Bucket() string             { return e.delegate.Bucket() }
func (e *kve[T]) Key() string                { return e.delegate.Key() }
func (e *kve[T]) Value() []byte              { return e.delegate.Value() }
func (e *kve[T]) Revision() uint64           { return e.delegate.Revision() }
func (e *kve[T]) Created() time.Time         { return e.delegate.Created() }
func (e *kve[T]) Delta() uint64              { return e.delegate.Delta() }
func (e *kve[T]) Operation() nats.KeyValueOp { return e.delegate.Operation() }

func (e *kve[T]) UnmarshalValue() (T, error) {
	// check if we have already unmarshalled the value
	v := e.value.Load()
	if v != nil {
		// value has already been unmarshalled
		return (*v).Unwrap()
	}

	var value T
	err := e.encoder.Decode("", e.delegate.Value(), &value)
	result := async.NewResult[T](value, err)

	// cache the result and return
	e.value.CompareAndSwap(nil, &result)

	return result.Unwrap()
}
