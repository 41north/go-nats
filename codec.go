package natsutil

import "encoding/json"

// Codec defines a means of encoding to/from bytes.
type Codec[T any] interface {
	// Marshal returns v represented as bytes.
	Marshal(v T) ([]byte, error)
	// Unmarshal decodes data and stores the result in the value pointed to by v.
	Unmarshal(data []byte, value *T) error
}

// JsonCodec defines a means of encoding to/from JSON using the default json package.
type JsonCodec[T any] struct{}

func (c JsonCodec[T]) Marshal(v T) ([]byte, error) {
	return json.Marshal(v)
}

func (c JsonCodec[T]) Unmarshal(data []byte, value *T) error {
	return json.Unmarshal(data, value)
}
