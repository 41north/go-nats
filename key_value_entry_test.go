package natsutil

import (
	"testing"

	"github.com/nats-io/nats.go/encoders/builtin"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
)

func TestKve(t *testing.T) {
	s := runBasicJetStreamServer(t)
	defer shutdownJSServerAndRemoveStorage(t, s)

	_, js := jsClient(t, s)
	bucket := createTestBucket(t, js)
	encoder := builtin.JsonEncoder{}

	kv := NewKeyValue[testPayload](bucket, &encoder)

	revision, err := kv.Put("foo", testPayload{123})
	assert.Nil(t, err)
	assert.Equal(t, uint64(1), revision)

	kve, err := kv.Get("foo")
	assert.Nil(t, err)

	assert.Equal(t, bucket.Bucket(), kve.Bucket())
	assert.Equal(t, "foo", kve.Key())
	assert.Equal(t, nats.KeyValuePut, kve.Operation())
	assert.Equal(t, revision, kve.Revision())
	assert.Equal(t, uint64(0), kve.Delta())
	assert.NotNil(t, kve.Created())

	bytes := kve.Value()
	assert.True(t, len(bytes) > 0)

	for i := 0; i < 3; i++ {
		// we do this a few times to trigger the caching related code for the value
		value, err := kve.UnmarshalValue()
		assert.Nil(t, err)
		assert.Equal(t, testPayload{123}, value)
	}
}
