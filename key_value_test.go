package natsutil_test

import (
	"testing"

	"github.com/41north/natsutil.go"

	"github.com/nats-io/nats.go/encoders/builtin"

	"github.com/nats-io/nats.go"

	"github.com/stretchr/testify/assert"
)

var encoder = builtin.JsonEncoder{}

func TestNewKeyValue(t *testing.T) {
	s := runBasicJetStreamServer(t)
	defer shutdownJSServerAndRemoveStorage(t, s)

	_, js := jsClient(t, s)
	bucket := createTestBucket(t, js)

	kv := natsutil.NewKeyValue[string](bucket, &encoder)

	assert.Equal(t, bucket.Bucket(), kv.Bucket())
	assert.Equal(t, &encoder, kv.Encoder())
}

func TestKv_Put(t *testing.T) {
	s := runBasicJetStreamServer(t)
	defer shutdownJSServerAndRemoveStorage(t, s)

	_, js := jsClient(t, s)
	kv := natsutil.NewKeyValue[testPayload](createTestBucket(t, js), &encoder)

	// insert some values
	revision, err := kv.Put("foo", testPayload{1})
	assert.Nil(t, err)
	assert.Equal(t, uint64(1), revision)

	revision, err = kv.Put("bar", testPayload{2})
	assert.Nil(t, err)
	assert.Equal(t, uint64(2), revision)

	revision, err = kv.Put("baz", testPayload{3})
	assert.Nil(t, err)
	assert.Equal(t, uint64(3), revision)

	// update one of them
	revision, err = kv.Put("bar", testPayload{4})
	assert.Nil(t, err)
	assert.Equal(t, uint64(4), revision)

	// retrieve them and verify their contents
	kve, err := kv.Get("foo")
	assert.Nil(t, err)
	assert.Equal(t, uint64(1), kve.Revision())
	v, err := kve.UnmarshalValue()
	assert.Nil(t, err)
	assert.Equal(t, testPayload{1}, v)

	kve, err = kv.Get("bar")
	assert.Nil(t, err)
	assert.Equal(t, uint64(4), kve.Revision())
	v, err = kve.UnmarshalValue()
	assert.Nil(t, err)
	assert.Equal(t, testPayload{4}, v)

	kve, err = kv.Get("baz")
	assert.Nil(t, err)
	assert.Equal(t, uint64(3), kve.Revision())
	v, err = kve.UnmarshalValue()
	assert.Nil(t, err)
	assert.Equal(t, testPayload{3}, v)
}

func TestKv_Create(t *testing.T) {
	s := runBasicJetStreamServer(t)
	defer shutdownJSServerAndRemoveStorage(t, s)

	_, js := jsClient(t, s)
	kv := natsutil.NewKeyValue[testPayload](createTestBucket(t, js), &encoder)

	// create a value
	revision, err := kv.Create("foo", testPayload{1})
	assert.Nil(t, err)
	assert.Equal(t, uint64(1), revision)

	// attempt to create with the same key
	revision, err = kv.Create("foo", testPayload{2})
	assert.NotNil(t, err)
}

func TestKv_Update(t *testing.T) {
	s := runBasicJetStreamServer(t)
	defer shutdownJSServerAndRemoveStorage(t, s)

	_, js := jsClient(t, s)
	kv := natsutil.NewKeyValue[testPayload](createTestBucket(t, js), &encoder)

	// create a value
	revision, err := kv.Create("foo", testPayload{1})
	assert.Nil(t, err)
	assert.Equal(t, uint64(1), revision)

	// update that value
	revision, err = kv.Update("foo", testPayload{2}, uint64(1))
	assert.Nil(t, err)

	// attempt to update that value with the wrong revision
	revision, err = kv.Update("foo", testPayload{3}, uint64(1))
	assert.NotNil(t, err)

	// try to update a value which doesn't exist
	revision, err = kv.Update("bar", testPayload{4}, uint64(1))
	assert.NotNil(t, err)
}

func TestKv_GetRevisionAndHistory(t *testing.T) {
	s := runBasicJetStreamServer(t)
	defer shutdownJSServerAndRemoveStorage(t, s)

	_, js := jsClient(t, s)
	kv := natsutil.NewKeyValue[testPayload](createTestBucket(t, js), &encoder)

	// create some versions for a given key
	revision, err := kv.Put("foo", testPayload{1})
	assert.Nil(t, err)
	assert.Equal(t, uint64(1), revision)

	revision, err = kv.Put("foo", testPayload{2})
	assert.Nil(t, err)
	assert.Equal(t, uint64(2), revision)

	revision, err = kv.Put("foo", testPayload{3})
	assert.Nil(t, err)
	assert.Equal(t, uint64(3), revision)

	// fetch individual revisions
	kve, err := kv.GetRevision("foo", uint64(1))
	assert.Nil(t, err)
	v, err := kve.UnmarshalValue()
	assert.Nil(t, err)
	assert.Equal(t, testPayload{1}, v)

	kve, err = kv.GetRevision("foo", uint64(2))
	assert.Nil(t, err)
	v, err = kve.UnmarshalValue()
	assert.Nil(t, err)
	assert.Equal(t, testPayload{2}, v)

	kve, err = kv.GetRevision("foo", uint64(3))
	assert.Nil(t, err)
	v, err = kve.UnmarshalValue()
	assert.Nil(t, err)
	assert.Equal(t, testPayload{3}, v)

	// get history
	entries, err := kv.History("foo")
	assert.Nil(t, err)
	assert.Equal(t, 3, len(entries))

	// history will be in reverse order
	for i := len(entries) - 1; i >= 0; i-- {
		entry := entries[i]
		v, err := entry.UnmarshalValue()
		assert.Nil(t, err)
		assert.Equal(t, testPayload{i + 1}, v)
		assert.Equal(t, uint64(i+1), entry.Revision())
	}
}

func TestKv_Watch(t *testing.T) {
	s := runBasicJetStreamServer(t)
	defer shutdownJSServerAndRemoveStorage(t, s)

	_, js := jsClient(t, s)
	kv := natsutil.NewKeyValue[testPayload](createTestBucket(t, js), &encoder)

	// create a watch on a specific key
	w, err := kv.Watch("foo")
	assert.Nil(t, err)

	// get the update channel
	ch := w.UpdatesUnmarshalled()

	// perform some crud
	_, err = kv.Put("foo", testPayload{1})
	assert.Nil(t, err)

	_, err = kv.Put("foo", testPayload{2})
	assert.Nil(t, err)

	err = kv.Delegate().Delete("foo")
	assert.Nil(t, err)

	// process updates from channel

	entry, ok := <-ch
	assert.True(t, ok)
	// first update always seems to be nil
	assert.Nil(t, entry)

	entry, ok = <-ch
	assert.True(t, ok)
	assert.Equal(t, "foo", entry.Key())
	assert.Equal(t, nats.KeyValuePut, entry.Operation())
	assert.Equal(t, uint64(1), entry.Revision())
	v, err := entry.UnmarshalValue()
	assert.Nil(t, err)
	assert.Equal(t, testPayload{1}, v)

	entry, ok = <-ch
	assert.True(t, ok)
	assert.Equal(t, "foo", entry.Key())
	assert.Equal(t, nats.KeyValuePut, entry.Operation())
	assert.Equal(t, uint64(2), entry.Revision())
	v, err = entry.UnmarshalValue()
	assert.Nil(t, err)
	assert.Equal(t, testPayload{2}, v)

	entry, ok = <-ch
	assert.True(t, ok)
	assert.Equal(t, "foo", entry.Key())
	assert.Equal(t, nats.KeyValueDelete, entry.Operation())
	assert.Equal(t, uint64(3), entry.Revision())
	v, err = entry.UnmarshalValue()
	assert.NotNil(t, err)
	// default value as there is no payload with a delete
	assert.Equal(t, testPayload{0}, v)

	// stop the watcher and check that the update channel is closed
	assert.Nil(t, w.Stop())

	_, ok = <-ch
	assert.False(t, ok)
}

func TestKv_WatchAll(t *testing.T) {
	s := runBasicJetStreamServer(t)
	defer shutdownJSServerAndRemoveStorage(t, s)

	_, js := jsClient(t, s)
	kv := natsutil.NewKeyValue[testPayload](createTestBucket(t, js), &encoder)

	// create a watch on a specific key
	w, err := kv.WatchAll()
	assert.Nil(t, err)

	// we didn't construct the watcher with a context
	// TODO test with a configured context
	assert.Nil(t, w.Context())

	// get the update channel
	ch := w.UpdatesUnmarshalled()

	// perform some crud
	_, err = kv.Put("foo", testPayload{1})
	assert.Nil(t, err)

	_, err = kv.Put("bar", testPayload{2})
	assert.Nil(t, err)

	_, err = kv.Put("baz", testPayload{3})
	assert.Nil(t, err)

	err = kv.Delegate().Delete("foo")
	assert.Nil(t, err)

	// process updates from channel

	entry, ok := <-ch
	assert.True(t, ok)
	// first update always seems to be nil
	assert.Nil(t, entry)

	entry, ok = <-ch
	assert.True(t, ok)
	assert.Equal(t, "foo", entry.Key())
	assert.Equal(t, nats.KeyValuePut, entry.Operation())
	assert.Equal(t, uint64(1), entry.Revision())
	v, err := entry.UnmarshalValue()
	assert.Nil(t, err)
	assert.Equal(t, testPayload{1}, v)

	entry, ok = <-ch
	assert.True(t, ok)
	assert.Equal(t, "bar", entry.Key())
	assert.Equal(t, nats.KeyValuePut, entry.Operation())
	assert.Equal(t, uint64(2), entry.Revision())
	v, err = entry.UnmarshalValue()
	assert.Nil(t, err)
	assert.Equal(t, testPayload{2}, v)

	entry, ok = <-ch
	assert.True(t, ok)
	assert.Equal(t, "baz", entry.Key())
	assert.Equal(t, nats.KeyValuePut, entry.Operation())
	assert.Equal(t, uint64(3), entry.Revision())
	v, err = entry.UnmarshalValue()
	assert.Nil(t, err)
	assert.Equal(t, testPayload{3}, v)

	entry, ok = <-ch
	assert.True(t, ok)
	assert.Equal(t, "foo", entry.Key())
	assert.Equal(t, nats.KeyValueDelete, entry.Operation())
	assert.Equal(t, uint64(4), entry.Revision())
	v, err = entry.UnmarshalValue()
	assert.NotNil(t, err)
	// default value as there is no payload with a delete
	assert.Equal(t, testPayload{0}, v)

	// stop the watcher and check that the update channel is closed
	assert.Nil(t, w.Stop())

	_, ok = <-ch
	assert.False(t, ok)
}
