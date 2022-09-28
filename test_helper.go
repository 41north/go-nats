package natsutil

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

const (
	_EMPTY_ = ""
)

func runBasicJetStreamServer(t *testing.T) *server.Server {
	t.Helper()
	opts := test.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	return test.RunServer(&opts)
}

func client(t *testing.T, s *server.Server, opts ...nats.Option) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(s.ClientURL(), opts...)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	return nc
}

func jsClient(t *testing.T, s *server.Server, opts ...nats.Option) (*nats.Conn, nats.JetStreamContext) {
	t.Helper()
	nc := client(t, s, opts...)
	js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
	if err != nil {
		t.Fatalf("Unexpected error getting JetStream context: %v", err)
	}
	return nc, js
}

func shutdownJSServerAndRemoveStorage(t *testing.T, s *server.Server) {
	t.Helper()
	var sd string
	if config := s.JetStreamConfig(); config != nil {
		sd = config.StoreDir
	}
	s.Shutdown()
	if sd != _EMPTY_ {
		if err := os.RemoveAll(sd); err != nil {
			t.Fatalf("Unable to remove storage %q: %v", sd, err)
		}
	}
	s.WaitForShutdown()
}

func createTestBucket(t *testing.T, js nats.JetStreamContext) nats.KeyValue {
	t.Helper()
	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:  "TestBucket",
		History: 10,
	})
	assert.Nil(t, err, "failed to create test bucket")
	return kv
}
