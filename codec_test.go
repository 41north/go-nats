package natsutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var stringJsonCodec = JsonCodec[string]{}

var testPayloadJsonCodec = JsonCodec[testPayload]{}

type testPayload struct {
	Value int `json:""`
}

func TestJsonCodec_Marshal(t *testing.T) {
	bytes, err := stringJsonCodec.Marshal("foo")
	assert.Nil(t, err)
	assert.Equal(t, "\"foo\"", string(bytes))
}

func TestJsonCodec_Unmarshal(t *testing.T) {
	json := "\"hello world\""
	var value string
	err := stringJsonCodec.Unmarshal([]byte(json), &value)
	assert.Nil(t, err)
	assert.Equal(t, "hello world", value)
}
