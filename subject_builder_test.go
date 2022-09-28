package natsutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSubjectBuilder_Add(t *testing.T) {
	sb := SubjectBuilder{}

	assert.Equal(t, "", sb.String())

	assert.Nil(t, sb.Add("foo"))
	assert.Equal(t, "foo", sb.String())

	assert.Nil(t, sb.Add("bar", "baz"))
	assert.Equal(t, "foo.bar.baz", sb.String())

	assert.Error(t, ErrSubjectInvalidCharacters, sb.Add("%"))
	assert.Error(t, ErrSubjectInvalidCharacters, sb.Add("-"))
	assert.Error(t, ErrSubjectInvalidCharacters, sb.Add("*"))
	assert.Error(t, ErrSubjectInvalidCharacters, sb.Add(">"))

	sb.Star()
	assert.Equal(t, "foo.bar.baz.*", sb.String())

	assert.Nil(t, sb.Add("hello"))
	assert.Equal(t, "foo.bar.baz.*.hello", sb.String())

	sb.Chevron()
	assert.Equal(t, "foo.bar.baz.*.hello.>", sb.String())
}
