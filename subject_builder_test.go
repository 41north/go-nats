package natsutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSubjectBuilder_PushPop(t *testing.T) {
	sb := SubjectBuilder{}

	assert.Equal(t, "", sb.String())

	assert.Nil(t, sb.Push("foo"))
	assert.Equal(t, "foo", sb.String())

	assert.Nil(t, sb.Push("bar", "baz"))
	assert.Equal(t, "foo.bar.baz", sb.String())

	assert.Nil(t, sb.Pop(2))
	assert.Equal(t, "foo", sb.String())

	assert.Error(t, ErrPopInsufficientElements, sb.Pop(2))

	sb.Star()
	assert.Equal(t, "foo.*", sb.String())

	assert.Nil(t, sb.Push("hello"))
	assert.Equal(t, "foo.*.hello", sb.String())

	sb.Chevron()
	assert.Equal(t, "foo.*.hello.>", sb.String())
}

func TestSubjectBuilder_InvalidCharacters(t *testing.T) {
	sb := SubjectBuilder{}

	assert.Nil(t, sb.Push("foo"))
	assert.Nil(t, sb.Push("BAR"))
	assert.Nil(t, sb.Push("hell0_wor1d"))

	assert.Equal(t, "foo.BAR.hell0_wor1d", sb.String())

	assert.Error(t, ErrSubjectInvalidCharacters, sb.Push("%"))
	assert.Error(t, ErrSubjectInvalidCharacters, sb.Push("-"))
	assert.Error(t, ErrSubjectInvalidCharacters, sb.Push("*"))
	assert.Error(t, ErrSubjectInvalidCharacters, sb.Push(">"))
	assert.Error(t, ErrSubjectInvalidCharacters, sb.Push("{"))
	assert.Error(t, ErrSubjectInvalidCharacters, sb.Push("}"))
	assert.Error(t, ErrSubjectInvalidCharacters, sb.Push("("))
	assert.Error(t, ErrSubjectInvalidCharacters, sb.Push(")"))
	assert.Error(t, ErrSubjectInvalidCharacters, sb.Push("+"))
}
