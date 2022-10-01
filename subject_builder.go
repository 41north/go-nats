package natsutil

import (
	"regexp"
	"strings"

	"github.com/juju/errors"
)

const (
	subjectSeparator = "."
	star             = "*"
	chevron          = ">"
)

const (
	ErrSubjectInvalidCharacters = errors.ConstError("subject component must only contain [a-zA-Z0-9_]")
	ErrPopInsufficientElements  = errors.ConstError("cannot pop more elements than have already been pushed")
)

// no fancy characters allowed, just simple alphanumeric
var elementRegex = regexp.MustCompile("\\w+")

// SubjectBuilder helps with constructing valid nats subject names.
type SubjectBuilder struct {
	elements []string
}

// Push takes the provided component and appends them to the subject under construction
func (b *SubjectBuilder) Push(elements ...string) error {
	for _, elem := range elements {
		if !elementRegex.MatchString(elem) {
			return ErrSubjectInvalidCharacters
		}
		b.elements = append(b.elements, elem)
	}
	return nil
}

func (b *SubjectBuilder) Pop(count int) error {
	if count > len(b.elements) {
		return ErrPopInsufficientElements
	}
	b.elements = b.elements[:(len(b.elements) - count)]
	return nil
}

// Star appends a '*' wildcard to the subject under construction, prepending a separator as needed.
func (b *SubjectBuilder) Star() {
	b.elements = append(b.elements, star)
}

// Chevron appends a '>' wildcard to the subject under construction, prepending a separator as needed.
func (b *SubjectBuilder) Chevron() {
	b.elements = append(b.elements, chevron)
}

// String outputs the subject that has been constructed so far.
func (b *SubjectBuilder) String() string {
	sb := strings.Builder{}
	for idx, elem := range b.elements {
		if idx > 0 {
			// prepend a separator
			sb.WriteString(subjectSeparator)
		}
		sb.WriteString(elem)
	}
	return sb.String()
}
