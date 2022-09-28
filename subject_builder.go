package natsutil

import (
	"regexp"
	"strings"

	"github.com/juju/errors"
)

const (
	subjectSeparator = "."

	ErrSubjectInvalidCharacters = errors.ConstError("subject component must only contain [a-zA-Z0-9_]")
)

// no fancy characters allowed, just simple alphanumeric
var componentRegex = regexp.MustCompile("\\w+")

// SubjectBuilder helps with constructing valid nats subject names.
type SubjectBuilder struct {
	sb strings.Builder
}

// prependSeparator prepends a '.' if necessary.
func (b *SubjectBuilder) prependSeparator() {
	if b.sb.Len() > 0 {
		// prepend a separator
		b.sb.WriteString(subjectSeparator)
	}
}

// Add takes the provided component and appends it to the subject under construction, prepending a separator as needed.
func (b *SubjectBuilder) Add(components ...string) error {
	for _, component := range components {
		if !componentRegex.MatchString(component) {
			return ErrSubjectInvalidCharacters
		}
		b.prependSeparator()
		b.sb.WriteString(component)
	}
	return nil
}

// Star appends a '*' wildcard to the subject under construction, prepending a separator as needed.
func (b *SubjectBuilder) Star() {
	b.prependSeparator()
	b.sb.WriteString("*")
}

// Chevron appends a '>' wildcard to the subject under construction, prepending a separator as needed.
func (b *SubjectBuilder) Chevron() {
	b.prependSeparator()
	b.sb.WriteString(">")
}

// String outputs the subject that has been constructed so far.
func (b *SubjectBuilder) String() string {
	return b.sb.String()
}
