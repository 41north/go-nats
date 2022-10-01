# natsutil.go

![Build](https://github.com/41north/natsutil.go/actions/workflows/ci.yml/badge.svg)
[![Coverage Status](https://coveralls.io/repos/github/41north/natsutil.go/badge.svg?branch=main)](https://coveralls.io/github/41north/natsutil.go?branch=main)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Status: _EXPERIMENTAL_

A collection of utilities for working with [NATS.io](https://nats.io/).

## Documentation

[![GoDoc](https://img.shields.io/badge/godoc-reference-blue.svg)](http://godoc.org/github.com/41north/natsutil.go)

Full `go doc` style documentation for the project can be viewed online without
installing this package by using the excellent GoDoc site here:
http://godoc.org/github.com/41north/natsutil.go

You can also view the documentation locally once the package is installed with
the `godoc` tool by running `godoc -http=":6060"` and pointing your browser to
http://localhost:6060/pkg/github.com/41north/natsutil.go

## Installation

```bash
$ go get -u github.com/41north/natsutil.go
```

Add this import line to the file you're working in:

```Go
import natsutil "github.com/41north/natsutil.go"
```

## Quick Start

The following utilities will be of interest:

- [Subject Builder](#subject-builder)
- [Generic Key Value Store](#generic-key-value-store)

### Subject Builder

A builder much like `strings.Builder` which helps with constructing valid subject names and ensures invalid characters
are not used:

```go
sb := natsutil.SubjectBuilder{}

// 'foo.bar.baz.*'
sb.Push("foo", "bar", "baz")
sb.Star()
subject := sb.String()

// 'foo.bar'
sb.Pop(2)
subject = sb.String()

// 'foo.bar.hello.>'
sb.Push("hello")
sb.Chevron()
subject = sb.String()
```

### Generic Key Value Store

A generic interface for interacting with JetStream Key-Value stores can be created with the following:

```go
type testPayload struct {
	Value int `json:""`
}

// create a nats connection
nc, err := nats.Connect(s.ClientURL(), opts...)
...

// get a JetStream context
js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
...

// get a reference to the nats KV interface
kv, err := js.KeyValue("my-bucket")
...

// create a generic KeyValue interface that uses JSON encoding
encoder := builtin.JsonEncoder{}
kvT = natsutil.NewKeyValue[testPayload](kv, &codec)

// put a testPayload object
kvT.Put("foo", testPayload{1})

// get a testPayload object
get, err := kvT.Get("foo")

// create a generic Watcher
watcher, err := kvT.WatchAll()
...
```

## License

Go-async is licensed under the [Apache 2.0 License](LICENSE)

## Contact

If you want to get in touch drop us an email at [hello@41north.dev](mailto:hello@41north.dev)
