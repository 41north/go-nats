package natsutil

import (
	"regexp"

	"github.com/41north/go-async"
	"github.com/juju/errors"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/encoders/builtin"
	"github.com/sirupsen/logrus"
)

var methodRegex = regexp.MustCompile("\\.+(\\w+)$")

type JrpcFuture = async.Future[async.Result[any]]

type JrpcHandler = func(method string, params any) JrpcFuture

type JrpcServer struct {
	conn    *nats.Conn
	encoder nats.Encoder
	handler JrpcHandler
	subs    []*nats.Subscription
}

func NewJrpcServer(conn *nats.Conn, subjects []string, handler JrpcHandler) (*JrpcServer, error) {
	server := &JrpcServer{
		conn:    conn,
		encoder: &builtin.JsonEncoder{},
		handler: handler,
	}

	for _, subject := range subjects {
		sub, err := conn.Subscribe(subject, server.handleMsg)
		if err != nil {
			return nil, errors.Annotatef(err, "failed to subscribe to subject: %s", subject)
		}
		server.subs = append(server.subs, sub)
	}

	return server, nil
}

func (s *JrpcServer) handleMsg(msg *nats.Msg) {
	log := logrus.WithFields(logrus.Fields{
		"subject": msg.Subject,
		"reply":   msg.Reply,
	})

	groups := methodRegex.FindStringSubmatch(msg.Subject)
	if len(groups) != 1 {
		respondWithError(msg, errors.New("invalid subject"), log)
		return
	}

	method := groups[0]
	var params any
	if err := s.encoder.Decode(msg.Subject, msg.Data, &params); err != nil {
		respondWithError(msg, errors.Annotate(err, "failed to unmarshal params"), log)
	}

	// the handler implementation can choose to operate synchronously or asynchronously
	future := s.handler(method, params)

	// handle the result asynchronously
	// TODO think about limiting the number of inflight requests, maybe this can be done within NATS?
	go func() {
		result, ok := <-future.Get()
		if !ok {
			// channel was closed before a result was set, we assume a timeout for now
			// TODO make timeout error a constant
			respondWithError(msg, errors.New("timeout"), log)
			return
		}

		value, err := result.Unwrap()
		if err != nil {
			// TODO how to distinguish different types of standard json rpc errors?
			respondWithError(msg, err, log)
			return
		}

		bytes, err := s.encoder.Encode(msg.Subject, value)
		if err != nil {
			log.WithError(err).
				Error("failed to encode response")
			// TODO replace with standard json rpc error response
			respondWithError(msg, errors.New("internal server error"), log)
		}

		if err := msg.Respond(bytes); err != nil {
			log.WithError(err).Error("failed to respond to msg")
		}
	}()
}

func respondWithError(msg *nats.Msg, err error, log *logrus.Entry) {
	resp := nats.NewMsg(msg.Reply)
	resp.Header.Set("error", err.Error())
	if err := msg.Respond([]byte{}); err != nil {
		log.WithError(err).Error("failed to respond to msg with error")
	}
}
