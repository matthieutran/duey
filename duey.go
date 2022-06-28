package duey

import (
	"os"
	"time"

	"github.com/nats-io/nats.go"
)

type EventStreamer struct {
	nc *nats.Conn
	ec *nats.EncodedConn
}

func Init() (*EventStreamer, error) {
	nc, err := nats.Connect(os.Getenv("NATS_URI"), nats.PingInterval(10*time.Second), nats.MaxPingsOutstanding(5))
	if err != nil {
		return nil, err
	}

	ec, err := nats.NewEncodedConn(nc, nats.JSON_ENCODER)

	return &EventStreamer{nc: nc, ec: ec}, err
}

// Publish publishes the model argument to the subject queue
func (s *EventStreamer) Publish(subject string, model interface{}) (err error) {
	return s.ec.Publish(subject, model)
}

// Request will request a reply from the subject queue and return the model
func (s *EventStreamer) Request(subject string, model interface{}, res interface{}, timeout time.Duration) (err error) {
	return s.ec.Request(subject, model, res, timeout)
}

type Handler interface{}

// Subscribe listens for models sent into the subject queue
func (s *EventStreamer) Subscribe(subject string, callback Handler) (err error) {
	_, err = s.ec.Subscribe(subject, callback)

	return
}

func (s *EventStreamer) Stop() {
	s.ec.Close()
	s.nc.Close()
}
