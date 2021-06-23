package gorabbitmq

import (
	"time"

	"github.com/Noobygames/amqp"
)

const delay = 5 // reconnect after delay seconds

// Connection amqp.Connection wrapper
type Connection struct {
	*amqp.Connection
}

// Dial wrap amqp.Dial, dial and get a reconnect connection
func Dial(url string) (*Connection, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	connection := &Connection{
		Connection: conn,
	}

	go func() {
		for {
			reason, ok := <-connection.Connection.NotifyClose(make(chan *amqp.Error))
			// exit this goroutine if closed by developer
			if !ok {
				debug("connection closed")
				break
			}
			debugf("connection closed, reason: %v", reason)

			// reconnect if not closed by developer
			for {
				// wait 1s for reconnect
				time.Sleep(delay * time.Second)

				conn, err := amqp.Dial(url)
				if err == nil {
					connection.Connection = conn
					debugf("reconnect success")
					break
				}

				debugf("reconnect failed, err: %v", err)
			}
		}
	}()

	return connection, nil
}
