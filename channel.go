package gorabbitmq

import (
	"github.com/Noobygames/amqp"
	"sync/atomic"
	"time"
)

// Channel amqp.Channel wapper
type Channel struct {
	*amqp.Channel
	errChan   chan *amqp.Error
	closed    int32
	connected int32
}

// IsConnected indicate if currently connected to rabbitmq
func (ch *Channel) IsConnected() bool {
	return (atomic.LoadInt32(&ch.connected) == 1)
}

// SetConnected flag channel connection to rabbitmq
func (ch *Channel) SetConnected(v int32) {
	atomic.StoreInt32(&ch.connected, v)
}

// IsClosed indicate closed by developer
func (ch *Channel) IsClosed() bool {
	return (atomic.LoadInt32(&ch.closed) == 1)
}

// Close ensure closed flag set
func (ch *Channel) Close() error {
	if ch.IsClosed() {
		return amqp.ErrClosed
	}

	atomic.StoreInt32(&ch.closed, 1)

	return ch.Channel.Close()
}

// Channel wrap amqp.Connection.Channel, get a auto reconnect channel
func (c *Connection) Channel() (*Channel, <-chan *amqp.Error, error) {
	ch, err := c.Connection.Channel()
	if err != nil {
		return nil, nil, err
	}

	channel := &Channel{
		Channel:   ch,
		connected: 1,
		errChan:   make(chan *amqp.Error),
	}

	go func() {
		for {
			reason, ok := <-channel.Channel.NotifyClose(make(chan *amqp.Error))
			// exit this goroutine if closed by developer
			if !ok || channel.IsClosed() {
				debug("channel closed")
				channel.Close() // close again, ensure closed flag set when connection closed
				break
			}

			if reason != nil {
				channel.SetConnected(0)
				channel.errChan <- reason
			}

			debug("channel closed, reason: %v", reason)

			// reconnect if not closed by developer
			for {
				// wait x seconds for connection reconnect
				time.Sleep(delay * time.Second)

				ch, err := c.Connection.Channel()
				if err == nil {
					debug("channel recreate success")
					channel.Channel = ch
					channel.SetConnected(1)
					break
				}
				//if reason.Code == http.StatusNotFound && reason.Reason

				debugf("channel recreate failed, err: %v", err)
			}
		}

	}()

	return channel, channel.errChan, nil
}

// Consume warp amqp.Channel.Consume, the returned delivery will end only when channel closed by developer
func (ch *Channel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	deliveries := make(chan amqp.Delivery)

	go func() {
		for {
			d, err := ch.Channel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
			if err != nil {
				debugf("consume failed, err: %v", err)
				time.Sleep(delay * time.Second)
				continue
			}

			for msg := range d {
				deliveries <- msg
			}

			// sleep before IsClose call. closed flag may not set before sleep.
			time.Sleep(delay * time.Second)

			if ch.IsClosed() {
				break
			}
		}
	}()

	return deliveries, nil
}
