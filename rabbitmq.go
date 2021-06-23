package gorabbitmq

import (
	"errors"
	"fmt"
	"github.com/Noobygames/amqp"
	"github.com/Noobygames/go-amqp-reconnect/rabbitmq"
	"log"
	"sync"
	"sync/atomic"
)

const prefix = "rabbitmq-lib "

type RabbitMQ interface {
	Close() error
	CheckHealth() (err error)
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	ExchangeBind(destination, key, source string, noWait bool, args amqp.Table) error
	ExchangeDelete(name string, ifUnused, noWait bool) error
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
	QueueUnbind(name, key, exchange string, args amqp.Table) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueDelete(name string, ifUnused, ifEmpty, noWait bool) (int, error)
	Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, prefetchCount, prefetchSize int, args amqp.Table) <-chan amqp.Delivery
	CreateChannel() (*Channel, error)
	ConsumeQos(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, prefetchCount, prefetchSize int, args amqp.Table) (<-chan amqp.Delivery, error)
	PublishWithChannel(channel *rabbitmq.Channel, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
}

type service struct {
	uri         string
	pubMutex    sync.Mutex
	publishChan *Channel
	conn        *Connection
	healthy     int32
}

func NewRabbitMQ(settings ConnectionSettings) (RabbitMQ, error) {
	rabbitMQ := service{
	}

	uri := amqp.URI{
		Host:     settings.Host,
		Username: settings.UserName,
		Password: settings.Password,
		Port:     settings.Port,
		Vhost:    "/",
		Scheme:   "amqp",
	}

	rabbitMQ.uri = uri.String()
	if err := rabbitMQ.connect(); err != nil {
		log.Print(prefix, err)
		return &rabbitMQ, err
	}
	rabbitMQ.SetHealthy(true)

	return &rabbitMQ, nil
}

// IsHealthy indicate if currently connected to rabbitmq
func (s *service) IsHealthy() bool {
	return (atomic.LoadInt32(&s.healthy) == 1)
}

// SetHealthy flag service completly connected to rabbitmq
func (s *service) SetHealthy(healthy bool) {
	if healthy {
		atomic.StoreInt32(&s.healthy, 1)
	} else {
		atomic.StoreInt32(&s.healthy, 0)
	}
}

// CheckHealth checks rabbitmq connection health
func (s *service) CheckHealth() (err error) {
	prefix := "rabbitmq healthcheck: "
	defer func() {
		if err != nil {
			err = errors.New(prefix + err.Error())
		}
	}()
	if s.conn == nil {
		err = errors.New("rabbitmq connection is closed")
		return err
	}

	if s.publishChan == nil {
		err = errors.New("rabbitmq channel is closed")
		return err
	}
	if !s.publishChan.IsConnected() {
		err = errors.New(fmt.Sprint("rabbitmq created channel failed:", err))
		return err
	}

	if !s.IsHealthy() {
		err = errors.New(fmt.Sprint("rabbitmq channel/s closed unexpetedly:", err))
		return err
	}

	return nil
}

func (s *service) CreateChannel() (*Channel, error) {
	return s.createChannel()
}

func (s *service) createChannel() (*Channel, error) {
	if s.conn == nil {
		err := errors.New(prefix + "no connection available")
		log.Print(prefix, err)
		return nil, err
	}
	channel, _, err := s.conn.Channel()
	if err != nil {
		log.Print(prefix, err)
		return channel, err
	}

	return channel, err
}

func (s *service) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	s.pubMutex.Lock()
	defer s.pubMutex.Unlock()
	if s.publishChan == nil || s.publishChan.IsClosed() {
		return fmt.Errorf("rabbitmq chan not connected")
	}
	return s.publishChan.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, args)
}

func (s *service) ExchangeBind(destination, key, source string, noWait bool, args amqp.Table) error {
	s.pubMutex.Lock()
	defer s.pubMutex.Unlock()
	if s.publishChan == nil || s.publishChan.IsClosed() {
		return fmt.Errorf("rabbitmq chan not connected")
	}
	return s.publishChan.ExchangeBind(destination, key, source, noWait, args)
}

func (s *service) ExchangeDelete(name string, ifUnused, noWait bool) error {
	s.pubMutex.Lock()
	defer s.pubMutex.Unlock()
	if s.publishChan == nil || s.publishChan.IsClosed() {
		return fmt.Errorf("rabbitmq chan not connected")
	}
	return s.publishChan.ExchangeDelete(name, ifUnused, noWait)
}

func (s *service) QueueDelete(name string, ifUnused, ifEmpty, noWait bool) (int, error) {
	s.pubMutex.Lock()
	defer s.pubMutex.Unlock()
	if s.publishChan == nil || s.publishChan.IsClosed() {
		return -1, fmt.Errorf("rabbitmq chan not connected")
	}
	return s.publishChan.QueueDelete(name, ifUnused, ifEmpty, noWait)
}

func (s *service) QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error {
	s.pubMutex.Lock()
	defer s.pubMutex.Unlock()
	if s.publishChan == nil || s.publishChan.IsClosed() {
		return fmt.Errorf("rabbitmq chan not connected")
	}
	return s.publishChan.QueueBind(name, key, exchange, noWait, args)
}

func (s *service) QueueUnbind(name, key, exchange string, args amqp.Table) error {
	s.pubMutex.Lock()
	defer s.pubMutex.Unlock()
	defer s.publishChan.Close()
	return s.publishChan.QueueUnbind(name, key, exchange, args)
}

func (s *service) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	s.pubMutex.Lock()
	defer s.pubMutex.Unlock()
	if s.publishChan == nil || s.publishChan.IsClosed() {
		return amqp.Queue{}, fmt.Errorf("rabbitmq chan not connected")
	}
	return s.publishChan.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
}

func (s *service) PublishWithChannel(channel *rabbitmq.Channel, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	if channel == nil {
		return errors.New("channel is nil")
	}
	err := channel.Publish(exchange, key, mandatory, immediate, msg)
	if err != nil {
		log.Print(prefix, err)
		return err
	}
	return nil
}

func (s *service) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	s.pubMutex.Lock()
	defer s.pubMutex.Unlock()
	err := s.publishChan.Publish(exchange, key, mandatory, immediate, msg)
	if err != nil {
		log.Print(prefix, err)
		return err
	}
	return nil
}

func (s *service) ConsumeQos(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, prefetchCount, prefetchSize int, args amqp.Table) (<-chan amqp.Delivery, error) {
	s.pubMutex.Lock()
	defer s.pubMutex.Unlock()
	if s.publishChan == nil || s.publishChan.IsClosed() {
		return nil, fmt.Errorf("rabbitmq chan not connected")
	}
	if err := s.publishChan.Qos(prefetchCount, prefetchSize, false); err != nil {
		return nil, err
	}
	return s.publishChan.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
}

func (s *service) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, prefetchCount, prefetchSize int, args amqp.Table) <-chan amqp.Delivery {
	config := consumerConfig{
		queue:         queue,
		noWait:        noWait,
		noLocal:       noLocal,
		exclusive:     exclusive,
		autoAck:       autoAck,
		consumer:      consumer,
		args:          args,
		prefetchCount: prefetchCount,
		prefetchSize:  prefetchSize,
	}
	externalDelivery := make(chan amqp.Delivery)
	channelWrapper := channelWrapper{
		originalDelivery: nil,
		externalDelivery: &externalDelivery,
		channel:          nil,
	}

	config.channelWrapper = channelWrapper
	if s.conn == nil {
		err := errors.New(prefix + "no connection available")
		log.Print(prefix, err)
		return *channelWrapper.externalDelivery
	}
	_ = s.connectConsumerWorker(&config)

	return *channelWrapper.externalDelivery
}

func (s *service) connectConsumerWorker(config *consumerConfig) (err error) {
	queueChan, errChan, err := s.conn.Channel()
	if err != nil {
		log.Print(prefix, err)
		return
	}
	err = queueChan.Qos(config.prefetchCount, config.prefetchSize, false)
	if err != nil {
		log.Print(prefix, err)
		return
	}
	chanDeliveries, err := queueChan.Consume(config.queue, config.consumer, config.autoAck, config.exclusive, config.noLocal, config.noWait, config.args)
	if err != nil {
		log.Print(prefix, err)
		return
	}
	config.channelWrapper.channel = queueChan
	config.errChan = errChan
	if chanDeliveries != nil {
		config.channelWrapper.originalDelivery = &chanDeliveries
	} else {
		return
	}
	log.Printf("starting consume worker config=%+v", *config)
	go s.runConsumerWorker(config)
	return nil
}

//async worker with nonblocking routing of deliveries and stop channel
func (s *service) runConsumerWorker(config *consumerConfig) {
	for {
		select {
		case reason, _ := <-config.errChan:
			if reason != nil {
				log.Print(prefix, " consumer channel ", reason)
			} else {
				log.Print(prefix, "errChan for consumer channel was cloesd")
			}
			s.SetHealthy(false)

		case delivery, isOpen := <-*config.originalDelivery:
			{
				if !isOpen {
					log.Print(prefix, "consume worker amqp.Delivery channel was closed by rabbitmq server")
					return
				}
				//route message through
				*config.externalDelivery <- delivery
			}
		}
	}
}

func (s *service) connect() error {
	var err error

	if s.conn != nil && !s.conn.IsClosed() {
		if err := s.conn.Close(); err != nil {
			log.Print(err)
		}
	}

	s.conn, err = Dial(s.uri)
	if err != nil {
		log.Print(prefix, err)
		return err
	}

	publishChan, errChan, err := s.conn.Channel()
	if err != nil {
		log.Print(prefix, err)
		return err
	}
	go func() {
		select {
		case reason, _ := <-errChan:
			if reason != nil {
				log.Print(prefix, " publish channel ", reason)
			} else {
				log.Print(prefix, "errChan for publish channel was cloesd")
			}
			s.SetHealthy(false)
		}
	}()
	s.publishChan = publishChan
	err = publishChan.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		log.Print(prefix, err)
	}

	log.Print(prefix, " rabbitmq service is connected!")
	return nil
}

func (s *service) Close() error {
	if s.publishChan != nil {
		err := s.publishChan.Close()
		if err != nil {
			return err
		}
	}
	if s.conn != nil {
		err := s.conn.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
