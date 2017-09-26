package amqphandler

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"os"
	"time"
)

type RabbitmqHandler struct {
	conn        *amqp.Connection
	channel     *amqp.Channel
	uri         string
	amqpError   chan *amqp.Error
	consumerTag string
	queueName   string
	logger      *log.Logger
}

func newRabbitmqHandler(uri, queueName string) *RabbitmqHandler {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "_deviceeventrabbit"
	}
	rabbitmq_handler_instance := &RabbitmqHandler{
		consumerTag: fmt.Sprintf("DeviceEventLogger-%s-%s", hostname, time.Now().Format(time.RFC850)),
		uri:         uri,
		queueName:   queueName,
		logger:      log.New(os.Stderr, "INFO: ", log.LstdFlags|log.Ldate|log.Ltime),
	}
	return rabbitmq_handler_instance
}

func StartRabbitmqHandler(uri, queueName string) {
	rabbitmq_handler := newRabbitmqHandler(uri, queueName)
	rabbitmq_handler.amqpError = make(chan *amqp.Error)
	rabbitmq_handler.Connect()

	go func() {
		for {
			<-rabbitmq_handler.amqpError
			rabbitmq_handler.logger.Println("AMQP Error! Try to reconnect")
			rabbitmq_handler.amqpError = make(chan *amqp.Error)
			rabbitmq_handler.Connect()
		}
	}()

}

func (r *RabbitmqHandler) Connect() {
	for {
		time.Sleep(1000 * time.Millisecond)
		r.logger.Printf("[%s][%s][%s]", r.uri, r.consumerTag, r.queueName)
		var err error
		r.conn, err = amqp.Dial(r.uri)
		if err != nil {
			fmt.Printf("amqp.Dial error, [err:%s]", err)
			continue
		}

		err = r.OpenChannel()
		if err != nil {
			r.logger.Printf("amqp. open channel error, [err:%s]", err)
			r.channel.Close()
			r.conn.Close()
			continue
		}

		err = r.PrepareQueue()
		if err != nil {
			r.logger.Printf("Prepare Queue error! : [err:%s]", err)
			r.conn.Close()
			continue
		}

		err = r.PrepareConsume()
		if err != nil {
			r.logger.Printf("Prepare Consume error, [err:%s]", err)
			r.conn.Close()
			continue
		}

		r.conn.NotifyClose(r.amqpError)
		break
	}
}

func (r *RabbitmqHandler) OpenChannel() error {
	var err error
	r.channel, err = r.conn.Channel()
	if err != nil {
		return fmt.Errorf("Channel Open Error, [err:%s]", err)
	}
	return nil
}

func (r *RabbitmqHandler) PrepareQueue() error {
	_, err := r.channel.QueueDeclare(
		r.queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("Prepare Queue Failed")
	}
	return nil
}

func (r *RabbitmqHandler) PrepareConsume() error {
	var err error
	amqp_delivery, err := r.channel.Consume(
		r.queueName,
		r.consumerTag,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("Prepare Consume Failed")
	}
	go func() {
		for d := range amqp_delivery {
			r.logger.Printf("Received a message: %s", d.Body)
		}
	}()

	return nil
}
