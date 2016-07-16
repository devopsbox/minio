/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"
)

// amqpQueue - represents logrus compatible AMQP hook.
// All fields represent AMQP configuration details.
type amqpQueue struct {
	AMQPServer   string
	Username     string
	Password     string
	Exchange     string
	RoutingKey   string
	exchangeType string
	mandatory    bool
	immediate    bool
	durable      bool
	internal     bool
	noWait       bool
	autoDeleted  bool
}

func enableAMQPQueue(amqpArn string) error {
	amqpParams := strings.Split(amqpArn, ":")
	server := amqpParams[0]
	port := amqpParams[1]
	server = server + ":" + port
	username := amqpParams[2]
	password := amqpParams[3]
	exchange := amqpParams[4]
	routingKey := amqpParams[5]

	dialURL := "amqp://" + username + ":" + password + "@" + server + "/"
	conn, err := amqp.Dial(dialURL)
	if err != nil {
		return err
	}
	defer conn.Close()

	amqpq := &amqpQueue{
		AMQPServer: server,
		Username:   username,
		Password:   password,
		Exchange:   exchange,
		RoutingKey: routingKey,
	}

	// Add a amqp hook.
	log.Hooks.Add(amqpq)
	// Set default JSON formatter.
	log.Formatter = new(logrus.JSONFormatter)

	// Successfully enabled.
	return nil
}

// Fire is called when an event should be sent to the message broker.
func (q *amqpQueue) Fire(entry *logrus.Entry) error {
	q.exchangeType = "direct"
	q.durable = true

	dialURL := "amqp://" + q.Username + ":" + q.Password + "@" + q.AMQPServer + "/"
	conn, err := amqp.Dial(dialURL)
	if err != nil {
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return nil
	}
	defer ch.Close()

	err = ch.ExchangeDeclare(
		q.Exchange,
		q.exchangeType,
		q.durable,
		q.autoDeleted,
		q.internal,
		q.noWait,
		nil,
	)
	if err != nil {
		return err
	}

	body, err := entry.String()
	if err != nil {
		return err
	}

	err = ch.Publish(
		q.Exchange,
		q.RoutingKey,
		q.mandatory,
		q.immediate,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})
	if err != nil {
		return err
	}

	return nil
}

// Levels is available logging levels.
func (q *amqpQueue) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.PanicLevel,
		logrus.FatalLevel,
		logrus.ErrorLevel,
		logrus.WarnLevel,
		logrus.InfoLevel,
		logrus.DebugLevel,
	}
}
