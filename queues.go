package main

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
)

// Maintains queue map for validating input fields.
var queueFields = map[string]int{
	"logrus-amqp": 6,
	// Add new queues here.
}

// Returns true if queueArn is for an AMQP queue.
func isAMQPQueue(queueArn string) bool {
	queueParams := strings.Split(queueArn, ":")
	// Sets to 'true' if last field in queueArn is 'logrus-amqp'.
	amqpQ := queueParams[len(queueParams)-1] == "logrus-amqp"
	// Sets to 'true' if total fields available are equal to known value.
	amqpF := queueFields["logrus-amqp"] == (len(queueParams) - 1)
	return amqpQ && amqpF
}

// Match function matches wild cards in 'pattern' for events.
func eventMatch(eventType EventName, events []string) (ok bool) {
	for _, event := range events {
		ok = wildCardMatch(event, eventType.String())
		if ok {
			break
		}
	}
	return ok
}

func notifyObjectCreatedEvent(eventType EventName, bucket string, object string, etag string, size int64) {
	region := serverConfig.GetRegion()
	sequencer := fmt.Sprintf("%X", time.Now().UTC().UnixNano())
	events := []*NotificationEvent{
		&NotificationEvent{
			EventVersion:      "2.0",
			EventSource:       "aws:s3",
			AwsRegion:         region,
			EventTime:         time.Now().UTC(),
			EventName:         eventType.String(),
			UserIdentity:      defaultIdentity(),
			RequestParameters: make(map[string]string), // TODO - not supported yet.
			ResponseElements:  make(map[string]string), // TODO - not supported yet.
			S3: s3Reference{
				SchemaVersion:   "1.0",
				ConfigurationID: "Config",
				Bucket: s3BucketReference{
					Name:          bucket,
					OwnerIdentity: defaultIdentity(),
					ARN:           "arn:aws:s3:::" + bucket,
				},
				Object: s3ObjectReference{
					Key:       url.QueryEscape(object),
					ETag:      etag,
					Size:      size,
					Sequencer: sequencer,
				},
			},
		},
	}
	for _, qConfig := range notificationCfg.QueueConfigurations {
		if isAMQPQueue(qConfig.QueueArn) {
			if eventMatch(eventType, qConfig.Events) {
				log.WithFields(logrus.Fields{
					"Records": events,
				})
			}
		}
		// Notify for more queue support here.
	}
}

func notifyObjectDeletedEvent(bucket, object string) {
	region := serverConfig.GetRegion()
	sequencer := fmt.Sprintf("%X", time.Now().UTC().UnixNano())
	events := []*NotificationEvent{
		&NotificationEvent{
			EventVersion:      "2.0",
			EventSource:       "aws:s3",
			AwsRegion:         region,
			EventTime:         time.Now().UTC(),
			EventName:         ObjectRemovedDelete.String(),
			UserIdentity:      defaultIdentity(),
			RequestParameters: make(map[string]string), // TODO - not supported yet.
			ResponseElements:  make(map[string]string), // TODO - not supported yet.
			S3: s3Reference{
				SchemaVersion:   "1.0",
				ConfigurationID: "Config",
				Bucket: s3BucketReference{
					Name:          bucket,
					OwnerIdentity: defaultIdentity(),
					ARN:           "arn:aws:s3:::" + bucket,
				},
				Object: s3ObjectReference{
					Key:       url.QueryEscape(object),
					Sequencer: sequencer,
				},
			},
		},
	}
	for _, qConfig := range notificationCfg.QueueConfigurations {
		if isAMQPQueue(qConfig.QueueArn) {
			if eventMatch(ObjectRemovedDelete, qConfig.Events) {
				log.WithFields(logrus.Fields{
					"Records": events,
				})
			}
		}
		// Notify for more queue support here.
	}
}
