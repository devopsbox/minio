package main

import "fmt"

// List of valid event types.
var suppportedEventTypes = map[string]struct{}{
	// Object created event types.
	"s3:ObjectCreated:*":                       struct{}{},
	"s3:ObjectCreated:Put":                     struct{}{},
	"s3:ObjectCreated:Post":                    struct{}{},
	"s3:ObjectCreated:Copy":                    struct{}{},
	"s3:ObjectCreated:CompleteMultipartUpload": struct{}{},
	// Object removed event types.
	"s3:ObjectRemoved:*":      struct{}{},
	"s3:ObjectRemoved:Delete": struct{}{},
}

// checkEvent - checks if an event is supported.
func checkEvent(event string) error {
	_, ok := suppportedEventTypes[event]
	if !ok {
		return fmt.Errorf("Unsupported event type detected %s", event)
	}
	return nil
}

// checkEvents - checks given list of events if all of them are valid.
// given if one of them is invalid, this function returns an error.
func checkEvents(events []string) error {
	for _, event := range events {
		if err := checkEvent(event); err != nil {
			return err
		}
	}
	return nil
}
