package events

//记录事件，用于web界面显示

import (
	"container/list"
	"fmt"
	"sync"
	"time"
)

type Event struct {
	Timestamp time.Time `json:"timestamp"`
	Message   string    `json:"message"`
	Type      string    `json:"type"` // e.g., "election", "commit", "fault"
}

const maxEvents = 50 // Keep a buffer of the last 50 events

var (
	eventLog = list.New()
	mu       sync.RWMutex
)

// Log adds a new event to the global event log.
func Log(eventType, format string, a ...interface{}) {
	mu.Lock()
	defer mu.Unlock()

	event := Event{
		Timestamp: time.Now(),
		Message:   fmt.Sprintf(format, a...),
		Type:      eventType,
	}

	eventLog.PushFront(event)
	if eventLog.Len() > maxEvents {
		eventLog.Remove(eventLog.Back())
	}
}

// GetAll returns a slice of all events currently in the log.
func GetAll() []Event {
	mu.RLock()
	defer mu.RUnlock()

	events := make([]Event, 0, eventLog.Len())
	for e := eventLog.Front(); e != nil; e = e.Next() {
		events = append(events, e.Value.(Event))
	}
	return events
}
