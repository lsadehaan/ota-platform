package mocksmsc

import (
	"sync"
	"time"
)

// StoredMessage represents a message received and stored by the mock SMSC.
type StoredMessage struct {
	MessageID  string
	SourceAddr string
	DestAddr   string
	Payload    []byte
	ReceivedAt time.Time
	DLRSent    bool
	DLRStatus  string
	MOSent     bool
}

// MessageStore provides thread-safe storage for messages received by the mock SMSC.
type MessageStore struct {
	messages map[string]*StoredMessage
	mu       sync.RWMutex
}

// NewMessageStore creates a new empty MessageStore.
func NewMessageStore() *MessageStore {
	return &MessageStore{
		messages: make(map[string]*StoredMessage),
	}
}

// Add stores a message, keyed by its MessageID.
func (ms *MessageStore) Add(msg *StoredMessage) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.messages[msg.MessageID] = msg
}

// Get retrieves a message by its MessageID. Returns nil if not found.
func (ms *MessageStore) Get(messageID string) *StoredMessage {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.messages[messageID]
}

// List returns all stored messages as a slice.
func (ms *MessageStore) List() []*StoredMessage {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	result := make([]*StoredMessage, 0, len(ms.messages))
	for _, msg := range ms.messages {
		result = append(result, msg)
	}
	return result
}

// MarkDLRSent updates the DLR status for a stored message, indicating a DLR has been sent.
func (ms *MessageStore) MarkDLRSent(messageID string, status string) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if msg, ok := ms.messages[messageID]; ok {
		msg.DLRSent = true
		msg.DLRStatus = status
	}
}

// MarkMOSent marks that an MO was emitted for a stored message.
func (ms *MessageStore) MarkMOSent(messageID string) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if msg, ok := ms.messages[messageID]; ok {
		msg.MOSent = true
	}
}
