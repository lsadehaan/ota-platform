package controlplane

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 4096
)

// WSEvent represents an event broadcast to WebSocket clients.
type WSEvent struct {
	Type       string      `json:"type"` // "campaign_progress", "dlr_update", "card_status", "system_event"
	CampaignID string      `json:"campaign_id,omitempty"`
	CardID     string      `json:"card_id,omitempty"`
	Data       interface{} `json:"data"`
}

// WSHub manages WebSocket connections and broadcasts.
type WSHub struct {
	clients    map[*WSClient]bool
	broadcast  chan *WSEvent
	register   chan *WSClient
	unregister chan *WSClient
	mu         sync.RWMutex
	logger     *zap.Logger
}

// WSClient represents a single WebSocket connection.
type WSClient struct {
	hub    *WSHub
	conn   *websocket.Conn
	send   chan []byte
	topics map[string]bool // subscribed topics/channels
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		origin := r.Header.Get("Origin")
		if origin == "" {
			return true
		}
		allowed := os.Getenv("ALLOWED_ORIGINS")
		if allowed == "" {
			allowed = "http://localhost:3001"
		}
		for _, o := range strings.Split(allowed, ",") {
			if strings.TrimSpace(o) == origin {
				return true
			}
		}
		return false
	},
}

// NewWSHub creates a new WebSocket hub.
func NewWSHub(logger *zap.Logger) *WSHub {
	return &WSHub{
		clients:    make(map[*WSClient]bool),
		broadcast:  make(chan *WSEvent, 256),
		register:   make(chan *WSClient),
		unregister: make(chan *WSClient),
		logger:     logger,
	}
}

// Run starts the hub's event loop (run as goroutine).
func (h *WSHub) Run(ctx context.Context) {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			h.mu.Unlock()
			h.logger.Debug("websocket client connected",
				zap.Int("total_clients", len(h.clients)),
			)

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
			}
			h.mu.Unlock()
			h.logger.Debug("websocket client disconnected",
				zap.Int("total_clients", len(h.clients)),
			)

		case event := <-h.broadcast:
			data, err := json.Marshal(event)
			if err != nil {
				h.logger.Error("failed to marshal websocket event", zap.Error(err))
				continue
			}
			h.mu.RLock()
			for client := range h.clients {
				select {
				case client.send <- data:
				default:
					// Client buffer full; disconnect it.
					go func(c *WSClient) {
						h.unregister <- c
					}(client)
				}
			}
			h.mu.RUnlock()

		case <-ctx.Done():
			return
		}
	}
}

// Broadcast sends an event to all connected clients.
func (h *WSHub) Broadcast(event *WSEvent) {
	select {
	case h.broadcast <- event:
	default:
		h.logger.Warn("websocket broadcast channel full, dropping event",
			zap.String("type", event.Type),
		)
	}
}

// BroadcastToCampaign sends an event only to clients watching a specific campaign.
func (h *WSHub) BroadcastToCampaign(campaignID string, event *WSEvent) {
	data, err := json.Marshal(event)
	if err != nil {
		h.logger.Error("failed to marshal websocket event", zap.Error(err))
		return
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	for client := range h.clients {
		// If the client has no topic subscriptions, send to all.
		// If the client has subscriptions, only send if they are subscribed to this campaign.
		if len(client.topics) == 0 || client.topics["campaign:"+campaignID] {
			select {
			case client.send <- data:
			default:
				go func(c *WSClient) {
					h.unregister <- c
				}(client)
			}
		}
	}
}

// HandleWebSocket is the HTTP handler for upgrading to WebSocket.
func (h *WSHub) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.logger.Error("websocket upgrade failed", zap.Error(err))
		return
	}

	client := &WSClient{
		hub:    h,
		conn:   conn,
		send:   make(chan []byte, 256),
		topics: make(map[string]bool),
	}

	h.register <- client

	go client.writePump()
	go client.readPump()
}

// subscribeMessage is the JSON structure clients send to subscribe to topics.
type subscribeMessage struct {
	Action string `json:"action"` // "subscribe" or "unsubscribe"
	Topic  string `json:"topic"`  // e.g., "campaign:<uuid>"
}

// readPump pumps messages from the WebSocket connection to the hub.
func (c *WSClient) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()

	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				c.hub.logger.Warn("websocket read error", zap.Error(err))
			}
			return
		}

		var msg subscribeMessage
		if err := json.Unmarshal(message, &msg); err != nil {
			c.hub.logger.Debug("ignoring invalid websocket message from client", zap.Error(err))
			continue
		}

		switch msg.Action {
		case "subscribe":
			if msg.Topic != "" {
				c.topics[msg.Topic] = true
				c.hub.logger.Debug("client subscribed to topic", zap.String("topic", msg.Topic))
			}
		case "unsubscribe":
			delete(c.topics, msg.Topic)
			c.hub.logger.Debug("client unsubscribed from topic", zap.String("topic", msg.Topic))
		}
	}
}

// writePump pumps messages from the hub to the WebSocket connection.
func (c *WSClient) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				// Hub closed the channel.
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			w.Write(message)

			// Drain any queued messages into the same write.
			n := len(c.send)
			for i := 0; i < n; i++ {
				w.Write([]byte("\n"))
				w.Write(<-c.send)
			}

			if err := w.Close(); err != nil {
				return
			}

		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
