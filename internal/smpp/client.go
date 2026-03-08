package smpp

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// Config holds the SMPP transceiver connection parameters.
type Config struct {
	Host           string
	Port           int
	SystemID       string
	Password       string
	SystemType     string
	SourceAddr     string
	SourceAddrTON  byte
	SourceAddrNPI  byte
	EnquireLinkSec int
}

// SubmitRequest represents an SMS to submit via SMPP.
type SubmitRequest struct {
	MSISDN      string
	DestTON     byte
	DestNPI     byte
	ESMClass    byte
	ProtocolID  byte
	DataCoding  byte
	Payload     []byte // binary payload (UDH + 03.48 packet)
	RegisterDLR bool
}

// SubmitResponse contains the SMSC's response to a submit_sm.
type SubmitResponse struct {
	MessageID string
	Error     error
}

// DeliverHandler is called when a deliver_sm PDU is received (DLR or MO).
type DeliverHandler func(sourceAddr string, destAddr string, esmClass byte, payload []byte)

// Client manages an SMPP transceiver connection over raw TCP.
type Client struct {
	config         Config
	handler        DeliverHandler
	deliverWorkers int
	logger         *zap.Logger
	mu             sync.Mutex
	conn           net.Conn
	bound          bool
	seqNum         uint32
	pending        map[uint32]chan *PDU
	pendingMu      sync.Mutex
	done           chan struct{}
	deliverQ       chan deliverMessage
}

type deliverMessage struct {
	sourceAddr string
	destAddr   string
	esmClass   byte
	payload    []byte
}

// NewClient creates a new SMPP transceiver client.
func NewClient(config Config, handler DeliverHandler, logger *zap.Logger) *Client {
	return NewClientWithWorkers(config, handler, 8, logger)
}

// NewClientWithWorkers creates a new SMPP transceiver client with a configurable
// number of deliver handler workers.
func NewClientWithWorkers(config Config, handler DeliverHandler, deliverWorkers int, logger *zap.Logger) *Client {
	if config.EnquireLinkSec <= 0 {
		config.EnquireLinkSec = 30
	}
	if deliverWorkers <= 0 {
		deliverWorkers = 8
	}
	return &Client{
		config:         config,
		handler:        handler,
		deliverWorkers: deliverWorkers,
		logger:         logger,
		pending:        make(map[uint32]chan *PDU),
		done:           make(chan struct{}),
		deliverQ:       make(chan deliverMessage, 1024),
	}
}

// nextSeq returns the next sequence number (1-based, wrapping).
func (c *Client) nextSeq() uint32 {
	return atomic.AddUint32(&c.seqNum, 1)
}

// Connect establishes the TCP connection and performs the SMPP bind_transceiver handshake.
func (c *Client) Connect(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.bound {
		return fmt.Errorf("already bound")
	}

	addr := fmt.Sprintf("%s:%d", c.config.Host, c.config.Port)
	c.logger.Info("connecting to SMSC", zap.String("addr", addr))

	dialer := net.Dialer{Timeout: 10 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("dial SMSC at %s: %w", addr, err)
	}
	c.conn = conn
	c.done = make(chan struct{})
	c.deliverQ = make(chan deliverMessage, 1024)

	// Start the reader goroutine before sending bind so we can receive the response.
	c.startDeliverLoop()
	go c.ReadLoop()

	// Send bind_transceiver.
	seq := c.nextSeq()
	bindBody := EncodeBindTransceiver(c.config.SystemID, c.config.Password, c.config.SystemType)
	bindPDU := &PDU{
		CommandID:      CmdBindTransceiver,
		CommandStatus:  StatusOK,
		SequenceNumber: seq,
		Body:           bindBody,
	}

	respCh := c.registerPending(seq)

	if err := c.writePDU(bindPDU); err != nil {
		c.unregisterPending(seq)
		conn.Close()
		return fmt.Errorf("send bind_transceiver: %w", err)
	}

	// Wait for bind response.
	select {
	case resp := <-respCh:
		if resp.CommandStatus != StatusOK {
			conn.Close()
			return fmt.Errorf("bind_transceiver failed with status 0x%08X", resp.CommandStatus)
		}
		c.bound = true
		c.logger.Info("SMPP bind successful",
			zap.String("system_id", c.config.SystemID),
		)
	case <-time.After(15 * time.Second):
		conn.Close()
		return fmt.Errorf("bind_transceiver response timeout")
	case <-ctx.Done():
		conn.Close()
		return ctx.Err()
	}

	// Start enquire_link keepalive loop.
	go c.enquireLinkLoop()

	return nil
}

// Submit sends an SMS via SMPP submit_sm and waits for the response.
func (c *Client) Submit(req *SubmitRequest) (*SubmitResponse, error) {
	c.mu.Lock()
	if !c.bound {
		c.mu.Unlock()
		return nil, fmt.Errorf("not bound to SMSC")
	}
	c.mu.Unlock()

	var registeredDelivery byte
	if req.RegisterDLR {
		registeredDelivery = 0x01
	}

	body := EncodeSubmitSM(
		c.config.SourceAddr,
		c.config.SourceAddrTON,
		c.config.SourceAddrNPI,
		req.MSISDN,
		req.DestTON,
		req.DestNPI,
		req.ESMClass,
		req.ProtocolID,
		req.DataCoding,
		registeredDelivery,
		req.Payload,
	)

	seq := c.nextSeq()
	pdu := &PDU{
		CommandID:      CmdSubmitSM,
		CommandStatus:  StatusOK,
		SequenceNumber: seq,
		Body:           body,
	}

	respCh := c.registerPending(seq)

	if err := c.writePDU(pdu); err != nil {
		c.unregisterPending(seq)
		return nil, fmt.Errorf("send submit_sm: %w", err)
	}

	// Wait for submit_sm_resp.
	select {
	case resp := <-respCh:
		if resp.CommandStatus != StatusOK {
			return &SubmitResponse{
				Error: fmt.Errorf("submit_sm failed with status 0x%08X", resp.CommandStatus),
			}, nil
		}
		msgID := ParseSubmitSMResp(resp.Body)
		c.logger.Debug("submit_sm_resp received",
			zap.String("message_id", msgID),
			zap.String("msisdn", req.MSISDN),
		)
		return &SubmitResponse{MessageID: msgID}, nil
	case <-time.After(30 * time.Second):
		c.unregisterPending(seq)
		return nil, fmt.Errorf("submit_sm response timeout")
	}
}

// Close unbinds from the SMSC and closes the TCP connection.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.bound {
		return nil
	}

	c.logger.Info("unbinding from SMSC")

	seq := c.nextSeq()
	unbindPDU := &PDU{
		CommandID:      CmdUnbind,
		CommandStatus:  StatusOK,
		SequenceNumber: seq,
		Body:           nil,
	}
	// Best-effort unbind; don't wait for response.
	_ = c.writePDU(unbindPDU)

	c.bound = false
	close(c.done)

	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// IsBound returns whether the client is currently bound to the SMSC.
func (c *Client) IsBound() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.bound
}

// writePDU encodes and writes a PDU to the connection. Caller must handle locking
// if exclusive access is needed; this method is safe for concurrent use because
// net.Conn.Write is thread-safe per the Go docs.
func (c *Client) writePDU(pdu *PDU) error {
	data := EncodePDU(pdu)
	_, err := c.conn.Write(data)
	if err != nil {
		c.logger.Error("failed to write PDU",
			zap.Uint32("command_id", pdu.CommandID),
			zap.Error(err),
		)
	}
	return err
}

// registerPending creates a channel for receiving the response to a request
// with the given sequence number.
func (c *Client) registerPending(seq uint32) chan *PDU {
	ch := make(chan *PDU, 1)
	c.pendingMu.Lock()
	c.pending[seq] = ch
	c.pendingMu.Unlock()
	return ch
}

// unregisterPending removes and returns the pending response channel for a
// given sequence number.
func (c *Client) unregisterPending(seq uint32) chan *PDU {
	c.pendingMu.Lock()
	ch := c.pending[seq]
	delete(c.pending, seq)
	c.pendingMu.Unlock()
	return ch
}

// ReadLoop continuously reads PDUs from the connection and dispatches them.
func (c *Client) ReadLoop() {
	headerBuf := make([]byte, pduHeaderLen)

	for {
		select {
		case <-c.done:
			return
		default:
		}

		// Read PDU header (16 bytes).
		_, err := io.ReadFull(c.conn, headerBuf)
		if err != nil {
			select {
			case <-c.done:
				return // connection closed intentionally
			default:
			}
			c.logger.Error("failed to read PDU header", zap.Error(err))
			c.handleDisconnect()
			return
		}

		cmdLen := binary.BigEndian.Uint32(headerBuf[0:4])
		if cmdLen < pduHeaderLen {
			c.logger.Error("invalid PDU command_length", zap.Uint32("length", cmdLen))
			c.handleDisconnect()
			return
		}

		// Read the rest of the PDU body.
		fullPDU := make([]byte, cmdLen)
		copy(fullPDU, headerBuf)
		if cmdLen > pduHeaderLen {
			_, err := io.ReadFull(c.conn, fullPDU[pduHeaderLen:])
			if err != nil {
				c.logger.Error("failed to read PDU body", zap.Error(err))
				c.handleDisconnect()
				return
			}
		}

		pdu, err := DecodePDU(fullPDU)
		if err != nil {
			c.logger.Error("failed to decode PDU", zap.Error(err))
			continue
		}

		c.dispatchPDU(pdu)
	}
}

// dispatchPDU routes an incoming PDU to the correct handler.
func (c *Client) dispatchPDU(pdu *PDU) {
	switch pdu.CommandID {
	case CmdBindTransceiverResp, CmdSubmitSMResp, CmdUnbindResp:
		// Response to a request we sent -- deliver to the pending channel.
		ch := c.unregisterPending(pdu.SequenceNumber)
		if ch != nil {
			ch <- pdu
		} else {
			c.logger.Warn("received response for unknown sequence",
				zap.Uint32("command_id", pdu.CommandID),
				zap.Uint32("sequence", pdu.SequenceNumber),
			)
		}

	case CmdDeliverSM:
		// Incoming deliver_sm: acknowledge immediately and queue handler work
		// off the socket read loop. Ordering is preserved per SMPP connection.
		sourceAddr, destAddr, esmClass, shortMessage := ParseDeliverSM(pdu.Body)

		resp := EncodeDeliverSMResp(pdu.SequenceNumber)
		if err := c.writePDU(resp); err != nil {
			c.logger.Error("failed to send deliver_sm_resp", zap.Error(err))
		}

		c.enqueueDeliver(deliverMessage{
			sourceAddr: sourceAddr,
			destAddr:   destAddr,
			esmClass:   esmClass,
			payload:    append([]byte(nil), shortMessage...),
		})

	case CmdEnquireLink:
		// Respond to enquire_link from SMSC.
		resp := &PDU{
			CommandID:      CmdEnquireLinkResp,
			CommandStatus:  StatusOK,
			SequenceNumber: pdu.SequenceNumber,
		}
		if err := c.writePDU(resp); err != nil {
			c.logger.Error("failed to send enquire_link_resp", zap.Error(err))
		}

	case CmdEnquireLinkResp:
		// Response to our enquire_link; nothing to do, connection is alive.
		c.logger.Debug("enquire_link_resp received")

	case CmdUnbind:
		// SMSC is unbinding us.
		resp := &PDU{
			CommandID:      CmdUnbindResp,
			CommandStatus:  StatusOK,
			SequenceNumber: pdu.SequenceNumber,
		}
		_ = c.writePDU(resp)
		c.handleDisconnect()

	default:
		c.logger.Warn("unhandled PDU command",
			zap.Uint32("command_id", pdu.CommandID),
			zap.Uint32("sequence", pdu.SequenceNumber),
		)
	}
}

// enquireLinkLoop sends periodic enquire_link PDUs to keep the connection alive.
func (c *Client) enquireLinkLoop() {
	ticker := time.NewTicker(time.Duration(c.config.EnquireLinkSec) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
			seq := c.nextSeq()
			pdu := &PDU{
				CommandID:      CmdEnquireLink,
				CommandStatus:  StatusOK,
				SequenceNumber: seq,
			}
			if err := c.writePDU(pdu); err != nil {
				c.logger.Error("failed to send enquire_link", zap.Error(err))
				return
			}
			c.logger.Debug("enquire_link sent", zap.Uint32("sequence", seq))
		}
	}
}

func (c *Client) startDeliverLoop() {
	for i := 0; i < c.deliverWorkers; i++ {
		go func(done <-chan struct{}, q <-chan deliverMessage) {
			for {
				select {
				case <-done:
					return
				case msg := <-q:
					if c.handler != nil {
						c.handler(msg.sourceAddr, msg.destAddr, msg.esmClass, msg.payload)
					}
				}
			}
		}(c.done, c.deliverQ)
	}
}

func (c *Client) enqueueDeliver(msg deliverMessage) {
	select {
	case <-c.done:
		return
	case c.deliverQ <- msg:
	default:
		c.logger.Warn("deliver queue full, blocking until capacity frees")
		select {
		case <-c.done:
			return
		case c.deliverQ <- msg:
		}
	}
}

func (c *Client) handleDisconnect() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.bound {
		return
	}

	c.logger.Warn("SMPP connection lost")
	c.bound = false

	// Signal all goroutines to stop.
	select {
	case <-c.done:
		// Already closed.
	default:
		close(c.done)
	}

	// Drain all pending requests with an error.
	c.pendingMu.Lock()
	for seq, ch := range c.pending {
		ch <- &PDU{
			CommandID:     CmdGenericNack,
			CommandStatus: StatusSysErr,
		}
		delete(c.pending, seq)
	}
	c.pendingMu.Unlock()

	if c.conn != nil {
		c.conn.Close()
	}
}
