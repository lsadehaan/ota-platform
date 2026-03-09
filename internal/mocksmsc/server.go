package mocksmsc

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"net"
	"sync"
	"time"

	"go.uber.org/zap"

	"ota-platform/internal/smpp"
	"ota-platform/pkg/hexutil"
	"ota-platform/pkg/smsframe"
)

// Config holds settings for the mock SMSC server.
type Config struct {
	Port           int
	DLRDelayMs     int     // delay before sending DLR (default 1000)
	DLRSuccessRate float64 // 0.0-1.0, probability of DELIVRD (default 0.95)
	EnableMO       bool    // whether to echo back MO responses
	MODelayMs      int     // delay before sending MO after successful DLR
	MOPayload      []byte  // payload sent as MO/PoR when enabled
}

// Server is a mock SMSC that accepts SMPP connections and simulates DLR responses.
type Server struct {
	config      Config
	listener    net.Listener
	clients     map[net.Conn]*sync.Mutex
	reassembler *smsframe.Reassembler
	store       *MessageStore
	mu          sync.Mutex
	logger      *zap.Logger
	done        chan struct{}
	seqNum      uint32
}

// NewServer creates a new mock SMSC server with the given configuration.
func NewServer(config Config, logger *zap.Logger) *Server {
	if config.DLRDelayMs <= 0 {
		config.DLRDelayMs = 1000
	}
	if config.DLRSuccessRate <= 0 {
		config.DLRSuccessRate = 0.95
	}
	if config.MODelayMs <= 0 {
		config.MODelayMs = 100
	}
	return &Server{
		config:      config,
		clients:     make(map[net.Conn]*sync.Mutex),
		reassembler: smsframe.NewReassembler(),
		store:       NewMessageStore(),
		logger:      logger,
		done:        make(chan struct{}),
	}
}

// Start begins listening for SMPP connections on the configured port.
func (s *Server) Start() error {
	addr := fmt.Sprintf(":%d", s.config.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}
	s.listener = listener
	s.logger.Info("mock SMSC listening", zap.String("addr", addr))

	go s.acceptLoop()
	return nil
}

// Stop gracefully shuts down the server, closing all client connections.
func (s *Server) Stop() {
	s.logger.Info("stopping mock SMSC server")
	close(s.done)

	if s.listener != nil {
		s.listener.Close()
	}

	s.mu.Lock()
	for conn := range s.clients {
		conn.Close()
	}
	s.clients = make(map[net.Conn]*sync.Mutex)
	s.mu.Unlock()

	s.logger.Info("mock SMSC server stopped")
}

// acceptLoop accepts new TCP connections and spawns goroutines to handle them.
func (s *Server) acceptLoop() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.done:
				return
			default:
			}
			s.logger.Error("accept error", zap.Error(err))
			continue
		}

		s.mu.Lock()
		s.clients[conn] = &sync.Mutex{}
		s.mu.Unlock()

		s.logger.Info("new SMPP client connected",
			zap.String("remote_addr", conn.RemoteAddr().String()),
		)

		go s.handleConnection(conn)
	}
}

// handleConnection handles a single SMPP client connection.
// It reads bind_transceiver, validates it, and then loops reading PDUs
// (submit_sm, enquire_link, unbind).
func (s *Server) handleConnection(conn net.Conn) {
	defer func() {
		s.mu.Lock()
		delete(s.clients, conn)
		s.mu.Unlock()
		conn.Close()
		s.logger.Info("SMPP client disconnected",
			zap.String("remote_addr", conn.RemoteAddr().String()),
		)
	}()

	bound := false
	headerBuf := make([]byte, 16) // PDU header is 16 bytes

	for {
		select {
		case <-s.done:
			return
		default:
		}

		// Read PDU header.
		_, err := io.ReadFull(conn, headerBuf)
		if err != nil {
			if err != io.EOF {
				select {
				case <-s.done:
					return
				default:
				}
				s.logger.Debug("read error from client", zap.Error(err))
			}
			return
		}

		cmdLen := binary.BigEndian.Uint32(headerBuf[0:4])
		if cmdLen < 16 {
			s.logger.Error("invalid PDU command_length", zap.Uint32("length", cmdLen))
			return
		}

		// Read the full PDU.
		fullPDU := make([]byte, cmdLen)
		copy(fullPDU, headerBuf)
		if cmdLen > 16 {
			_, err := io.ReadFull(conn, fullPDU[16:])
			if err != nil {
				s.logger.Error("failed to read PDU body", zap.Error(err))
				return
			}
		}

		pdu, err := smpp.DecodePDU(fullPDU)
		if err != nil {
			s.logger.Error("failed to decode PDU", zap.Error(err))
			return
		}

		switch pdu.CommandID {
		case smpp.CmdBindTransceiver:
			// Parse bind fields for logging. The body contains C-strings:
			// system_id, password, system_type, then interface_version, addr_ton, addr_npi, address_range
			systemID := readCStringFromBody(pdu.Body, 0)
			s.logger.Info("bind_transceiver received",
				zap.String("system_id", systemID),
			)

			// Send bind_transceiver_resp with success.
			respBody := writeCStringToBytes("MOCKSMSC")
			resp := &smpp.PDU{
				CommandID:      smpp.CmdBindTransceiverResp,
				CommandStatus:  smpp.StatusOK,
				SequenceNumber: pdu.SequenceNumber,
				Body:           respBody,
			}
			if err := s.writePDU(conn, resp); err != nil {
				s.logger.Error("failed to send bind_transceiver_resp", zap.Error(err))
				return
			}
			bound = true
			s.logger.Info("client bound as transceiver", zap.String("system_id", systemID))

		case smpp.CmdSubmitSM:
			if !bound {
				s.logger.Warn("submit_sm received before bind")
				resp := &smpp.PDU{
					CommandID:      smpp.CmdSubmitSMResp,
					CommandStatus:  smpp.StatusInvBnd,
					SequenceNumber: pdu.SequenceNumber,
					Body:           []byte{0x00},
				}
				s.writePDU(conn, resp)
				continue
			}

			sourceAddr, destAddr, _, shortMessage := smpp.ParseDeliverSM(pdu.Body)

			// Generate a unique message ID.
			s.mu.Lock()
			s.seqNum++
			seq := s.seqNum
			s.mu.Unlock()
			messageID := fmt.Sprintf("MOCK-%d", seq)

			// Store the message.
			storedMsg := &StoredMessage{
				MessageID:  messageID,
				SourceAddr: sourceAddr,
				DestAddr:   destAddr,
				Payload:    shortMessage,
				ReceivedAt: time.Now(),
			}
			s.store.Add(storedMsg)

			// Log the decoded payload.
			s.logger.Info("submit_sm received",
				zap.String("message_id", messageID),
				zap.String("source", sourceAddr),
				zap.String("dest", destAddr),
				zap.Int("payload_len", len(shortMessage)),
				zap.String("payload_hex", hexutil.Encode(shortMessage)),
			)

			// Send submit_sm_resp with the message ID.
			respBody := writeCStringToBytes(messageID)
			resp := &smpp.PDU{
				CommandID:      smpp.CmdSubmitSMResp,
				CommandStatus:  smpp.StatusOK,
				SequenceNumber: pdu.SequenceNumber,
				Body:           respBody,
			}
			if err := s.writePDU(conn, resp); err != nil {
				s.logger.Error("failed to send submit_sm_resp", zap.Error(err))
				return
			}

			// Schedule DLR delivery after the configured delay.
			success := rand.Float64() < s.config.DLRSuccessRate
			go func(c net.Conn, mid string, src string, dest string, ok bool) {
				// Add random jitter (0–100% of base delay) to prevent
				// thundering herd when many submit_sm arrive in a burst.
				jitter := time.Duration(rand.Int63n(int64(s.config.DLRDelayMs)+1)) * time.Millisecond
				select {
				case <-s.done:
					return
				case <-time.After(time.Duration(s.config.DLRDelayMs)*time.Millisecond + jitter):
				}
				s.sendDLR(c, mid, dest, ok)
				if ok && s.config.EnableMO && len(s.config.MOPayload) > 0 {
					moJitter := time.Duration(rand.Int63n(int64(s.config.MODelayMs)+1)) * time.Millisecond
					select {
					case <-s.done:
						return
					case <-time.After(time.Duration(s.config.MODelayMs)*time.Millisecond + moJitter):
					}
					s.sendMO(c, mid, dest, src, s.config.MOPayload)
				}
			}(conn, messageID, sourceAddr, destAddr, success)

		case smpp.CmdEnquireLink:
			resp := &smpp.PDU{
				CommandID:      smpp.CmdEnquireLinkResp,
				CommandStatus:  smpp.StatusOK,
				SequenceNumber: pdu.SequenceNumber,
			}
			if err := s.writePDU(conn, resp); err != nil {
				s.logger.Error("failed to send enquire_link_resp", zap.Error(err))
				return
			}
			s.logger.Debug("enquire_link_resp sent")

		case smpp.CmdDeliverSMResp:
			// The client acknowledges previously delivered DLR/MO PDUs with
			// deliver_sm_resp. This is expected protocol traffic and requires no
			// further action from the mock SMSC.
			s.logger.Debug("deliver_sm_resp received", zap.Uint32("sequence", pdu.SequenceNumber))

		case smpp.CmdUnbind:
			resp := &smpp.PDU{
				CommandID:      smpp.CmdUnbindResp,
				CommandStatus:  smpp.StatusOK,
				SequenceNumber: pdu.SequenceNumber,
			}
			s.writePDU(conn, resp)
			s.logger.Info("client unbound")
			return

		default:
			s.logger.Warn("unhandled PDU command from client",
				zap.Uint32("command_id", pdu.CommandID),
				zap.Uint32("sequence", pdu.SequenceNumber),
			)
			// Send generic_nack.
			resp := &smpp.PDU{
				CommandID:      smpp.CmdGenericNack,
				CommandStatus:  smpp.StatusInvCmdID,
				SequenceNumber: pdu.SequenceNumber,
			}
			s.writePDU(conn, resp)
		}
	}
}

// sendDLR sends a simulated delivery report to the client as a deliver_sm PDU.
func (s *Server) sendDLR(conn net.Conn, messageID string, destAddr string, success bool) {
	status := "DELIVRD"
	errCode := "000"
	dlvrd := "001"
	if !success {
		status = "UNDELIV"
		errCode = "069"
		dlvrd = "000"
	}

	s.store.MarkDLRSent(messageID, status)

	now := time.Now()
	dateStr := now.Format("0601021504") // YYMMDDhhmm

	// Build the DLR receipt text.
	receiptText := fmt.Sprintf(
		"id:%s sub:001 dlvrd:%s submit date:%s done date:%s stat:%s err:%s text:...",
		messageID, dlvrd, dateStr, dateStr, status, errCode,
	)

	// Build deliver_sm body.
	body := s.buildDeliverSMBody("", destAddr, 0x04, []byte(receiptText))

	s.mu.Lock()
	s.seqNum++
	seq := s.seqNum
	s.mu.Unlock()

	pdu := &smpp.PDU{
		CommandID:      smpp.CmdDeliverSM,
		CommandStatus:  smpp.StatusOK,
		SequenceNumber: seq,
		Body:           body,
	}

	if err := s.writePDU(conn, pdu); err != nil {
		s.logger.Error("failed to send DLR deliver_sm",
			zap.String("message_id", messageID),
			zap.Error(err),
		)
		return
	}

	s.logger.Info("DLR sent",
		zap.String("message_id", messageID),
		zap.String("dest", destAddr),
		zap.String("status", status),
	)
}

// sendMO sends a simulated MO/PoR deliver_sm back to the client.
func (s *Server) sendMO(conn net.Conn, messageID string, sourceAddr string, destAddr string, payload []byte) {
	s.store.MarkMOSent(messageID)

	body := s.buildDeliverSMBody(sourceAddr, destAddr, 0x00, payload)

	s.mu.Lock()
	s.seqNum++
	seq := s.seqNum
	s.mu.Unlock()

	pdu := &smpp.PDU{
		CommandID:      smpp.CmdDeliverSM,
		CommandStatus:  smpp.StatusOK,
		SequenceNumber: seq,
		Body:           body,
	}

	if err := s.writePDU(conn, pdu); err != nil {
		s.logger.Error("failed to send MO deliver_sm",
			zap.String("message_id", messageID),
			zap.Error(err),
		)
		return
	}

	s.logger.Info("MO sent",
		zap.String("message_id", messageID),
		zap.String("source", sourceAddr),
		zap.String("dest", destAddr),
		zap.Int("payload_len", len(payload)),
	)
}

// buildDeliverSMBody constructs the body of a deliver_sm PDU.
func (s *Server) buildDeliverSMBody(sourceAddr string, destAddr string, esmClass byte, shortMessage []byte) []byte {
	var buf bytes.Buffer

	// service_type (C-string, empty)
	buf.WriteByte(0x00)

	// source_addr_ton, source_addr_npi
	buf.WriteByte(0x00) // ton
	buf.WriteByte(0x00) // npi

	// source_addr (C-string)
	buf.WriteString(sourceAddr)
	buf.WriteByte(0x00)

	// dest_addr_ton, dest_addr_npi
	buf.WriteByte(0x01) // ton (international)
	buf.WriteByte(0x01) // npi (ISDN)

	// destination_addr (C-string)
	buf.WriteString(destAddr)
	buf.WriteByte(0x00)

	// esm_class
	buf.WriteByte(esmClass)

	// protocol_id
	buf.WriteByte(0x00)

	// priority_flag
	buf.WriteByte(0x00)

	// schedule_delivery_time (C-string, empty)
	buf.WriteByte(0x00)

	// validity_period (C-string, empty)
	buf.WriteByte(0x00)

	// registered_delivery
	buf.WriteByte(0x00)

	// replace_if_present_flag
	buf.WriteByte(0x00)

	// data_coding
	buf.WriteByte(0x00)

	// sm_default_msg_id
	buf.WriteByte(0x00)

	// sm_length + short_message
	if len(shortMessage) > 254 {
		buf.WriteByte(0x00)
		binary.Write(&buf, binary.BigEndian, uint16(0x0424))
		binary.Write(&buf, binary.BigEndian, uint16(len(shortMessage)))
		buf.Write(shortMessage)
	} else {
		buf.WriteByte(byte(len(shortMessage)))
		buf.Write(shortMessage)
	}

	return buf.Bytes()
}

// writePDU encodes and writes a PDU to the given connection.
func (s *Server) writePDU(conn net.Conn, pdu *smpp.PDU) error {
	data := smpp.EncodePDU(pdu)

	s.mu.Lock()
	connMu, ok := s.clients[conn]
	s.mu.Unlock()
	if !ok {
		return fmt.Errorf("connection not registered for write")
	}

	connMu.Lock()
	defer connMu.Unlock()
	_, err := conn.Write(data)
	return err
}

// readCStringFromBody reads a null-terminated string from the given body starting at offset.
func readCStringFromBody(body []byte, offset int) string {
	if offset >= len(body) {
		return ""
	}
	end := offset
	for end < len(body) && body[end] != 0x00 {
		end++
	}
	return string(body[offset:end])
}

// writeCStringToBytes creates a byte slice containing a null-terminated string.
func writeCStringToBytes(s string) []byte {
	b := make([]byte, len(s)+1)
	copy(b, s)
	b[len(s)] = 0x00
	return b
}
