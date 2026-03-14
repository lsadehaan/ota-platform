// smpp-probe-smsc is a controllable SMPP SMSC for integration testing.
//
// It accepts SMPP bind_transceiver connections and records every submit_sm
// it receives (including raw PDU body bytes). DLR and MO deliver_sm PDUs
// are only emitted when explicitly triggered via the HTTP control API.
//
// SMPP server on :2775 (configurable via PROBE_SMPP_PORT)
// HTTP control API on :8080 (configurable via PROBE_HTTP_PORT)
//
// HTTP endpoints:
//
//	GET  /captures            — list all captured submit_sm records
//	GET  /captures/{id}       — get a specific capture by message ID
//	POST /captures/clear      — clear all captures
//	POST /dlr                 — trigger DLR for a message ID
//	POST /mo                  — trigger MO deliver_sm
//	POST /config              — set submit response behavior
//	GET  /health              — health check
package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/idnteq/go-smsc/smpp"
)

// ── Capture store ──────────────────────────────────────────────────────

// Capture holds a single captured submit_sm.
type Capture struct {
	MessageID  string `json:"message_id"`
	SourceAddr string `json:"source_addr"`
	DestAddr   string `json:"dest_addr"`
	RawBody    []byte `json:"-"`         // raw submit_sm body bytes (binary)
	RawBodyHex string `json:"raw_body"`  // hex-encoded for JSON
	ReceivedAt string `json:"received_at"`
}

type CaptureStore struct {
	mu       sync.RWMutex
	captures map[string]*Capture // messageID → Capture
	order    []string            // insertion order
}

func NewCaptureStore() *CaptureStore {
	return &CaptureStore{
		captures: make(map[string]*Capture),
	}
}

func (cs *CaptureStore) Add(c *Capture) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.captures[c.MessageID] = c
	cs.order = append(cs.order, c.MessageID)
}

func (cs *CaptureStore) Get(id string) (*Capture, bool) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	c, ok := cs.captures[id]
	return c, ok
}

func (cs *CaptureStore) All() []*Capture {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	result := make([]*Capture, 0, len(cs.order))
	for _, id := range cs.order {
		if c, ok := cs.captures[id]; ok {
			result = append(result, c)
		}
	}
	return result
}

func (cs *CaptureStore) Clear() {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.captures = make(map[string]*Capture)
	cs.order = nil
}

// ── Submit response config ─────────────────────────────────────────────

// SubmitConfig controls how submit_sm_resp is sent.
type SubmitConfig struct {
	mu       sync.RWMutex
	mode     string // "accept", "reject", "timeout"
	status   uint32 // SMPP status for "reject" mode
	delayMs  int    // delay before sending response
}

func NewSubmitConfig() *SubmitConfig {
	return &SubmitConfig{mode: "accept"}
}

func (sc *SubmitConfig) Get() (string, uint32, int) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.mode, sc.status, sc.delayMs
}

func (sc *SubmitConfig) Set(mode string, status uint32, delayMs int) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.mode = mode
	sc.status = status
	sc.delayMs = delayMs
}

// ── SMPP Server ────────────────────────────────────────────────────────

type ProbeServer struct {
	smppAddr string
	httpAddr string
	store    *CaptureStore
	config   *SubmitConfig

	listener net.Listener
	connsMu  sync.Mutex
	conns    map[net.Conn]*sync.Mutex // per-connection write locks
	seqNum   atomic.Uint32
	done     chan struct{}
}

func NewProbeServer(smppAddr, httpAddr string) *ProbeServer {
	return &ProbeServer{
		smppAddr: smppAddr,
		httpAddr: httpAddr,
		store:    NewCaptureStore(),
		config:   NewSubmitConfig(),
		conns:    make(map[net.Conn]*sync.Mutex),
		done:     make(chan struct{}),
	}
}

func (ps *ProbeServer) Start() error {
	listener, err := net.Listen("tcp", ps.smppAddr)
	if err != nil {
		return fmt.Errorf("smpp listen on %s: %w", ps.smppAddr, err)
	}
	ps.listener = listener
	log.Printf("probe SMSC SMPP listening on %s", ps.smppAddr)

	go ps.acceptLoop()
	return nil
}

func (ps *ProbeServer) Stop() {
	close(ps.done)
	if ps.listener != nil {
		ps.listener.Close()
	}
	ps.connsMu.Lock()
	for conn := range ps.conns {
		conn.Close()
	}
	ps.conns = make(map[net.Conn]*sync.Mutex)
	ps.connsMu.Unlock()
}

func (ps *ProbeServer) acceptLoop() {
	for {
		conn, err := ps.listener.Accept()
		if err != nil {
			select {
			case <-ps.done:
				return
			default:
			}
			log.Printf("accept error: %v", err)
			continue
		}

		log.Printf("SMPP client connected: %s", conn.RemoteAddr())

		ps.connsMu.Lock()
		ps.conns[conn] = &sync.Mutex{}
		ps.connsMu.Unlock()

		go ps.handleConnection(conn)
	}
}

func (ps *ProbeServer) handleConnection(conn net.Conn) {
	defer func() {
		ps.connsMu.Lock()
		delete(ps.conns, conn)
		ps.connsMu.Unlock()
		conn.Close()
		log.Printf("SMPP client disconnected: %s", conn.RemoteAddr())
	}()

	headerBuf := make([]byte, 16)
	for {
		select {
		case <-ps.done:
			return
		default:
		}

		_, err := io.ReadFull(conn, headerBuf)
		if err != nil {
			if err != io.EOF {
				select {
				case <-ps.done:
					return
				default:
				}
				log.Printf("read error: %v", err)
			}
			return
		}

		cmdLen := binary.BigEndian.Uint32(headerBuf[0:4])
		if cmdLen < 16 {
			log.Printf("invalid PDU length: %d", cmdLen)
			return
		}

		fullPDU := make([]byte, cmdLen)
		copy(fullPDU, headerBuf)
		if cmdLen > 16 {
			if _, err := io.ReadFull(conn, fullPDU[16:]); err != nil {
				log.Printf("read body error: %v", err)
				return
			}
		}

		pdu, err := smpp.DecodePDU(fullPDU)
		if err != nil {
			log.Printf("decode error: %v", err)
			return
		}

		switch pdu.CommandID {
		case smpp.CmdBindTransceiver:
			systemID := readCString(pdu.Body, 0)
			log.Printf("bind_transceiver: system_id=%s", systemID)

			respBody := writeCStringBytes("PROBE-SMSC")
			resp := &smpp.PDU{
				CommandID:      smpp.CmdBindTransceiverResp,
				CommandStatus:  smpp.StatusOK,
				SequenceNumber: pdu.SequenceNumber,
				Body:           respBody,
			}
			ps.writePDU(conn, resp)

		case smpp.CmdSubmitSM:
			ps.handleSubmitSM(conn, pdu)

		case smpp.CmdEnquireLink:
			resp := &smpp.PDU{
				CommandID:      smpp.CmdEnquireLinkResp,
				CommandStatus:  smpp.StatusOK,
				SequenceNumber: pdu.SequenceNumber,
			}
			ps.writePDU(conn, resp)

		case smpp.CmdDeliverSMResp:
			log.Printf("deliver_sm_resp: seq=%d status=%d",
				pdu.SequenceNumber, pdu.CommandStatus)

		case smpp.CmdUnbind:
			resp := &smpp.PDU{
				CommandID:      smpp.CmdUnbindResp,
				CommandStatus:  smpp.StatusOK,
				SequenceNumber: pdu.SequenceNumber,
			}
			ps.writePDU(conn, resp)
			return

		default:
			log.Printf("unhandled command: 0x%08x", pdu.CommandID)
			resp := &smpp.PDU{
				CommandID:      smpp.CmdGenericNack,
				CommandStatus:  smpp.StatusInvCmdID,
				SequenceNumber: pdu.SequenceNumber,
			}
			ps.writePDU(conn, resp)
		}
	}
}

func (ps *ProbeServer) handleSubmitSM(conn net.Conn, pdu *smpp.PDU) {
	sourceAddr, destAddr, _, _ := smpp.ParseDeliverSM(pdu.Body)

	seq := ps.seqNum.Add(1)
	messageID := fmt.Sprintf("PROBE-%d", seq)

	// Capture the raw body bytes.
	rawBody := make([]byte, len(pdu.Body))
	copy(rawBody, pdu.Body)

	capture := &Capture{
		MessageID:  messageID,
		SourceAddr: sourceAddr,
		DestAddr:   destAddr,
		RawBody:    rawBody,
		RawBodyHex: hex.EncodeToString(rawBody),
		ReceivedAt: time.Now().UTC().Format(time.RFC3339),
	}
	ps.store.Add(capture)

	log.Printf("submit_sm captured: id=%s src=%s dst=%s body_len=%d",
		messageID, sourceAddr, destAddr, len(rawBody))

	// Check response config.
	mode, status, delayMs := ps.config.Get()

	if delayMs > 0 {
		time.Sleep(time.Duration(delayMs) * time.Millisecond)
	}

	switch mode {
	case "reject":
		if status == 0 {
			status = smpp.StatusSysErr
		}
		resp := &smpp.PDU{
			CommandID:      smpp.CmdSubmitSMResp,
			CommandStatus:  status,
			SequenceNumber: pdu.SequenceNumber,
			Body:           []byte{0x00},
		}
		ps.writePDU(conn, resp)

	case "timeout":
		// Do not send any response — simulate SMSC timeout.
		log.Printf("submit_sm timeout mode: no response for %s", messageID)

	default: // "accept"
		respBody := writeCStringBytes(messageID)
		resp := &smpp.PDU{
			CommandID:      smpp.CmdSubmitSMResp,
			CommandStatus:  smpp.StatusOK,
			SequenceNumber: pdu.SequenceNumber,
			Body:           respBody,
		}
		ps.writePDU(conn, resp)
	}
}

func (ps *ProbeServer) writePDU(conn net.Conn, pdu *smpp.PDU) error {
	data := smpp.EncodePDU(pdu)

	ps.connsMu.Lock()
	connMu, ok := ps.conns[conn]
	ps.connsMu.Unlock()
	if !ok {
		return fmt.Errorf("connection not registered for write")
	}

	connMu.Lock()
	defer connMu.Unlock()
	_, err := conn.Write(data)
	return err
}

// sendDeliverSM sends a deliver_sm to all connected clients.
func (ps *ProbeServer) sendDeliverSM(sourceAddr, destAddr string, esmClass byte, payload []byte) error {
	ps.connsMu.Lock()
	conns := make([]net.Conn, 0, len(ps.conns))
	for conn := range ps.conns {
		conns = append(conns, conn)
	}
	ps.connsMu.Unlock()

	if len(conns) == 0 {
		return fmt.Errorf("no SMPP client connected")
	}

	// Send to the first connection (for DLR/MO, the gateway routes by affinity).
	conn := conns[0]

	body := buildDeliverSMBody(sourceAddr, destAddr, esmClass, payload)
	seq := ps.seqNum.Add(1)

	pdu := &smpp.PDU{
		CommandID:      smpp.CmdDeliverSM,
		CommandStatus:  smpp.StatusOK,
		SequenceNumber: seq,
		Body:           body,
	}

	return ps.writePDU(conn, pdu)
}

// ── HTTP Control API ───────────────────────────────────────────────────

func (ps *ProbeServer) StartHTTP() error {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})

	mux.HandleFunc("GET /captures", func(w http.ResponseWriter, r *http.Request) {
		captures := ps.store.All()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(captures)
	})

	mux.HandleFunc("GET /captures/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		c, ok := ps.store.Get(id)
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(c)
	})

	mux.HandleFunc("POST /captures/clear", func(w http.ResponseWriter, r *http.Request) {
		ps.store.Clear()
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"cleared":true}`))
	})

	// POST /dlr — trigger DLR for a message ID.
	// Body: {"message_id": "PROBE-1", "status": "DELIVRD"}
	// Optional: "status" defaults to "DELIVRD". Values: DELIVRD, UNDELIV, EXPIRED, REJECTD.
	mux.HandleFunc("POST /dlr", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			MessageID string `json:"message_id"`
			Status    string `json:"status"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if req.MessageID == "" {
			http.Error(w, "message_id required", http.StatusBadRequest)
			return
		}
		if req.Status == "" {
			req.Status = "DELIVRD"
		}

		c, ok := ps.store.Get(req.MessageID)
		if !ok {
			http.Error(w, "message_id not found in captures", http.StatusNotFound)
			return
		}

		dlvrd := "001"
		errCode := "000"
		if req.Status != "DELIVRD" {
			dlvrd = "000"
			errCode = "069"
		}

		now := time.Now().Format("0601021504")
		receiptText := fmt.Sprintf(
			"id:%s sub:001 dlvrd:%s submit date:%s done date:%s stat:%s err:%s text:...",
			req.MessageID, dlvrd, now, now, req.Status, errCode,
		)

		if err := ps.sendDeliverSM("", c.DestAddr, 0x04, []byte(receiptText)); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"sent": "dlr", "message_id": req.MessageID, "status": req.Status,
		})
	})

	// POST /mo — trigger MO deliver_sm.
	// Body: {"source_addr": "+27831234567", "dest_addr": "GATEWAY", "payload": "hex-encoded-bytes"}
	// Or: {"source_addr": "+27831234567", "dest_addr": "GATEWAY", "text": "plain text"}
	mux.HandleFunc("POST /mo", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			SourceAddr string `json:"source_addr"`
			DestAddr   string `json:"dest_addr"`
			Payload    string `json:"payload"` // hex-encoded
			Text       string `json:"text"`    // plain text alternative
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if req.SourceAddr == "" {
			http.Error(w, "source_addr required", http.StatusBadRequest)
			return
		}
		if req.DestAddr == "" {
			req.DestAddr = "GATEWAY"
		}

		var payload []byte
		if req.Payload != "" {
			var err error
			payload, err = hex.DecodeString(req.Payload)
			if err != nil {
				http.Error(w, "invalid hex payload: "+err.Error(), http.StatusBadRequest)
				return
			}
		} else if req.Text != "" {
			payload = []byte(req.Text)
		} else {
			http.Error(w, "payload or text required", http.StatusBadRequest)
			return
		}

		// MO: esm_class = 0x00 (not a DLR)
		if err := ps.sendDeliverSM(req.SourceAddr, req.DestAddr, 0x00, payload); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"sent": "mo"})
	})

	// POST /config — set submit response behavior.
	// Body: {"mode": "accept"} | {"mode": "reject", "status": 8} | {"mode": "timeout"}
	// Optional: "delay_ms" (delay before response in any mode)
	mux.HandleFunc("POST /config", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Mode    string `json:"mode"`
			Status  uint32 `json:"status"`
			DelayMs int    `json:"delay_ms"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if req.Mode == "" {
			req.Mode = "accept"
		}
		if req.Mode != "accept" && req.Mode != "reject" && req.Mode != "timeout" {
			http.Error(w, "mode must be accept, reject, or timeout", http.StatusBadRequest)
			return
		}

		ps.config.Set(req.Mode, req.Status, req.DelayMs)
		log.Printf("config updated: mode=%s status=%d delay_ms=%d", req.Mode, req.Status, req.DelayMs)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"mode": req.Mode, "status": req.Status, "delay_ms": req.DelayMs,
		})
	})

	server := &http.Server{Addr: ps.httpAddr, Handler: mux}
	log.Printf("probe SMSC HTTP API listening on %s", ps.httpAddr)
	go func() {
		<-ps.done
		server.Close()
	}()
	return server.ListenAndServe()
}

// ── Helpers ────────────────────────────────────────────────────────────

func readCString(body []byte, offset int) string {
	if offset >= len(body) {
		return ""
	}
	end := offset
	for end < len(body) && body[end] != 0x00 {
		end++
	}
	return string(body[offset:end])
}

func writeCStringBytes(s string) []byte {
	b := make([]byte, len(s)+1)
	copy(b, s)
	return b
}

func buildDeliverSMBody(sourceAddr, destAddr string, esmClass byte, shortMessage []byte) []byte {
	var buf bytes.Buffer

	buf.WriteByte(0x00) // service_type
	buf.WriteByte(0x00) // source_addr_ton
	buf.WriteByte(0x00) // source_addr_npi
	buf.WriteString(sourceAddr)
	buf.WriteByte(0x00) // null terminator
	buf.WriteByte(0x01) // dest_addr_ton
	buf.WriteByte(0x01) // dest_addr_npi
	buf.WriteString(destAddr)
	buf.WriteByte(0x00) // null terminator
	buf.WriteByte(esmClass)
	buf.WriteByte(0x00) // protocol_id
	buf.WriteByte(0x00) // priority_flag
	buf.WriteByte(0x00) // schedule_delivery_time
	buf.WriteByte(0x00) // validity_period
	buf.WriteByte(0x00) // registered_delivery
	buf.WriteByte(0x00) // replace_if_present_flag
	buf.WriteByte(0x00) // data_coding
	buf.WriteByte(0x00) // sm_default_msg_id

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

// ── Main ───────────────────────────────────────────────────────────────

func main() {
	smppPort := envOrDefault("PROBE_SMPP_PORT", "2775")
	httpPort := envOrDefault("PROBE_HTTP_PORT", "8080")

	ps := NewProbeServer(":"+smppPort, ":"+httpPort)

	if err := ps.Start(); err != nil {
		log.Fatalf("failed to start SMPP server: %v", err)
	}

	// Handle shutdown.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("shutting down...")
		ps.Stop()
	}()

	// HTTP API runs in foreground (blocks).
	if err := ps.StartHTTP(); err != nil && !strings.Contains(err.Error(), "closed") {
		log.Fatalf("HTTP server error: %v", err)
	}
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// Silence unused import warning for strconv.
var _ = strconv.Itoa
