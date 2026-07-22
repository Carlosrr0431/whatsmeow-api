package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
)

type AgentRegistryEntry struct {
	AgentCode     string `json:"agent_code"`
	WebhookURL    string `json:"webhook_url"`
	WebhookSecret string `json:"webhook_secret,omitempty"`
	UpdatedAt     string `json:"updated_at,omitempty"`
}

type SessionRegistry struct {
	Agents map[string]*AgentRegistryEntry `json:"agents"`
}

type SessionStatusInfo struct {
	AgentCode   string `json:"agent_code"`
	Status      string `json:"status"`
	Phone       string `json:"phone"`
	HasSession  bool   `json:"has_session"`
	WebhookURL  string `json:"webhook_url,omitempty"`
	Connected   bool   `json:"connected"`
}

type AgentSession struct {
	AgentCode     string
	WebhookURL    string
	WebhookSecret string

	container *sqlstore.Container
	client    *whatsmeow.Client
	qrCode    string
	connected bool
	messages  []MessageEvent
	rawMedia  map[string]*waE2E.Message
	rawOrder  []string
	// pollOptions: message_id del PollCreation → textos de opciones (para DecryptPollVote)
	pollOptions        map[string][]string
	pollOrder          []string
	undecryptableSeen  map[string]struct{}
	undecryptableOrder []string
	statusNotifyMu     sync.Mutex
	disconnectTimer    *time.Timer
	disconnectNotified bool
	mu                 sync.RWMutex

	manager    *SessionManager
	httpClient *http.Client
	sessionDir string
}

type SessionManager struct {
	dataDir       string
	registryPath  string
	defaultSecret string
	registry      *SessionRegistry
	sessions      map[string]*AgentSession
	draining      bool
	mu            sync.RWMutex
	httpClient    *http.Client
}

func NewSessionManager(dataDir, defaultSecret string) (*SessionManager, error) {
	initRuntimeConfig()
	fmt.Printf("[CONFIG] max_msg_history=%d max_raw_media=%d skip_groups=%v client_log=%s sqlite_busy_ms=%d connect_stagger_sec=%d connect_delay_sec=%d shutdown_wait_sec=%d status_debounce_sec=%d\n",
		maxMsgHistLimit, maxRawMediaLimit, skipGroupsAndBroadcast, clientLogLevel, sqliteBusyTimeoutMS, autoConnectStaggerSec, autoConnectDelaySec, shutdownWaitSec, sessionStatusDebounceSec)

	if dataDir == "" {
		dataDir = "/app/data"
	}
	sessionsDir := filepath.Join(dataDir, "sessions")
	if err := os.MkdirAll(sessionsDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir sessions: %w", err)
	}

	sm := &SessionManager{
		dataDir:       dataDir,
		registryPath:  filepath.Join(sessionsDir, "registry.json"),
		defaultSecret: defaultSecret,
		registry:      &SessionRegistry{Agents: map[string]*AgentRegistryEntry{}},
		sessions:      map[string]*AgentSession{},
		httpClient:    &http.Client{Timeout: 15 * time.Second},
	}

	if err := sm.loadRegistry(); err != nil {
		fmt.Printf("[SESSIONS] Warning loading registry: %v\n", err)
	}
	sm.migrateLegacySession()

	for code, entry := range sm.registry.Agents {
		if _, err := sm.initSession(code, entry.WebhookURL, entry.WebhookSecret); err != nil {
			fmt.Printf("[SESSIONS] Init %s: %v\n", code, err)
		}
	}

	return sm, nil
}

func safeAgentDir(agentCode string) string {
	var b strings.Builder
	for _, r := range agentCode {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-' {
			b.WriteRune(r)
		} else {
			b.WriteRune('_')
		}
	}
	out := b.String()
	if out == "" {
		return "agent"
	}
	return out
}

func (sm *SessionManager) sessionDBPath(agentCode string) string {
	return filepath.Join(sm.dataDir, "sessions", safeAgentDir(agentCode), "whatsapp.db")
}

func (sm *SessionManager) loadRegistry() error {
	data, err := os.ReadFile(sm.registryPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	return json.Unmarshal(data, sm.registry)
}

func (sm *SessionManager) saveRegistry() error {
	if sm.registry.Agents == nil {
		sm.registry.Agents = map[string]*AgentRegistryEntry{}
	}
	data, err := json.MarshalIndent(sm.registry, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(sm.registryPath, data, 0o644)
}

func (sm *SessionManager) migrateLegacySession() {
	if len(sm.registry.Agents) > 0 {
		return
	}

	legacyDB := filepath.Join(sm.dataDir, "whatsapp.db")
	if _, err := os.Stat(legacyDB); os.IsNotExist(err) {
		// Also try file: prefix path residue
		legacyDB = filepath.Join(sm.dataDir, "whatsapp.db")
	}

	agentCode := os.Getenv("AGENT_CODE")
	webhookURL := os.Getenv("WEBHOOK_URL")
	if agentCode == "" {
		return
	}

	// Move legacy DB into per-agent folder if it exists
	targetDir := filepath.Dir(sm.sessionDBPath(agentCode))
	_ = os.MkdirAll(targetDir, 0o755)
	targetDB := sm.sessionDBPath(agentCode)

	for _, candidate := range []string{legacyDB, "/app/data/whatsapp.db"} {
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			if _, err := os.Stat(targetDB); os.IsNotExist(err) {
				if err := os.Rename(candidate, targetDB); err == nil {
					fmt.Printf("[SESSIONS] Migrated legacy DB → %s\n", targetDB)
				}
			}
			break
		}
	}

	secret := os.Getenv("WEBHOOK_SECRET")
	if secret == "" {
		secret = sm.defaultSecret
	}
	sm.registry.Agents[agentCode] = &AgentRegistryEntry{
		AgentCode:     agentCode,
		WebhookURL:    webhookURL,
		WebhookSecret: secret,
		UpdatedAt:     time.Now().UTC().Format(time.RFC3339),
	}
	_ = sm.saveRegistry()
	fmt.Printf("[SESSIONS] Migrated legacy config for agent %s\n", agentCode)
}

func (sm *SessionManager) sqliteDSN(agentCode string) string {
	path := sm.sessionDBPath(agentCode)
	return fmt.Sprintf(
		"file:%s?_foreign_keys=on&_journal_mode=WAL&_busy_timeout=%d&_synchronous=NORMAL&cache_size=-2000",
		path, sqliteBusyTimeoutMS,
	)
}

func newClientLogger(agentCode string) waLog.Logger {
	return newFilteredClientLogger(agentCode)
}

func configureWhatsAppClient(client *whatsmeow.Client) {
	client.EnableAutoReconnect = true
	client.InitialAutoReconnect = true
}

func (sm *SessionManager) initSession(agentCode, webhookURL, webhookSecret string) (*AgentSession, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if s, ok := sm.sessions[agentCode]; ok {
		return s, nil
	}

	dbDir := filepath.Dir(sm.sessionDBPath(agentCode))
	if err := os.MkdirAll(dbDir, 0o755); err != nil {
		return nil, err
	}

	dbPath := sm.sqliteDSN(agentCode)
	dbLog := waLog.Stdout("Database-"+safeAgentDir(agentCode), "ERROR", true)
	container, err := sqlstore.New(context.Background(), "sqlite3", dbPath, dbLog)
	if err != nil {
		return nil, err
	}

	if webhookSecret == "" {
		webhookSecret = sm.defaultSecret
	}

	s := &AgentSession{
		AgentCode:          agentCode,
		WebhookURL:         webhookURL,
		WebhookSecret:      webhookSecret,
		container:          container,
		messages:           make([]MessageEvent, 0),
		rawMedia:           make(map[string]*waE2E.Message),
		rawOrder:           make([]string, 0),
		pollOptions:        make(map[string][]string),
		pollOrder:          make([]string, 0),
		undecryptableSeen:  make(map[string]struct{}),
		undecryptableOrder: make([]string, 0),
		manager:            sm,
		httpClient:         sm.httpClient,
		sessionDir:         dbDir,
	}
	s.loadPollOptionsFromDisk()
	sm.sessions[agentCode] = s
	return s, nil
}

func (sm *SessionManager) EnsureSession(agentCode, webhookURL, webhookSecret string) (*AgentSession, error) {
	if agentCode == "" {
		return nil, fmt.Errorf("agent_code is required")
	}

	sm.mu.Lock()
	entry, exists := sm.registry.Agents[agentCode]
	if !exists {
		entry = &AgentRegistryEntry{AgentCode: agentCode}
		sm.registry.Agents[agentCode] = entry
	}
	if webhookURL != "" {
		entry.WebhookURL = webhookURL
	}
	if webhookSecret != "" {
		entry.WebhookSecret = webhookSecret
	}
	entry.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	sm.mu.Unlock()

	if err := sm.saveRegistry(); err != nil {
		fmt.Printf("[SESSIONS] saveRegistry error: %v\n", err)
	}

	return sm.initSession(agentCode, entry.WebhookURL, entry.WebhookSecret)
}

func (sm *SessionManager) GetSession(agentCode string) (*AgentSession, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	s, ok := sm.sessions[agentCode]
	return s, ok
}

func (sm *SessionManager) UpdateWebhook(agentCode, webhookURL, webhookSecret string) error {
	s, err := sm.EnsureSession(agentCode, webhookURL, webhookSecret)
	if err != nil {
		return err
	}
	s.mu.Lock()
	if webhookURL != "" {
		s.WebhookURL = webhookURL
	}
	if webhookSecret != "" {
		s.WebhookSecret = webhookSecret
	} else if s.WebhookSecret == "" {
		s.WebhookSecret = sm.defaultSecret
	}
	s.mu.Unlock()
	return nil
}

func (sm *SessionManager) ListRegistry() map[string]*AgentRegistryEntry {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	out := make(map[string]*AgentRegistryEntry, len(sm.registry.Agents))
	for k, v := range sm.registry.Agents {
		copy := *v
		out[k] = &copy
	}
	return out
}

func (sm *SessionManager) ListStatus() []SessionStatusInfo {
	sm.mu.RLock()
	codes := make([]string, 0, len(sm.registry.Agents))
	for code := range sm.registry.Agents {
		codes = append(codes, code)
	}
	sm.mu.RUnlock()

	result := make([]SessionStatusInfo, 0, len(codes))
	for _, code := range codes {
		if s, ok := sm.GetSession(code); ok {
			result = append(result, s.StatusInfo())
		}
	}
	return result
}

func (sm *SessionManager) IsDraining() bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.draining
}

// DisconnectAll cierra los websockets de WhatsApp antes de apagar el proceso (SIGTERM en Railway).
// Evita que el contenedor viejo y el nuevo compitan por la misma sesión.
func (sm *SessionManager) DisconnectAll() {
	sm.mu.Lock()
	sm.draining = true
	sessions := make([]*AgentSession, 0, len(sm.sessions))
	for _, s := range sm.sessions {
		sessions = append(sessions, s)
	}
	sm.mu.Unlock()

	fmt.Println("[SESSIONS] Graceful shutdown: disconnecting WhatsApp clients...")
	for _, s := range sessions {
		s.mu.RLock()
		connected := s.connected && s.client != nil
		code := s.AgentCode
		s.mu.RUnlock()
		if !connected {
			continue
		}
		fmt.Printf("[SESSIONS] Disconnecting %s...\n", code)
		s.Disconnect()
	}
	fmt.Println("[SESSIONS] All WhatsApp clients disconnected")
}

func (sm *SessionManager) AutoConnectAll() {
	sm.mu.RLock()
	codes := make([]string, 0, len(sm.registry.Agents))
	for code := range sm.registry.Agents {
		codes = append(codes, code)
	}
	sm.mu.RUnlock()

	for i, code := range codes {
		if i > 0 && autoConnectStaggerSec > 0 {
			time.Sleep(time.Duration(autoConnectStaggerSec) * time.Second)
		}
		s, ok := sm.GetSession(code)
		if !ok {
			continue
		}
		deviceStore, err := s.container.GetFirstDevice(context.Background())
		if err != nil || deviceStore.ID == nil {
			continue
		}
		fmt.Printf("[SESSIONS] Auto-connecting %s...\n", code)
		go func(session *AgentSession) {
			// Reintentos: tras redeploy el WS a veces falla al primer intento
			var lastErr error
			for attempt := 1; attempt <= 4; attempt++ {
				if sm.IsDraining() {
					return
				}
				lastErr = session.connectExisting()
				if lastErr == nil {
					return
				}
				fmt.Printf("[SESSIONS] Auto-connect %s attempt %d/4 failed: %v\n", session.AgentCode, attempt, lastErr)
				time.Sleep(time.Duration(attempt*3) * time.Second)
			}
			fmt.Printf("[SESSIONS] Auto-connect %s gave up: %v\n", session.AgentCode, lastErr)
		}(s)
	}
}

// ReconnectDisconnected vuelve a conectar sesiones con device que no estén connected.
// No toca pairing/QR ni hace logout.
func (sm *SessionManager) ReconnectDisconnected() {
	if sm.IsDraining() {
		return
	}
	sm.mu.RLock()
	codes := make([]string, 0, len(sm.registry.Agents))
	for code := range sm.registry.Agents {
		codes = append(codes, code)
	}
	sm.mu.RUnlock()

	for _, code := range codes {
		s, ok := sm.GetSession(code)
		if !ok {
			continue
		}
		s.mu.RLock()
		already := s.connected && s.client != nil
		s.mu.RUnlock()
		if already {
			continue
		}
		deviceStore, err := s.container.GetFirstDevice(context.Background())
		if err != nil || deviceStore.ID == nil {
			continue
		}
		fmt.Printf("[SESSIONS] ReconnectDisconnected %s...\n", code)
		go func(session *AgentSession) {
			if err := session.connectExisting(); err != nil {
				fmt.Printf("[SESSIONS] ReconnectDisconnected %s failed: %v\n", session.AgentCode, err)
			}
		}(s)
	}
}

func (s *AgentSession) isLoggedInLocked() bool {
	return s.client != nil && s.client.Store != nil && s.client.Store.ID != nil
}

func (s *AgentSession) StatusInfo() SessionStatusInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	info := SessionStatusInfo{
		AgentCode:  s.AgentCode,
		WebhookURL: s.WebhookURL,
		Status:     "disconnected",
	}

	loggedIn := s.isLoggedInLocked()
	if loggedIn {
		info.HasSession = true
		info.Phone = s.client.Store.ID.User
	}

	// connected=true sin Store.ID = websocket esperando QR (NO es sesión usable)
	if loggedIn && s.connected {
		info.Connected = true
		info.Status = "connected"
	} else if s.client != nil && s.qrCode != "" {
		info.Status = "need_scan"
	} else if loggedIn {
		info.Status = "connecting"
	} else if s.client != nil {
		info.Status = "need_scan"
	} else {
		deviceStore, err := s.container.GetFirstDevice(context.Background())
		if err == nil && deviceStore.ID != nil {
			info.HasSession = true
			info.Phone = deviceStore.ID.User
			info.Status = "disconnected"
		} else {
			info.Status = "logged_out"
		}
	}
	return info
}

func (s *AgentSession) dispatchWebhook(event string, data interface{}) {
	s.mu.RLock()
	url := s.WebhookURL
	secret := s.WebhookSecret
	agentCode := s.AgentCode
	client := s.httpClient
	s.mu.RUnlock()

	if url == "" {
		fmt.Printf("[WEBHOOK] %s: sin webhook_url configurada\n", agentCode)
		return
	}

	go func() {
		payload, err := json.Marshal(map[string]interface{}{
			"event":      event,
			"data":       data,
			"agent_code": agentCode,
		})
		if err != nil {
			return
		}

		req, err := http.NewRequest("POST", url, bytes.NewReader(payload))
		if err != nil {
			return
		}
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
		if secret != "" {
			req.Header.Set("X-Webhook-Secret", secret)
		}

		resp, err := client.Do(req)
		if err != nil {
			fmt.Printf("[WEBHOOK] %s %s → error: %v\n", agentCode, event, err)
			return
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			fmt.Printf("[WEBHOOK] %s %s → %s: HTTP %d %s\n", agentCode, event, url, resp.StatusCode, strings.TrimSpace(string(body)))
			return
		}
		if event == "messages.upsert" {
			fmt.Printf("[WEBHOOK] %s messages.upsert → OK\n", agentCode)
		} else if event == "messages.button" || event == "messages.list" {
			if msg, ok := data.(MessageEvent); ok {
				fmt.Printf("[WEBHOOK] %s %s id=%s button_id=%s → OK\n", agentCode, event, msg.ID, msg.ButtonID)
			} else {
				fmt.Printf("[WEBHOOK] %s %s → OK\n", agentCode, event)
			}
		} else if event == "messages.status" {
			if dataMap, ok := data.(map[string]interface{}); ok {
				fmt.Printf("[WEBHOOK] %s messages.status id=%v status=%v → OK\n", agentCode, dataMap["id"], dataMap["status"])
			} else {
				fmt.Printf("[WEBHOOK] %s messages.status → OK\n", agentCode)
			}
		} else {
			logVerbose("[WEBHOOK] %s %s → %s: %d\n", agentCode, event, url, resp.StatusCode)
		}
	}()
}

func (s *AgentSession) eventHandler(evt interface{}) {
	switch v := evt.(type) {
	case *events.Message:
		if v.Message == nil {
			return
		}
		if !shouldProcessChat(v.Info) {
			logVerbose("[SKIP][%s] grupo/broadcast chat=%s\n", s.AgentCode, v.Info.Chat.String())
			return
		}
		if v.Message.GetSenderKeyDistributionMessage() != nil {
			return
		}
		if v.Message.GetReactionMessage() != nil {
			s.handleReactionMessage(v.Info, v.Message.GetReactionMessage())
			return
		}
		if v.Message.GetEncReactionMessage() != nil {
			s.handleEncReaction(v)
			return
		}
		if pm := v.Message.GetProtocolMessage(); pm != nil {
			s.handleProtocolMessage(v.Info, pm)
			return
		}
		if v.Message.GetPollUpdateMessage() != nil {
			s.handlePollVote(v)
			return
		}
		msg := s.messageFromEvent(v.Info, v.Message)
		logVerbose("[MSG][%s] from=%s chat=%s pn=%s isFromMe=%v type=%s hasMedia=%v\n",
			s.AgentCode, msg.From, msg.ChatJID, msg.SenderPN, msg.IsFromMe, msg.Type, msg.HasMedia)
		if msg.HasMedia && v.Message != nil {
			s.storeRawMedia(msg.ID, v.Message)
		}
		s.appendMessage(msg)
		// Respuestas a botones/listas: upsert (pipeline CRM) + eventos dedicados
		if msg.Type == "button_reply" {
			fmt.Printf("[BUTTON_REPLY][%s] id=%s button_id=%q body=%q\n", s.AgentCode, msg.ID, msg.ButtonID, msg.Body)
			s.dispatchWebhook("messages.upsert", msg)
			s.dispatchWebhook("messages.button", msg)
		} else if msg.Type == "list_reply" {
			fmt.Printf("[LIST_REPLY][%s] id=%s row_id=%q body=%q\n", s.AgentCode, msg.ID, msg.ButtonID, msg.Body)
			s.dispatchWebhook("messages.upsert", msg)
			s.dispatchWebhook("messages.list", msg)
			// Compat: clientes/configs que solo escuchan messages.button
			s.dispatchWebhook("messages.button", msg)
		} else {
			s.dispatchWebhook("messages.upsert", msg)
		}

	case *events.UndecryptableMessage:
		s.handleUndecryptableMessage(v)

	case *events.Receipt:
		s.handleReceipt(v)

	case *events.Connected:
		s.handleConnected()

	case *events.PairSuccess:
		// Pairing nuevo: Store.ID ya existe. No toca sesiones ajenas.
		s.mu.Lock()
		s.qrCode = ""
		phone := v.ID.User
		if s.isLoggedInLocked() {
			s.connected = true
			phone = s.client.Store.ID.User
		}
		s.mu.Unlock()
		fmt.Printf("✓ [%s] PairSuccess (%s)\n", s.AgentCode, phone)
		payload := map[string]string{"status": "connected"}
		if phone != "" {
			payload["phone"] = phone
		}
		if s.manager == nil || !s.manager.IsDraining() {
			s.dispatchWebhook("session.status", payload)
		}

	case *events.Disconnected:
		s.handleDisconnected()

	case *events.LoggedOut:
		s.cancelDisconnectNotify()
		s.mu.Lock()
		s.connected = false
		s.client = nil
		s.qrCode = ""
		s.mu.Unlock()
		fmt.Printf("✗ [%s] WhatsApp logged out\n", s.AgentCode)
		if s.manager == nil || !s.manager.IsDraining() {
			s.dispatchWebhook("session.status", map[string]string{"status": "logged_out"})
		}
	}
}

func (s *AgentSession) cancelDisconnectNotify() {
	s.statusNotifyMu.Lock()
	defer s.statusNotifyMu.Unlock()
	if s.disconnectTimer != nil {
		s.disconnectTimer.Stop()
		s.disconnectTimer = nil
	}
}

func (s *AgentSession) handleDisconnected() {
	if s.manager != nil && s.manager.IsDraining() {
		s.mu.Lock()
		s.connected = false
		s.mu.Unlock()
		return
	}

	s.mu.Lock()
	s.connected = false
	s.mu.Unlock()

	delay := time.Duration(sessionStatusDebounceSec) * time.Second
	if delay <= 0 {
		fmt.Printf("✗ [%s] WhatsApp disconnected\n", s.AgentCode)
		s.dispatchWebhook("session.status", map[string]string{"status": "disconnected"})
		s.statusNotifyMu.Lock()
		s.disconnectNotified = true
		s.statusNotifyMu.Unlock()
		return
	}

	s.statusNotifyMu.Lock()
	if s.disconnectTimer != nil {
		s.disconnectTimer.Stop()
	}
	code := s.AgentCode
	s.disconnectTimer = time.AfterFunc(delay, func() {
		s.statusNotifyMu.Lock()
		s.disconnectTimer = nil
		s.statusNotifyMu.Unlock()

		s.mu.RLock()
		stillDown := !s.connected
		s.mu.RUnlock()
		if !stillDown {
			return
		}

		fmt.Printf("✗ [%s] WhatsApp disconnected (sin reconexión tras %ds)\n", code, sessionStatusDebounceSec)
		s.statusNotifyMu.Lock()
		s.disconnectNotified = true
		s.statusNotifyMu.Unlock()
		if s.manager == nil || !s.manager.IsDraining() {
			s.dispatchWebhook("session.status", map[string]string{"status": "disconnected"})
		}
	})
	s.statusNotifyMu.Unlock()

	logVerbose("↻ [%s] corte de websocket (reconexión automática en curso)\n", s.AgentCode)
}

func (s *AgentSession) handleConnected() {
	s.statusNotifyMu.Lock()
	notified := s.disconnectNotified
	hadPendingTimer := s.disconnectTimer != nil
	if s.disconnectTimer != nil {
		s.disconnectTimer.Stop()
		s.disconnectTimer = nil
	}
	s.statusNotifyMu.Unlock()

	s.mu.Lock()
	// Connected del websocket puede llegar ANTES del login/QR.
	// Sin Store.ID no hay sesión usable (SendMessage → ErrNotLoggedIn).
	// NO hace Disconnect/Logout: solo evita marcar connected=true a sesiones sin device.
	if !s.isLoggedInLocked() {
		s.connected = false
		s.mu.Unlock()
		fmt.Printf("… [%s] WebSocket up, esperando login/QR\n", s.AgentCode)
		return
	}
	s.connected = true
	s.qrCode = ""
	phone := s.client.Store.ID.User
	if s.client != nil {
		s.client.AutoReconnectErrors = 0
	}
	s.mu.Unlock()

	if notified {
		fmt.Printf("✓ [%s] WhatsApp connected (%s)\n", s.AgentCode, phone)
		s.statusNotifyMu.Lock()
		s.disconnectNotified = false
		s.statusNotifyMu.Unlock()
		if s.manager == nil || !s.manager.IsDraining() {
			s.dispatchWebhook("session.status", map[string]string{"status": "connected", "phone": phone})
		}
		return
	}

	if hadPendingTimer {
		logVerbose("✓ [%s] reconectado tras corte breve de red\n", s.AgentCode)
		return
	}

	fmt.Printf("✓ [%s] WhatsApp connected (%s)\n", s.AgentCode, phone)
	if s.manager == nil || !s.manager.IsDraining() {
		s.dispatchWebhook("session.status", map[string]string{"status": "connected", "phone": phone})
	}
}

func (s *AgentSession) handleReceipt(receipt *events.Receipt) {
	if skipGroupsAndBroadcast && (receipt.IsGroup || !shouldProcessChatJID(receipt.Chat.String())) {
		return
	}
	status := receiptTypeToStatus(receipt.Type)
	if status < 0 {
		return
	}
	chatJID := receipt.Chat.String()
	for _, msgID := range receipt.MessageIDs {
		if msgID == "" {
			continue
		}
		logVerbose("[RECEIPT][%s] id=%s chat=%s type=%s status=%d\n", s.AgentCode, msgID, chatJID, receipt.Type, status)
		s.dispatchWebhook("messages.status", map[string]interface{}{
			"id":           msgID,
			"message_id":   msgID,
			"status":       status,
			"chat_jid":     chatJID,
			"timestamp":    receipt.Timestamp.Unix(),
			"receipt_type": string(receipt.Type),
		})
	}
}

func (s *AgentSession) appendMessage(msg MessageEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, existing := range s.messages {
		if existing.ID == msg.ID {
			return
		}
	}
	s.messages = append(s.messages, msg)
	if maxMsgHistLimit > 0 && len(s.messages) > maxMsgHistLimit {
		s.messages = s.messages[len(s.messages)-maxMsgHistLimit:]
	}
}

func (s *AgentSession) connectExisting() error {
	s.mu.Lock()
	if s.connected && s.client != nil {
		s.mu.Unlock()
		return nil
	}

	clientLog := newClientLogger(s.AgentCode)
	deviceStore, err := s.container.GetFirstDevice(context.Background())
	if err != nil {
		s.mu.Unlock()
		return err
	}

	client := whatsmeow.NewClient(deviceStore, clientLog)
	configureWhatsAppClient(client)
	client.AddEventHandler(s.eventHandler)
	s.client = client
	s.mu.Unlock()

	return client.Connect()
}

func (s *AgentSession) Connect() (needsQR bool, err error) {
	s.mu.Lock()
	if s.connected && s.client != nil {
		s.mu.Unlock()
		return false, nil
	}

	deviceStore, err := s.container.GetFirstDevice(context.Background())
	if err != nil {
		s.mu.Unlock()
		return false, err
	}

	clientLog := newClientLogger(s.AgentCode)
	client := whatsmeow.NewClient(deviceStore, clientLog)
	configureWhatsAppClient(client)
	client.AddEventHandler(s.eventHandler)
	s.client = client

	if client.Store.ID == nil {
		qrChan, _ := client.GetQRChannel(context.Background())
		err = client.Connect()
		if err != nil {
			s.mu.Unlock()
			return false, err
		}

		go func() {
			for evt := range qrChan {
				qrPayload := ""
				s.mu.Lock()
				if evt.Event == "code" {
					s.qrCode = evt.Code
					qrPayload = evt.Code
				} else {
					s.qrCode = ""
				}
				s.mu.Unlock()
				// Notifica al CRM vía webhook (sin polling de /api/session/qr)
				if qrPayload != "" {
					s.dispatchWebhook("session.status", map[string]string{
						"status":  "need_scan",
						"qr_code": qrPayload,
					})
				}
			}
		}()
		s.mu.Unlock()
		return true, nil
	}

	err = client.Connect()
	s.mu.Unlock()
	return false, err
}

func (s *AgentSession) GetQR() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.qrCode
}

func (s *AgentSession) Disconnect() {
	s.cancelDisconnectNotify()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.client != nil {
		s.client.Disconnect()
	}
	s.connected = false
}

func (s *AgentSession) Logout(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.client == nil {
		return fmt.Errorf("not connected")
	}
	err := s.client.Logout(ctx)
	s.client = nil
	s.connected = false
	return err
}

func (s *AgentSession) Client() (*whatsmeow.Client, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.connected || !s.isLoggedInLocked() {
		return nil, false
	}
	return s.client, true
}

func (s *AgentSession) Messages() []MessageEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]MessageEvent, len(s.messages))
	copy(out, s.messages)
	return out
}

func (s *AgentSession) storeSentMessage(id, toJID, body, msgType string, timestamp int64) {
	myJID := ""
	s.mu.RLock()
	if s.client != nil && s.client.Store.ID != nil {
		myJID = s.client.Store.ID.String()
	}
	s.mu.RUnlock()

	msg := MessageEvent{
		ID:        id,
		From:      myJID,
		To:        toJID,
		ChatJID:   toJID,
		Body:      body,
		Type:      msgType,
		Timestamp: timestamp,
		IsGroup:   strings.Contains(toJID, "@g.us"),
		IsFromMe:  true,
		PushName:  "Yo",
	}
	s.appendMessage(msg)
}

const maxPollOptionsStored = 200

func (s *AgentSession) pollOptionsPath() string {
	if s.sessionDir == "" {
		return ""
	}
	return filepath.Join(s.sessionDir, "poll_options.json")
}

func (s *AgentSession) loadPollOptionsFromDisk() {
	path := s.pollOptionsPath()
	if path == "" {
		return
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	var stored map[string][]string
	if err := json.Unmarshal(data, &stored); err != nil || len(stored) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, opts := range stored {
		if id == "" || len(opts) == 0 {
			continue
		}
		s.pollOptions[id] = opts
		s.pollOrder = append(s.pollOrder, id)
	}
	fmt.Printf("[POLL][%s] loaded %d poll option maps from disk\n", s.AgentCode, len(s.pollOptions))
}

func (s *AgentSession) persistPollOptionsLocked() {
	path := s.pollOptionsPath()
	if path == "" {
		return
	}
	data, err := json.Marshal(s.pollOptions)
	if err != nil {
		return
	}
	_ = os.WriteFile(path, data, 0o644)
}

func (s *AgentSession) storePollOptions(messageID string, options []string) {
	if messageID == "" || len(options) == 0 {
		return
	}
	copied := make([]string, len(options))
	copy(copied, options)

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.pollOptions[messageID]; !exists {
		s.pollOrder = append(s.pollOrder, messageID)
	}
	s.pollOptions[messageID] = copied
	for len(s.pollOrder) > maxPollOptionsStored {
		oldest := s.pollOrder[0]
		s.pollOrder = s.pollOrder[1:]
		delete(s.pollOptions, oldest)
	}
	s.persistPollOptionsLocked()
}

func (s *AgentSession) getPollOptions(messageID string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	opts, ok := s.pollOptions[messageID]
	if !ok || len(opts) == 0 {
		return nil
	}
	out := make([]string, len(opts))
	copy(out, opts)
	return out
}

// handlePollVote desencripta el voto y lo reenvía al CRM como button_reply (opt_N / body=N).
func (s *AgentSession) handlePollVote(evt *events.Message) {
	if evt == nil || evt.Message == nil {
		return
	}
	pollUpdate := evt.Message.GetPollUpdateMessage()
	if pollUpdate == nil {
		return
	}

	s.mu.RLock()
	client := s.client
	s.mu.RUnlock()
	if client == nil {
		fmt.Printf("[POLL_VOTE][%s] no client\n", s.AgentCode)
		return
	}

	vote, err := client.DecryptPollVote(context.Background(), evt)
	if err != nil {
		fmt.Printf("[POLL_VOTE][%s] decrypt failed: %v\n", s.AgentCode, err)
		return
	}

	pollKey := pollUpdate.GetPollCreationMessageKey()
	pollID := ""
	if pollKey != nil {
		pollID = pollKey.GetID()
	}
	options := s.getPollOptions(pollID)
	selectedIdx := -1
	selectedText := ""

	if len(options) > 0 {
		hashes := whatsmeow.HashPollOptions(options)
		for _, selected := range vote.GetSelectedOptions() {
			for i, h := range hashes {
				if bytes.Equal(selected, h) {
					selectedIdx = i
					selectedText = options[i]
					break
				}
			}
			if selectedIdx >= 0 {
				break
			}
		}
	}

	msg := s.messageFromEvent(evt.Info, evt.Message)
	msg.Type = "button_reply"
	msg.PollID = pollID
	if selectedIdx >= 0 {
		num := selectedIdx + 1
		msg.ButtonID = fmt.Sprintf("opt_%d", num)
		msg.Body = strconv.Itoa(num)
		msg.PollOption = selectedText
	} else if selectedText != "" {
		msg.Body = selectedText
		msg.PollOption = selectedText
	} else {
		// Sin opciones en memoria: no inventamos; logueamos para debug
		fmt.Printf("[POLL_VOTE][%s] id=%s poll=%s — sin opciones guardadas (hashes=%d)\n",
			s.AgentCode, msg.ID, pollID, len(vote.GetSelectedOptions()))
		msg.Body = ""
		msg.Type = "poll_vote"
	}

	fmt.Printf("[POLL_VOTE][%s] id=%s poll=%s button_id=%q body=%q option=%q\n",
		s.AgentCode, msg.ID, pollID, msg.ButtonID, msg.Body, selectedText)

	if msg.Body == "" && msg.ButtonID == "" {
		return
	}

	s.appendMessage(msg)
	// Compat CRM: upsert + button (opt_N) + evento dedicado poll
	s.dispatchWebhook("messages.upsert", msg)
	s.dispatchWebhook("messages.button", msg)
	s.dispatchWebhook("messages.poll", msg)
}

func jidPhoneUser(jid types.JID) string {
	if jid.Server == types.DefaultUserServer && jid.User != "" {
		return jid.User
	}
	return ""
}

func stripDeviceFromUser(user string) string {
	if idx := strings.Index(user, ":"); idx > 0 {
		return user[:idx]
	}
	return user
}

func normalizeLIDJID(jid types.JID) types.JID {
	if jid.IsEmpty() || jid.Server != types.HiddenUserServer {
		return jid
	}
	user := stripDeviceFromUser(jid.User)
	if user == jid.User {
		return jid
	}
	return types.NewJID(user, types.HiddenUserServer)
}

func lidUserFromJID(jid types.JID) string {
	if jid.Server != types.HiddenUserServer {
		return ""
	}
	return stripDeviceFromUser(jid.User)
}

func (s *AgentSession) resolvePNFromLID(jid types.JID) (types.JID, string) {
	jid = normalizeLIDJID(jid)
	if jid.IsEmpty() || jid.Server != types.HiddenUserServer {
		return jid, ""
	}
	s.mu.RLock()
	client := s.client
	s.mu.RUnlock()
	if client == nil {
		return jid, ""
	}
	pn, err := client.Store.LIDs.GetPNForLID(context.Background(), jid)
	if err != nil || pn.IsEmpty() {
		return jid, ""
	}
	return pn, pn.User
}

func (s *AgentSession) resolveJIDToPhone(jid types.JID) string {
	if phone := jidPhoneUser(jid); phone != "" {
		return phone
	}
	if jid.Server == types.HiddenUserServer {
		if _, phone := s.resolvePNFromLID(jid); phone != "" {
			return phone
		}
	}
	return ""
}

func (s *AgentSession) enrichJIDs(info types.MessageInfo) (chatJID, senderPN, senderLID string) {
	if info.Sender.Server == types.HiddenUserServer {
		senderLID = lidUserFromJID(info.Sender)
	} else if info.SenderAlt.Server == types.HiddenUserServer {
		senderLID = lidUserFromJID(info.SenderAlt)
	}

	chatJID = info.Chat.String()
	if info.IsFromMe && info.RecipientAlt.User != "" {
		chatJID = info.RecipientAlt.String()
		senderPN = info.RecipientAlt.User
	} else if !info.IsGroup && !info.IsFromMe && info.SenderAlt.User != "" {
		chatJID = info.SenderAlt.String()
		senderPN = info.SenderAlt.User
	} else if info.Chat.Server == types.HiddenUserServer && info.RecipientAlt.User != "" {
		chatJID = info.RecipientAlt.String()
		senderPN = info.RecipientAlt.User
	}

	if senderPN == "" {
		senderPN = s.resolveJIDToPhone(info.SenderAlt)
	}
	if senderPN == "" {
		senderPN = s.resolveJIDToPhone(info.Sender)
	}
	if senderPN == "" && info.IsFromMe {
		senderPN = s.resolveJIDToPhone(info.RecipientAlt)
	}

	if chatParsed, err := types.ParseJID(chatJID); err == nil {
		if resolved, phone := s.resolvePNFromLID(chatParsed); phone != "" {
			chatJID = resolved.String()
			if senderPN == "" {
				senderPN = phone
			}
		} else if phone := jidPhoneUser(chatParsed); phone != "" && chatJID == info.Chat.String() {
			chatJID = chatParsed.String()
		}
	}

	if senderPN == "" && chatJID != "" {
		if chatParsed, err := types.ParseJID(chatJID); err == nil {
			senderPN = s.resolveJIDToPhone(chatParsed)
		}
	}

	return chatJID, senderPN, senderLID
}

func (s *AgentSession) messageFromEvent(info types.MessageInfo, message *waE2E.Message) MessageEvent {
	chatJID, senderPN, senderLID := s.enrichJIDs(info)

	msg := MessageEvent{
		ID:        info.ID,
		From:      info.Sender.String(),
		To:        chatJID,
		ChatJID:   chatJID,
		SenderPN:  senderPN,
		SenderLID: senderLID,
		Timestamp: info.Timestamp.Unix(),
		IsGroup:   info.IsGroup,
		IsFromMe:  info.IsFromMe,
		PushName:  info.PushName,
	}
	extractMessageContent(message, &msg)
	if msg.Type == "" {
		msg.Type = "unknown"
	}
	return msg
}

func (s *AgentSession) storeRawMedia(id string, message *waE2E.Message) {
	if id == "" || message == nil || maxRawMediaLimit <= 0 {
		return
	}
	slim := slimMediaMessage(message)
	if slim == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.rawMedia[id]; !exists {
		s.rawOrder = append(s.rawOrder, id)
	}
	s.rawMedia[id] = slim
	for len(s.rawOrder) > maxRawMediaLimit {
		oldest := s.rawOrder[0]
		s.rawOrder = s.rawOrder[1:]
		delete(s.rawMedia, oldest)
	}
}

func (s *AgentSession) getRawMedia(id string) (*waE2E.Message, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	msg, ok := s.rawMedia[id]
	return msg, ok
}

func (s *AgentSession) getMessageByID(id string) (MessageEvent, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, msg := range s.messages {
		if msg.ID == id {
			return msg, true
		}
	}
	return MessageEvent{}, false
}

func messageEventToProto(evt MessageEvent) *waE2E.Message {
	if !evt.HasMedia || evt.MediaURL == "" {
		return nil
	}
	mk, _ := base64.StdEncoding.DecodeString(evt.MediaKey)
	fe, _ := base64.StdEncoding.DecodeString(evt.FileEncSHA256)
	fs, _ := base64.StdEncoding.DecodeString(evt.FileSHA256)
	msg := &waE2E.Message{}
	switch evt.Type {
	case "image":
		msg.ImageMessage = &waE2E.ImageMessage{
			URL:           proto.String(evt.MediaURL),
			Mimetype:      proto.String(evt.Mimetype),
			MediaKey:      mk,
			DirectPath:    proto.String(evt.DirectPath),
			FileEncSHA256: fe,
			FileSHA256:    fs,
			FileLength:    proto.Uint64(evt.FileLength),
			Width:         proto.Uint32(evt.Width),
			Height:        proto.Uint32(evt.Height),
		}
	case "video":
		msg.VideoMessage = &waE2E.VideoMessage{
			URL:           proto.String(evt.MediaURL),
			Mimetype:      proto.String(evt.Mimetype),
			MediaKey:      mk,
			DirectPath:    proto.String(evt.DirectPath),
			FileEncSHA256: fe,
			FileSHA256:    fs,
			FileLength:    proto.Uint64(evt.FileLength),
			Width:         proto.Uint32(evt.Width),
			Height:        proto.Uint32(evt.Height),
			Seconds:       proto.Uint32(evt.Seconds),
		}
	case "sticker":
		msg.StickerMessage = &waE2E.StickerMessage{
			URL:           proto.String(evt.MediaURL),
			Mimetype:      proto.String(evt.Mimetype),
			MediaKey:      mk,
			DirectPath:    proto.String(evt.DirectPath),
			FileEncSHA256: fe,
			FileSHA256:    fs,
			FileLength:    proto.Uint64(evt.FileLength),
			Width:         proto.Uint32(evt.Width),
			Height:        proto.Uint32(evt.Height),
		}
	case "audio", "ptt":
		msg.AudioMessage = &waE2E.AudioMessage{
			URL:           proto.String(evt.MediaURL),
			Mimetype:      proto.String(evt.Mimetype),
			MediaKey:      mk,
			DirectPath:    proto.String(evt.DirectPath),
			FileEncSHA256: fe,
			FileSHA256:    fs,
			FileLength:    proto.Uint64(evt.FileLength),
			Seconds:       proto.Uint32(evt.Seconds),
			PTT:           proto.Bool(evt.Type == "ptt"),
		}
	case "document":
		msg.DocumentMessage = &waE2E.DocumentMessage{
			URL:           proto.String(evt.MediaURL),
			Mimetype:      proto.String(evt.Mimetype),
			MediaKey:      mk,
			DirectPath:    proto.String(evt.DirectPath),
			FileEncSHA256: fe,
			FileSHA256:    fs,
			FileLength:    proto.Uint64(evt.FileLength),
			FileName:      proto.String(evt.FileName),
		}
	default:
		return nil
	}
	return msg
}

func b64(data []byte) string {
	if len(data) == 0 {
		return ""
	}
	return base64.StdEncoding.EncodeToString(data)
}

func unwrapMessage(msg *waE2E.Message) *waE2E.Message {
	if msg == nil {
		return nil
	}
	switch {
	case msg.GetEphemeralMessage() != nil:
		return unwrapMessage(msg.GetEphemeralMessage().GetMessage())
	case msg.GetViewOnceMessage() != nil:
		return unwrapMessage(msg.GetViewOnceMessage().GetMessage())
	case msg.GetViewOnceMessageV2() != nil:
		return unwrapMessage(msg.GetViewOnceMessageV2().GetMessage())
	case msg.GetDocumentWithCaptionMessage() != nil:
		return unwrapMessage(msg.GetDocumentWithCaptionMessage().GetMessage())
	case msg.GetEditedMessage() != nil:
		return unwrapMessage(msg.GetEditedMessage().GetMessage())
	default:
		return msg
	}
}

func fillMediaFields(evt *MessageEvent, url, mimetype, directPath string, mediaKey, fileEnc, fileSha []byte, fileLength uint64, fileName string, width, height, seconds uint32) {
	evt.HasMedia = true
	evt.MediaURL = url
	evt.Mimetype = mimetype
	evt.DirectPath = directPath
	evt.MediaKey = b64(mediaKey)
	evt.FileEncSHA256 = b64(fileEnc)
	evt.FileSHA256 = b64(fileSha)
	evt.FileLength = fileLength
	if fileName != "" {
		evt.FileName = fileName
	}
	if width > 0 {
		evt.Width = width
	}
	if height > 0 {
		evt.Height = height
	}
	if seconds > 0 {
		evt.Seconds = seconds
	}
}

func extractMessageContent(msg *waE2E.Message, evt *MessageEvent) {
	msg = unwrapMessage(msg)
	if msg == nil {
		return
	}
	switch {
	case msg.GetConversation() != "":
		evt.Body = msg.GetConversation()
		evt.Type = "text"
	case msg.GetExtendedTextMessage() != nil:
		evt.Body = msg.GetExtendedTextMessage().GetText()
		evt.Type = "text"
	case msg.GetImageMessage() != nil:
		im := msg.GetImageMessage()
		evt.Type = "image"
		evt.Body = im.GetCaption()
		fillMediaFields(evt, im.GetURL(), im.GetMimetype(), im.GetDirectPath(), im.GetMediaKey(), im.GetFileEncSHA256(), im.GetFileSHA256(), im.GetFileLength(), "", im.GetWidth(), im.GetHeight(), 0)
	case msg.GetVideoMessage() != nil:
		vm := msg.GetVideoMessage()
		mime := vm.GetMimetype()
		if vm.GetGifPlayback() || strings.Contains(mime, "webp") {
			evt.Type = "sticker"
			evt.IsAnimatedSticker = vm.GetGifPlayback()
			fillMediaFields(evt, vm.GetURL(), mime, vm.GetDirectPath(), vm.GetMediaKey(), vm.GetFileEncSHA256(), vm.GetFileSHA256(), vm.GetFileLength(), "", vm.GetWidth(), vm.GetHeight(), vm.GetSeconds())
		} else {
			evt.Type = "video"
			evt.Body = vm.GetCaption()
			fillMediaFields(evt, vm.GetURL(), mime, vm.GetDirectPath(), vm.GetMediaKey(), vm.GetFileEncSHA256(), vm.GetFileSHA256(), vm.GetFileLength(), "", vm.GetWidth(), vm.GetHeight(), vm.GetSeconds())
		}
	case msg.GetStickerMessage() != nil:
		sm := msg.GetStickerMessage()
		evt.Type = "sticker"
		fillMediaFields(evt, sm.GetURL(), sm.GetMimetype(), sm.GetDirectPath(), sm.GetMediaKey(), sm.GetFileEncSHA256(), sm.GetFileSHA256(), sm.GetFileLength(), "", sm.GetWidth(), sm.GetHeight(), 0)
	case msg.GetDocumentMessage() != nil:
		dm := msg.GetDocumentMessage()
		evt.Type = "document"
		evt.Body = dm.GetCaption()
		if evt.Body == "" {
			evt.Body = dm.GetFileName()
		}
		fillMediaFields(evt, dm.GetURL(), dm.GetMimetype(), dm.GetDirectPath(), dm.GetMediaKey(), dm.GetFileEncSHA256(), dm.GetFileSHA256(), dm.GetFileLength(), dm.GetFileName(), 0, 0, 0)
	case msg.GetAudioMessage() != nil:
		am := msg.GetAudioMessage()
		evt.Type = "audio"
		if am.GetPTT() {
			evt.Type = "ptt"
		}
		fillMediaFields(evt, am.GetURL(), am.GetMimetype(), am.GetDirectPath(), am.GetMediaKey(), am.GetFileEncSHA256(), am.GetFileSHA256(), am.GetFileLength(), "", 0, 0, am.GetSeconds())
	case msg.GetContactMessage() != nil:
		evt.Type = "contact"
		evt.Body = msg.GetContactMessage().GetDisplayName()
	case msg.GetLocationMessage() != nil:
		evt.Type = "location"
		loc := msg.GetLocationMessage()
		evt.Body = fmt.Sprintf("📍 %.6f, %.6f", loc.GetDegreesLatitude(), loc.GetDegreesLongitude())
	case msg.GetReactionMessage() != nil:
		rm := msg.GetReactionMessage()
		evt.Type = "reaction"
		evt.Body = rm.GetText()
		if key := rm.GetKey(); key != nil {
			evt.ReactionTargetID = key.GetID()
		}
	case msg.GetButtonsResponseMessage() != nil:
		br := msg.GetButtonsResponseMessage()
		evt.Type = "button_reply"
		evt.Body = strings.TrimSpace(br.GetSelectedDisplayText())
		evt.ButtonID = strings.TrimSpace(br.GetSelectedButtonID())
		if evt.Body == "" {
			evt.Body = evt.ButtonID
		}
	case msg.GetTemplateButtonReplyMessage() != nil:
		tr := msg.GetTemplateButtonReplyMessage()
		evt.Type = "button_reply"
		evt.Body = strings.TrimSpace(tr.GetSelectedDisplayText())
		evt.ButtonID = strings.TrimSpace(tr.GetSelectedID())
		if evt.Body == "" {
			evt.Body = evt.ButtonID
		}
	case msg.GetListResponseMessage() != nil:
		lr := msg.GetListResponseMessage()
		evt.Type = "list_reply"
		evt.Body = strings.TrimSpace(lr.GetTitle())
		if single := lr.GetSingleSelectReply(); single != nil {
			evt.ButtonID = strings.TrimSpace(single.GetSelectedRowID())
			if evt.Body == "" {
				evt.Body = evt.ButtonID
			}
		}
	case msg.GetInteractiveResponseMessage() != nil:
		ir := msg.GetInteractiveResponseMessage()
		evt.Type = "button_reply"
		if body := ir.GetBody(); body != nil {
			if t := strings.TrimSpace(body.GetText()); t != "" {
				evt.Body = t
			}
		}
		if nf := ir.GetNativeFlowResponseMessage(); nf != nil {
			params := strings.TrimSpace(nf.GetParamsJSON())
			if id := extractButtonIDFromParams(params); id != "" {
				evt.ButtonID = id
			}
			if display := extractButtonDisplayFromParams(params); display != "" {
				evt.Body = display
			}
			// Si params_json viene anidado (string JSON dentro de JSON)
			if evt.ButtonID == "" {
				if nested := extractJSONStringField(params, "params_json"); nested != "" {
					if id := extractButtonIDFromParams(nested); id != "" {
						evt.ButtonID = id
					}
					if display := extractButtonDisplayFromParams(nested); display != "" {
						evt.Body = display
					}
				}
			}
			if evt.Body == "" && nf.GetName() != "" && evt.ButtonID == "" {
				evt.Body = strings.TrimSpace(nf.GetName())
			}
		}
		if evt.Body == "" && evt.ButtonID != "" {
			evt.Body = evt.ButtonID
		}
		if evt.Body == "" {
			evt.Body = "button_reply"
		}
	}
}

func extractButtonIDFromParams(params string) string {
	for _, key := range []string{"id", "button_id", "selectedId", "selected_id", "row_id"} {
		if id := extractJSONStringField(params, key); id != "" {
			return id
		}
	}
	return ""
}

func extractButtonDisplayFromParams(params string) string {
	for _, key := range []string{"display_text", "displayText", "title", "text", "selectedDisplayText"} {
		if t := extractJSONStringField(params, key); t != "" {
			return t
		}
	}
	return ""
}

// extractJSONStringField lee un campo string de un JSON plano (o JSON stringificado).
func extractJSONStringField(raw, key string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" || key == "" {
		return ""
	}
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return ""
	}
	if v, ok := m[key]; ok {
		switch t := v.(type) {
		case string:
			return strings.TrimSpace(t)
		case float64:
			return strings.TrimSpace(strconv.FormatInt(int64(t), 10))
		}
	}
	return ""
}

func extractTextFromMessage(msg *waE2E.Message) string {
	msg = unwrapMessage(msg)
	if msg == nil {
		return ""
	}
	if c := msg.GetConversation(); c != "" {
		return c
	}
	if e := msg.GetExtendedTextMessage(); e != nil {
		return e.GetText()
	}
	return ""
}

func (s *AgentSession) senderPhone(info types.MessageInfo) string {
	_, senderPN, _ := s.enrichJIDs(info)
	return senderPN
}

func (s *AgentSession) rememberUndecryptable(id string) bool {
	if id == "" {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, seen := s.undecryptableSeen[id]; seen {
		return false
	}
	s.undecryptableSeen[id] = struct{}{}
	s.undecryptableOrder = append(s.undecryptableOrder, id)
	const maxUndecryptableSeen = 500
	for len(s.undecryptableOrder) > maxUndecryptableSeen {
		oldest := s.undecryptableOrder[0]
		s.undecryptableOrder = s.undecryptableOrder[1:]
		delete(s.undecryptableSeen, oldest)
	}
	return true
}

func (s *AgentSession) handleUndecryptableMessage(evt *events.UndecryptableMessage) {
	chatJID, senderPN, senderLID := s.enrichJIDs(evt.Info)
	if !shouldProcessChat(evt.Info) {
		logVerbose("[SKIP-UNDECRYPTABLE][%s] id=%s chat=%s\n", s.AgentCode, evt.Info.ID, chatJID)
		return
	}
	firstLog := s.rememberUndecryptable(evt.Info.ID)
	if firstLog {
		fmt.Printf("[UNDECRYPTABLE][%s] id=%s from=%s chat=%s pn=%s lid=%s unavailable=%v type=%q\n",
			s.AgentCode, evt.Info.ID, evt.Info.Sender, chatJID, senderPN, senderLID,
			evt.IsUnavailable, evt.UnavailableType)
	} else {
		logVerbose("[UNDECRYPTABLE-RETRY][%s] id=%s pn=%s\n", s.AgentCode, evt.Info.ID, senderPN)
	}

	// whatsmeow ya envía retry receipts; avisamos al CRM para chats 1:1 con teléfono resuelto.
	if senderPN != "" {
		s.dispatchWebhook("messages.undecryptable", map[string]interface{}{
			"id":               evt.Info.ID,
			"message_id":       evt.Info.ID,
			"from":             evt.Info.Sender.String(),
			"chat_jid":         chatJID,
			"sender_pn":        senderPN,
			"sender_lid":       senderLID,
			"push_name":        evt.Info.PushName,
			"timestamp":        evt.Info.Timestamp.Unix(),
			"is_unavailable":   evt.IsUnavailable,
			"unavailable_type": string(evt.UnavailableType),
			"is_from_me":       evt.Info.IsFromMe,
		})
	}
}

func (s *AgentSession) handleReactionMessage(info types.MessageInfo, rm *waE2E.ReactionMessage) {
	targetID := ""
	if key := rm.GetKey(); key != nil {
		targetID = key.GetID()
	}
	fromPhone := s.senderPhone(info)
	logVerbose("[REACTION][%s] target=%s emoji=%q from=%s\n", s.AgentCode, targetID, rm.GetText(), fromPhone)
	s.dispatchWebhook("messages.reaction", map[string]interface{}{
		"reaction_target_id": targetID,
		"target_id":          targetID,
		"id":                 targetID,
		"emoji":              rm.GetText(),
		"body":               rm.GetText(),
		"from_phone":         fromPhone,
		"sender_pn":          fromPhone,
		"is_from_me":         info.IsFromMe,
		"chat_jid":           info.Chat.String(),
		"timestamp":          info.Timestamp.Unix(),
	})
}

func (s *AgentSession) handleEncReaction(evt *events.Message) {
	s.mu.RLock()
	client := s.client
	s.mu.RUnlock()
	if client == nil {
		return
	}
	rm, err := client.DecryptReaction(context.Background(), evt)
	if err != nil || rm == nil {
		fmt.Printf("[REACTION][%s] decrypt error: %v\n", s.AgentCode, err)
		return
	}
	s.handleReactionMessage(evt.Info, rm)
}

func (s *AgentSession) handleProtocolMessage(info types.MessageInfo, pm *waE2E.ProtocolMessage) {
	targetID := ""
	if key := pm.GetKey(); key != nil {
		targetID = key.GetID()
	}
	switch pm.GetType() {
	case waE2E.ProtocolMessage_REVOKE:
		logVerbose("[REVOKE][%s] id=%s chat=%s\n", s.AgentCode, targetID, info.Chat.String())
		s.dispatchWebhook("messages.deleted", map[string]interface{}{
			"id":         targetID,
			"message_id": targetID,
			"chat_jid":   info.Chat.String(),
			"timestamp":  info.Timestamp.Unix(),
			"is_from_me": info.IsFromMe,
		})
	case waE2E.ProtocolMessage_MESSAGE_EDIT:
		newBody := extractTextFromMessage(pm.GetEditedMessage())
		logVerbose("[EDIT][%s] id=%s body=%q\n", s.AgentCode, targetID, newBody)
		s.dispatchWebhook("messages.edit", map[string]interface{}{
			"id":         targetID,
			"message_id": targetID,
			"body":       newBody,
			"content":    newBody,
			"chat_jid":   info.Chat.String(),
			"timestamp":  info.Timestamp.Unix(),
			"is_from_me": info.IsFromMe,
		})
	}
}
