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
	"sort"
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
	jidByPhone         map[string]jidCacheEntry
	lastOnWAQuery      time.Time
	lastPollVoteKey    string
	lastPollVoteAt     time.Time
	statusNotifyMu     sync.Mutex
	disconnectTimer    *time.Timer
	disconnectNotified bool
	loggedOut          bool
	reconnecting       bool
	reconnectAttempt   int
	nextReconnectAt    time.Time
	lastDisconnectAt   time.Time
	lastConnectedAt    time.Time
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
	fmt.Printf("[CONFIG] max_msg_history=%d max_raw_media=%d skip_groups=%v client_log=%s sqlite_busy_ms=%d connect_stagger_sec=%d connect_delay_sec=%d reconnect_watch_sec=%d shutdown_wait_sec=%d status_debounce_sec=%d send_delay=%d-%dms jitter=%dms webhook_retry=%d/%dms\n",
		maxMsgHistLimit, maxRawMediaLimit, skipGroupsAndBroadcast, clientLogLevel, sqliteBusyTimeoutMS, autoConnectStaggerSec, autoConnectDelaySec, reconnectWatchSec, shutdownWaitSec, sessionStatusDebounceSec,
		sendMinDelayMs, sendMaxDelayMs, sendJitterMs, webhookRetryMax, webhookRetryDelayMs)

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
		jidByPhone:         make(map[string]jidCacheEntry),
		manager:            sm,
		httpClient:         sm.httpClient,
		sessionDir:         dbDir,
	}
	s.loadPollOptionsFromDisk()
	s.loadJIDCacheFromDisk()
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

func (sm *SessionManager) sortedAgentCodes() []string {
	sm.mu.RLock()
	codes := make([]string, 0, len(sm.registry.Agents))
	for code := range sm.registry.Agents {
		codes = append(codes, code)
	}
	sm.mu.RUnlock()
	sort.Strings(codes)
	return codes
}

func (sm *SessionManager) AutoConnectAll() {
	codes := sm.sortedAgentCodes()
	started := 0
	for _, code := range codes {
		if sm.IsDraining() {
			return
		}
		s, ok := sm.GetSession(code)
		if !ok {
			continue
		}
		deviceStore, err := s.container.GetFirstDevice(context.Background())
		if err != nil || deviceStore.ID == nil {
			continue
		}
		if started > 0 && autoConnectStaggerSec > 0 {
			fmt.Printf("[SESSIONS] Esperando %ds antes de conectar la siguiente línea...\n", autoConnectStaggerSec)
			time.Sleep(time.Duration(autoConnectStaggerSec) * time.Second)
			if sm.IsDraining() {
				return
			}
		}
		started++
		fmt.Printf("[SESSIONS] Auto-connecting %s (%d/%d)...\n", code, started, len(codes))
		if err := s.connectExisting(); err != nil {
			s.noteReconnectFailure()
			fmt.Printf("[SESSIONS] Auto-connect %s failed: %v (próximo reintento con backoff)\n", s.AgentCode, err)
		}
	}
}

// ReconnectDisconnected reconecta sesiones con device que quedaron offline.
// Una por vez, con backoff. No toca pairing/QR ni hace logout.
func (sm *SessionManager) ReconnectDisconnected() {
	if sm.IsDraining() {
		return
	}
	codes := sm.sortedAgentCodes()
	started := 0
	for _, code := range codes {
		if sm.IsDraining() {
			return
		}
		s, ok := sm.GetSession(code)
		if !ok {
			continue
		}
		if !s.shouldAutoReconnectNow() {
			continue
		}
		deviceStore, err := s.container.GetFirstDevice(context.Background())
		if err != nil || deviceStore.ID == nil {
			continue
		}
		if started > 0 && autoConnectStaggerSec > 0 {
			time.Sleep(time.Duration(autoConnectStaggerSec) * time.Second)
			if sm.IsDraining() {
				return
			}
			if !s.shouldAutoReconnectNow() {
				continue
			}
		}
		started++
		fmt.Printf("[SESSIONS] ReconnectDisconnected %s (intento backoff)...\n", code)
		if err := s.connectExisting(); err != nil {
			s.noteReconnectFailure()
			fmt.Printf("[SESSIONS] ReconnectDisconnected %s failed: %v\n", s.AgentCode, err)
		}
	}
}

func (s *AgentSession) isLoggedInLocked() bool {
	return s.client != nil && s.client.Store != nil && s.client.Store.ID != nil
}

func (s *AgentSession) isLiveLocked() bool {
	return s.client != nil && s.client.IsConnected() && s.isLoggedInLocked()
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

	liveWS := s.client != nil && s.client.IsConnected()
	// Preferir websocket real; flag stale con WS caído → disconnected (has_session true)
	if loggedIn && liveWS {
		info.Connected = true
		info.Status = "connected"
	} else if loggedIn && !liveWS {
		info.Connected = false
		info.HasSession = true
		info.Status = "disconnected"
	} else if s.client != nil && s.qrCode != "" {
		info.Status = "need_scan"
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

		maxRetries := webhookRetryMax
		if maxRetries < 0 {
			maxRetries = 0
		}
		retryDelayMs := webhookRetryDelayMs
		if retryDelayMs < 0 {
			retryDelayMs = 0
		}

		for attempt := 0; attempt <= maxRetries; attempt++ {
			if attempt > 0 {
				time.Sleep(time.Duration(retryDelayMs) * time.Millisecond)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(payload))
			if err != nil {
				cancel()
				return
			}
			req.Header.Set("Content-Type", "application/json; charset=utf-8")
			if secret != "" {
				req.Header.Set("X-Webhook-Secret", secret)
			}

			resp, err := client.Do(req)
			cancel()
			if err != nil {
				if attempt < maxRetries {
					fmt.Printf("[WEBHOOK] %s %s → error (reintento %d/%d): %v\n", agentCode, event, attempt+1, maxRetries, err)
					continue
				}
				fmt.Printf("[WEBHOOK] %s %s → error: %v\n", agentCode, event, err)
				return
			}
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
			resp.Body.Close()

			if resp.StatusCode >= 500 && attempt < maxRetries {
				fmt.Printf("[WEBHOOK] %s %s → HTTP %d (reintento %d/%d)\n", agentCode, event, resp.StatusCode, attempt+1, maxRetries)
				continue
			}
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
			return
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

	case *events.StreamReplaced:
		s.handleStreamReplaced()

	case *events.LoggedOut:
		s.cancelDisconnectNotify()
		s.mu.Lock()
		s.connected = false
		s.loggedOut = true
		s.reconnecting = false
		s.reconnectAttempt = 0
		s.nextReconnectAt = time.Time{}
		s.client = nil
		s.qrCode = ""
		s.mu.Unlock()
		fmt.Printf("✗ [%s] WhatsApp logged out (sin auto-reconnect)\n", s.AgentCode)
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

func (s *AgentSession) handleStreamReplaced() {
	if s.manager != nil && s.manager.IsDraining() {
		s.mu.Lock()
		s.connected = false
		s.reconnecting = false
		s.mu.Unlock()
		return
	}

	s.cancelDisconnectNotify()
	s.mu.Lock()
	if s.client != nil {
		s.client.EnableAutoReconnect = false
	}
	s.connected = false
	s.reconnecting = false
	s.lastDisconnectAt = time.Now()
	s.nextReconnectAt = time.Now().Add(streamReplacedBackoff)
	s.mu.Unlock()
	fmt.Printf("[SESSIONS] %s StreamReplaced: otra conexión tomó el socket. Reintento en %s (sin logout).\n",
		s.AgentCode, streamReplacedBackoff)
}

func (s *AgentSession) handleDisconnected() {
	if s.manager != nil && s.manager.IsDraining() {
		s.mu.Lock()
		s.connected = false
		s.mu.Unlock()
		return
	}

	s.mu.Lock()
	s.markDisconnectedLocked()
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
	s.lastConnectedAt = time.Now()
	s.resetReconnectStateLocked()
	s.qrCode = ""
	phone := s.client.Store.ID.User
	if s.client != nil {
		s.client.AutoReconnectErrors = 0
	}
	localClient := s.client
	s.mu.Unlock()

	// Señalizar al servidor de WhatsApp que el cliente está disponible.
	// Esto es importante para el cumplimiento de políticas de Meta y evitar
	// que la sesión sea marcada como inactiva o sospechosa.
	go func() {
		if localClient != nil {
			_ = localClient.SendPresence(types.PresenceAvailable)
		}
	}()

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
	// Recibos delivered/read son tráfico interno de WhatsApp. Reenviarlos al
	// dashboard satura Vercel y no alimenta el agente (Agente_IA los ignora).
	if skipGroupsAndBroadcast && (receipt.IsGroup || !shouldProcessChatJID(receipt.Chat.String())) {
		return
	}
	status := receiptTypeToStatus(receipt.Type)
	if status < 0 {
		return
	}
	logVerbose("[RECEIPT][%s] chat=%s type=%s status=%d (no webhook)\n",
		s.AgentCode, receipt.Chat.String(), receipt.Type, status)
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

func (s *AgentSession) shouldAutoReconnectNow() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.isLiveLocked() {
		return false
	}
	return !shouldSkipAutoReconnect(s.loggedOut, false, s.reconnecting, time.Now(), s.nextReconnectAt)
}

func (s *AgentSession) noteReconnectFailure() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reconnectAttempt = reconnectAttemptCapped(s.reconnectAttempt + 1)
	s.nextReconnectAt = time.Now().Add(reconnectDelayAfterAttempt(s.reconnectAttempt))
	s.reconnecting = false
	fmt.Printf("[SESSIONS] %s próximo reintento en %s (intento %d)\n",
		s.AgentCode, reconnectDelayAfterAttempt(s.reconnectAttempt), s.reconnectAttempt)
}

func (s *AgentSession) resetReconnectStateLocked() {
	s.reconnectAttempt = 0
	s.nextReconnectAt = time.Time{}
	s.loggedOut = false
	s.reconnecting = false
}

func (s *AgentSession) markDisconnectedLocked() {
	now := time.Now()
	dur := time.Duration(0)
	if !s.lastConnectedAt.IsZero() {
		dur = now.Sub(s.lastConnectedAt)
	}
	next, attempt := nextReconnectAtAfterDisconnect(now, s.lastConnectedAt, s.nextReconnectAt, s.reconnectAttempt, dur)
	s.lastDisconnectAt = now
	s.reconnectAttempt = attempt
	s.nextReconnectAt = next
	s.connected = false
}

func (s *AgentSession) connectExisting() error {
	s.mu.Lock()
	if s.loggedOut {
		s.mu.Unlock()
		return fmt.Errorf("session logged out; scan QR to login")
	}
	if s.isLiveLocked() {
		s.connected = true
		s.mu.Unlock()
		return nil
	}
	if s.reconnecting {
		s.mu.Unlock()
		return fmt.Errorf("reconnect already in progress")
	}

	disconnectAge := time.Duration(0)
	if !s.lastDisconnectAt.IsZero() {
		disconnectAge = time.Since(s.lastDisconnectAt)
	}
	neverConnected := s.lastConnectedAt.IsZero()
	clientExists := s.client != nil
	if !shouldReplaceWhatsAppClient(false, clientExists, neverConnected, disconnectAge) {
		s.mu.Unlock()
		return nil
	}

	s.reconnecting = true

	if s.client != nil {
		if s.isLiveLocked() {
			s.reconnecting = false
			s.connected = true
			s.mu.Unlock()
			return nil
		}
		s.client.Disconnect()
		s.client = nil
		s.connected = false
	}

	clientLog := newClientLogger(s.AgentCode)
	deviceStore, err := s.container.GetFirstDevice(context.Background())
	if err != nil {
		s.reconnecting = false
		s.mu.Unlock()
		return err
	}

	client := whatsmeow.NewClient(deviceStore, clientLog)
	configureWhatsAppClient(client)
	client.AddEventHandler(s.eventHandler)
	s.client = client
	s.mu.Unlock()

	err = client.Connect()
	if err != nil {
		s.mu.Lock()
		s.reconnecting = false
		s.mu.Unlock()
	}
	return err
}

func (s *AgentSession) Connect() (needsQR bool, err error) {
	s.mu.Lock()
	if s.isLiveLocked() {
		s.connected = true
		s.resetReconnectStateLocked()
		s.mu.Unlock()
		return false, nil
	}

	s.resetReconnectStateLocked()

	// Sesión ya vinculada: no cortar el cliente para generar QR.
	if s.client != nil && s.isLoggedInLocked() {
		s.mu.Unlock()
		return false, s.connectExisting()
	}

	if s.client != nil {
		s.client.Disconnect()
		s.client = nil
		s.connected = false
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
	s.client = nil
	s.connected = false
	s.qrCode = ""
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
	if s.client == nil || !s.isLoggedInLocked() {
		return nil, false
	}
	// No confiar solo en s.connected: puede quedar true con websocket muerto
	// (luego falla usync: "websocket not connected").
	if !s.client.IsConnected() {
		return nil, false
	}
	return s.client, true
}

// EnsureConnected espera la reconexión automática. No hace login nuevo en cada envío.
func (s *AgentSession) EnsureConnected() error {
	if _, ok := s.Client(); ok {
		return nil
	}

	s.mu.RLock()
	loggedOut := s.loggedOut
	nextAt := s.nextReconnectAt
	s.mu.RUnlock()
	if loggedOut {
		return fmt.Errorf("session logged out")
	}

	deadline := time.Now().Add(ensureConnectedWait)
	for time.Now().Before(deadline) {
		if _, ok := s.Client(); ok {
			return nil
		}
		time.Sleep(ensureConnectedStep)
	}
	if _, ok := s.Client(); ok {
		return nil
	}

	if !nextAt.IsZero() && time.Now().Before(nextAt) {
		return fmt.Errorf("websocket not connected; reconnect scheduled in %s", time.Until(nextAt).Round(time.Second))
	}

	if err := s.connectExisting(); err != nil {
		return err
	}
	waitDeadline := time.Now().Add(ensureConnectedWait)
	for time.Now().Before(waitDeadline) {
		if _, ok := s.Client(); ok {
			return nil
		}
		time.Sleep(ensureConnectedStep)
	}
	if _, ok := s.Client(); ok {
		return nil
	}
	return fmt.Errorf("websocket still not connected after reconnect")
}

func (s *AgentSession) Messages() []MessageEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]MessageEvent, len(s.messages))
	copy(out, s.messages)
	return out
}

func (s *AgentSession) storeSentMessage(id, toJID, body, msgType string, timestamp int64, phoneHint ...string) {
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

	// Completar sender_pn / sender_lid para que el CRM pueda resolver el teléfono
	// (los envíos vía API no generan events.Message de WhatsApp).
	parsed := parseJID(toJID)
	if parsed.Server == types.DefaultUserServer && parsed.User != "" {
		msg.SenderPN = parsed.User
	} else if parsed.Server == types.HiddenUserServer {
		msg.SenderLID = stripDeviceFromUser(parsed.User)
		if _, pnUser := s.resolvePNFromLID(parsed); pnUser != "" {
			msg.SenderPN = pnUser
		}
	}
	if msg.SenderPN == "" && len(phoneHint) > 0 && phoneHint[0] != "" {
		msg.SenderPN = phoneHint[0]
	}
	if !parsed.IsEmpty() && msg.SenderPN != "" {
		s.rememberPeerJID(msg.SenderPN, parsed)
	}

	s.appendMessage(msg)
	// Persistir en CRM: sin esto, los recordatorios/cron aparecen "enviados"
	// en logs pero nunca llegan a chats/messages de Supabase.
	s.dispatchWebhook("messages.upsert", msg)
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

func matchPollSelection(options []string, selected [][]byte) (int, string) {
	if len(options) == 0 || len(selected) == 0 {
		return -1, ""
	}
	hashes := whatsmeow.HashPollOptions(options)
	for _, sel := range selected {
		for i, h := range hashes {
			if bytes.Equal(sel, h) {
				return i, options[i]
			}
		}
	}
	return -1, ""
}

func (s *AgentSession) resolvePollVote(pollID string, selected [][]byte) (options []string, idx int, text string) {
	if pollID != "" {
		options = s.getPollOptions(pollID)
		if idx, text = matchPollSelection(options, selected); idx >= 0 {
			return options, idx, text
		}
	}

	s.mu.RLock()
	ids := append([]string(nil), s.pollOrder...)
	copied := make(map[string][]string, len(s.pollOptions))
	for id, opts := range s.pollOptions {
		next := make([]string, len(opts))
		copy(next, opts)
		copied[id] = next
	}
	s.mu.RUnlock()

	for i := len(ids) - 1; i >= 0; i-- {
		opts := copied[ids[i]]
		if idx, text = matchPollSelection(opts, selected); idx >= 0 {
			return opts, idx, text
		}
	}
	return nil, -1, ""
}

func (s *AgentSession) pollVoterPhone(info types.MessageInfo) string {
	if phone := s.resolveJIDToPhone(info.Chat); phone != "" {
		return phone
	}
	if info.RecipientAlt.User != "" {
		if phone := s.resolveJIDToPhone(info.RecipientAlt); phone != "" {
			return phone
		}
	}
	if info.SenderAlt.User != "" {
		if phone := s.resolveJIDToPhone(info.SenderAlt); phone != "" {
			return phone
		}
	}
	return s.resolveJIDToPhone(info.Sender)
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
	_, selectedIdx, selectedText := s.resolvePollVote(pollID, vote.GetSelectedOptions())

	msg := s.messageFromEvent(evt.Info, evt.Message)
	msg.Type = "button_reply"
	msg.PollID = pollID
	msg.IsFromMe = false
	if voter := s.pollVoterPhone(evt.Info); voter != "" {
		msg.SenderPN = voter
	}
	if selectedIdx >= 0 {
		num := selectedIdx + 1
		msg.ButtonID = fmt.Sprintf("opt_%d", num)
		msg.Body = selectedText
		msg.PollOption = selectedText
	} else if selectedText != "" {
		msg.Body = selectedText
		msg.PollOption = selectedText
	} else {
		fmt.Printf("[POLL_VOTE][%s] id=%s poll=%s — sin opciones guardadas (hashes=%d)\n",
			s.AgentCode, msg.ID, pollID, len(vote.GetSelectedOptions()))
		msg.Body = ""
		msg.Type = "poll_vote"
	}

	fmt.Printf("[POLL_VOTE][%s] id=%s poll=%s fromMe=%v pn=%s button_id=%q body=%q option=%q\n",
		s.AgentCode, msg.ID, pollID, evt.Info.IsFromMe, msg.SenderPN, msg.ButtonID, msg.Body, selectedText)

	if msg.Body == "" && msg.ButtonID == "" {
		return
	}

	s.appendMessage(msg)

	voteKey := strings.TrimSpace(pollID) + "|" + strings.TrimSpace(msg.SenderPN) + "|" + strings.TrimSpace(msg.PollOption+msg.ButtonID+msg.Body)
	s.mu.Lock()
	if voteKey != "" && voteKey == s.lastPollVoteKey && time.Since(s.lastPollVoteAt) < 15*time.Second {
		s.mu.Unlock()
		fmt.Printf("[POLL_VOTE][%s] duplicado ignorado key=%s\n", s.AgentCode, voteKey)
		return
	}
	s.lastPollVoteKey = voteKey
	s.lastPollVoteAt = time.Now()
	s.mu.Unlock()

	// Un solo webhook: upsert+button+poll disparaba 3 respuestas y Meta lo marca como spam.
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
	s.rememberInboundPeer(info, senderPN, chatJID)
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
		evt.Latitude = loc.GetDegreesLatitude()
		evt.Longitude = loc.GetDegreesLongitude()
		evt.Address = strings.TrimSpace(loc.GetName())
		if addr := strings.TrimSpace(loc.GetAddress()); addr != "" {
			if evt.Address != "" {
				evt.Address = evt.Address + " — " + addr
			} else {
				evt.Address = addr
			}
		}
		evt.Body = fmt.Sprintf("📍 %.6f, %.6f", evt.Latitude, evt.Longitude)
		if evt.Address != "" {
			evt.Body = evt.Address
		}
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

	// whatsmeow ya envía retry receipts. No reenviar al dashboard.
	return
}

func (s *AgentSession) handleReactionMessage(info types.MessageInfo, rm *waE2E.ReactionMessage) {
	targetID := ""
	if key := rm.GetKey(); key != nil {
		targetID = key.GetID()
	}
	fromPhone := s.senderPhone(info)
	logVerbose("[REACTION][%s] target=%s emoji=%q from=%s\n", s.AgentCode, targetID, rm.GetText(), fromPhone)
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
	case waE2E.ProtocolMessage_MESSAGE_EDIT:
		newBody := extractTextFromMessage(pm.GetEditedMessage())
		logVerbose("[EDIT][%s] id=%s body=%q\n", s.AgentCode, targetID, newBody)
	}
}
