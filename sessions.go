package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
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

const maxMsgHist = 1000
const maxRawMedia = 500

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
	mu        sync.RWMutex

	manager    *SessionManager
	httpClient *http.Client
}

type SessionManager struct {
	dataDir       string
	registryPath  string
	defaultSecret string
	registry      *SessionRegistry
	sessions      map[string]*AgentSession
	mu            sync.RWMutex
	httpClient    *http.Client
}

func NewSessionManager(dataDir, defaultSecret string) (*SessionManager, error) {
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

	dbPath := "file:" + sm.sessionDBPath(agentCode) + "?_foreign_keys=on"
	dbLog := waLog.Stdout("Database-"+safeAgentDir(agentCode), "WARN", true)
	container, err := sqlstore.New(context.Background(), "sqlite3", dbPath, dbLog)
	if err != nil {
		return nil, err
	}

	if webhookSecret == "" {
		webhookSecret = sm.defaultSecret
	}

	s := &AgentSession{
		AgentCode:     agentCode,
		WebhookURL:    webhookURL,
		WebhookSecret: webhookSecret,
		container:     container,
		messages:      make([]MessageEvent, 0),
		rawMedia:      make(map[string]*waE2E.Message),
		rawOrder:      make([]string, 0),
		manager:       sm,
		httpClient:    sm.httpClient,
	}
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

func (sm *SessionManager) AutoConnectAll() {
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
		deviceStore, err := s.container.GetFirstDevice(context.Background())
		if err != nil || deviceStore.ID == nil {
			continue
		}
		fmt.Printf("[SESSIONS] Auto-connecting %s...\n", code)
		go func(session *AgentSession) {
			if err := session.connectExisting(); err != nil {
				fmt.Printf("[SESSIONS] Auto-connect %s failed: %v\n", session.AgentCode, err)
			}
		}(s)
	}
}

func (s *AgentSession) StatusInfo() SessionStatusInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	info := SessionStatusInfo{
		AgentCode:  s.AgentCode,
		WebhookURL: s.WebhookURL,
		Status:     "disconnected",
	}

	if s.client != nil {
		if s.client.Store.ID != nil {
			info.HasSession = true
		}
		if s.connected {
			info.Connected = true
			info.Status = "connected"
			if s.client.Store.ID != nil {
				info.Phone = s.client.Store.ID.User
			}
		} else if s.qrCode != "" {
			info.Status = "need_scan"
		} else if info.HasSession {
			info.Status = "connecting"
		} else {
			info.Status = "logged_out"
		}
	} else {
		deviceStore, err := s.container.GetFirstDevice(context.Background())
		if err == nil && deviceStore.ID != nil {
			info.HasSession = true
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

		req, err := http.NewRequest("POST", url, strings.NewReader(string(payload)))
		if err != nil {
			return
		}
		req.Header.Set("Content-Type", "application/json")
		if secret != "" {
			req.Header.Set("X-Webhook-Secret", secret)
		}

		resp, err := client.Do(req)
		if err != nil {
			fmt.Printf("[WEBHOOK] %s %s → error: %v\n", agentCode, event, err)
			return
		}
		defer resp.Body.Close()
		fmt.Printf("[WEBHOOK] %s %s → %s: %d\n", agentCode, event, url, resp.StatusCode)
	}()
}

func (s *AgentSession) eventHandler(evt interface{}) {
	switch v := evt.(type) {
	case *events.Message:
		if v.Message == nil {
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
		msg := messageFromEvent(v.Info, v.Message)
		fmt.Printf("[MSG][%s] from=%s chat=%s pn=%s isFromMe=%v type=%s hasMedia=%v\n",
			s.AgentCode, msg.From, msg.ChatJID, msg.SenderPN, msg.IsFromMe, msg.Type, msg.HasMedia)
		if msg.HasMedia && v.Message != nil {
			s.storeRawMedia(msg.ID, v.Message)
		}
		s.appendMessage(msg)
		s.dispatchWebhook("messages.upsert", msg)

	case *events.Receipt:
		s.handleReceipt(v)

	case *events.Connected:
		s.mu.Lock()
		s.connected = true
		s.mu.Unlock()
		fmt.Printf("✓ [%s] WhatsApp connected\n", s.AgentCode)
		s.dispatchWebhook("session.status", map[string]string{"status": "connected"})

	case *events.Disconnected:
		s.mu.Lock()
		s.connected = false
		s.mu.Unlock()
		fmt.Printf("✗ [%s] WhatsApp disconnected\n", s.AgentCode)
		s.dispatchWebhook("session.status", map[string]string{"status": "disconnected"})

	case *events.LoggedOut:
		s.mu.Lock()
		s.connected = false
		s.client = nil
		s.mu.Unlock()
		fmt.Printf("✗ [%s] WhatsApp logged out\n", s.AgentCode)
		s.dispatchWebhook("session.status", map[string]string{"status": "logged_out"})
	}
}

func (s *AgentSession) handleReceipt(receipt *events.Receipt) {
	status := receiptTypeToStatus(receipt.Type)
	if status < 0 {
		return
	}
	chatJID := receipt.Chat.String()
	for _, msgID := range receipt.MessageIDs {
		if msgID == "" {
			continue
		}
		fmt.Printf("[RECEIPT][%s] id=%s chat=%s type=%s status=%d\n", s.AgentCode, msgID, chatJID, receipt.Type, status)
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
	if len(s.messages) > maxMsgHist {
		s.messages = s.messages[len(s.messages)-maxMsgHist:]
	}
}

func (s *AgentSession) connectExisting() error {
	s.mu.Lock()
	if s.connected && s.client != nil {
		s.mu.Unlock()
		return nil
	}

	clientLog := waLog.Stdout("Client-"+safeAgentDir(s.AgentCode), "WARN", true)
	deviceStore, err := s.container.GetFirstDevice(context.Background())
	if err != nil {
		s.mu.Unlock()
		return err
	}

	client := whatsmeow.NewClient(deviceStore, clientLog)
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

	clientLog := waLog.Stdout("Client-"+safeAgentDir(s.AgentCode), "WARN", true)
	client := whatsmeow.NewClient(deviceStore, clientLog)
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
				s.mu.Lock()
				if evt.Event == "code" {
					s.qrCode = evt.Code
				} else {
					s.qrCode = ""
				}
				s.mu.Unlock()
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
	if !s.connected || s.client == nil {
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

func messageFromEvent(info types.MessageInfo, message *waE2E.Message) MessageEvent {
	senderPN := ""
	if info.SenderAlt.User != "" {
		senderPN = info.SenderAlt.User
	}

	chatJID := info.Chat.String()
	if info.IsFromMe && info.RecipientAlt.User != "" {
		chatJID = info.RecipientAlt.String()
		senderPN = info.RecipientAlt.User
	} else if !info.IsGroup && !info.IsFromMe && info.SenderAlt.User != "" {
		chatJID = info.SenderAlt.String()
	} else if info.Chat.Server == types.HiddenUserServer && info.RecipientAlt.User != "" {
		chatJID = info.RecipientAlt.String()
		if senderPN == "" {
			senderPN = info.RecipientAlt.User
		}
	}

	msg := MessageEvent{
		ID:        info.ID,
		From:      info.Sender.String(),
		To:        info.Chat.String(),
		ChatJID:   chatJID,
		SenderPN:  senderPN,
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
	if id == "" || message == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.rawMedia[id]; !exists {
		s.rawOrder = append(s.rawOrder, id)
	}
	s.rawMedia[id] = proto.Clone(message).(*waE2E.Message)
	for len(s.rawOrder) > maxRawMedia {
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
	}
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
	if info.IsFromMe && info.RecipientAlt.User != "" {
		return info.RecipientAlt.User
	}
	if info.SenderAlt.User != "" {
		return info.SenderAlt.User
	}
	return info.Sender.User
}

func (s *AgentSession) handleReactionMessage(info types.MessageInfo, rm *waE2E.ReactionMessage) {
	targetID := ""
	if key := rm.GetKey(); key != nil {
		targetID = key.GetID()
	}
	fromPhone := s.senderPhone(info)
	fmt.Printf("[REACTION][%s] target=%s emoji=%q from=%s\n", s.AgentCode, targetID, rm.GetText(), fromPhone)
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
		fmt.Printf("[REVOKE][%s] id=%s chat=%s\n", s.AgentCode, targetID, info.Chat.String())
		s.dispatchWebhook("messages.deleted", map[string]interface{}{
			"id":         targetID,
			"message_id": targetID,
			"chat_jid":   info.Chat.String(),
			"timestamp":  info.Timestamp.Unix(),
			"is_from_me": info.IsFromMe,
		})
	case waE2E.ProtocolMessage_MESSAGE_EDIT:
		newBody := extractTextFromMessage(pm.GetEditedMessage())
		fmt.Printf("[EDIT][%s] id=%s body=%q\n", s.AgentCode, targetID, newBody)
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
