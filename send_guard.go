package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
)

const (
	jidPositiveTTL    = 24 * time.Hour
	jidNegativeTTL    = 30 * time.Minute
	onWAMinInterval   = 8 * time.Second
	typingPollDelay   = 800 * time.Millisecond
)

type jidCacheEntry struct {
	JID     types.JID `json:"-"`
	JIDStr  string    `json:"jid,omitempty"`
	Present bool      `json:"present"`
	At      time.Time `json:"at"`
}

func digitsOnlyPhone(phone string) string {
	var b strings.Builder
	for _, r := range phone {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func phoneLookupKeys(digits string) []string {
	digits = strings.TrimSpace(digits)
	if digits == "" {
		return nil
	}
	seen := map[string]struct{}{digits: {}}
	keys := []string{digits}
	add := func(k string) {
		if k == "" {
			return
		}
		if _, ok := seen[k]; ok {
			return
		}
		seen[k] = struct{}{}
		keys = append(keys, k)
	}
	if strings.HasPrefix(digits, "549") && len(digits) > 3 {
		add(digits[3:])
	} else if strings.HasPrefix(digits, "54") && len(digits) > 2 {
		rest := digits[2:]
		add(rest)
		if !strings.HasPrefix(rest, "9") {
			add("549" + rest)
		}
	} else if len(digits) >= 8 && len(digits) <= 11 {
		add("549" + digits)
		add("54" + digits)
	}
	return keys
}

func (s *AgentSession) jidCachePath() string {
	if s == nil || s.sessionDir == "" {
		return ""
	}
	return filepath.Join(s.sessionDir, "jid_cache.json")
}

func (s *AgentSession) loadJIDCacheFromDisk() {
	if s == nil {
		return
	}
	path := s.jidCachePath()
	if path == "" {
		return
	}
	data, err := os.ReadFile(path)
	if err != nil || len(data) == 0 {
		return
	}
	var stored map[string]jidCacheEntry
	if err := json.Unmarshal(data, &stored); err != nil || len(stored) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.jidByPhone == nil {
		s.jidByPhone = map[string]jidCacheEntry{}
	}
	now := time.Now()
	loaded := 0
	for phone, entry := range stored {
		if !entry.Present || entry.JIDStr == "" {
			continue
		}
		if !entry.At.IsZero() && now.Sub(entry.At) > jidPositiveTTL {
			continue
		}
		jid, err := types.ParseJID(entry.JIDStr)
		if err != nil || jid.IsEmpty() {
			continue
		}
		entry.JID = jid.ToNonAD()
		s.jidByPhone[phone] = entry
		loaded++
	}
	if loaded > 0 {
		fmt.Printf("[JID-CACHE][%s] loaded %d entries\n", s.AgentCode, loaded)
	}
}

func (s *AgentSession) persistJIDCacheLocked() {
	path := s.jidCachePath()
	if path == "" || s.jidByPhone == nil {
		return
	}
	out := make(map[string]jidCacheEntry, len(s.jidByPhone))
	for phone, entry := range s.jidByPhone {
		if !entry.Present {
			continue
		}
		jidStr := entry.JIDStr
		if jidStr == "" && !entry.JID.IsEmpty() {
			jidStr = entry.JID.ToNonAD().String()
		}
		if jidStr == "" {
			continue
		}
		out[phone] = jidCacheEntry{JIDStr: jidStr, Present: true, At: entry.At}
	}
	data, err := json.Marshal(out)
	if err != nil {
		return
	}
	_ = os.WriteFile(path, data, 0o644)
}

func (s *AgentSession) rememberPeerJID(phone string, jid types.JID) {
	if s == nil || jid.IsEmpty() {
		return
	}
	if jid.Server == types.HiddenUserServer {
		jid = normalizeLIDJID(jid)
	} else {
		jid = jid.ToNonAD()
	}
	keys := phoneLookupKeys(digitsOnlyPhone(phone))
	if len(keys) == 0 {
		return
	}
	entry := jidCacheEntry{JID: jid, JIDStr: jid.String(), Present: true, At: time.Now()}
	s.mu.Lock()
	if s.jidByPhone == nil {
		s.jidByPhone = map[string]jidCacheEntry{}
	}
	for _, key := range keys {
		s.jidByPhone[key] = entry
	}
	s.persistJIDCacheLocked()
	s.mu.Unlock()
}

func (s *AgentSession) rememberMissingPhone(phone string) {
	if s == nil {
		return
	}
	keys := phoneLookupKeys(digitsOnlyPhone(phone))
	if len(keys) == 0 {
		return
	}
	entry := jidCacheEntry{Present: false, At: time.Now()}
	s.mu.Lock()
	if s.jidByPhone == nil {
		s.jidByPhone = map[string]jidCacheEntry{}
	}
	for _, key := range keys {
		s.jidByPhone[key] = entry
	}
	s.mu.Unlock()
}

func (s *AgentSession) rememberInboundPeer(info types.MessageInfo, senderPN, chatJID string) {
	if s == nil || info.IsGroup {
		return
	}
	chat := info.Chat
	if chatJID != "" {
		if parsed, err := types.ParseJID(chatJID); err == nil && !parsed.IsEmpty() {
			chat = parsed
		}
	}
	if chat.IsEmpty() {
		return
	}
	if chat.Server != types.DefaultUserServer && chat.Server != types.HiddenUserServer {
		return
	}
	s.rememberPeerJID(senderPN, chat)
	if senderPN == "" && chat.Server == types.DefaultUserServer {
		s.rememberPeerJID(chat.User, chat)
	}
}

func (s *AgentSession) lookupPeerJID(phone string) (types.JID, bool, bool) {
	if s == nil {
		return types.EmptyJID, false, false
	}
	keys := phoneLookupKeys(digitsOnlyPhone(phone))
	if len(keys) == 0 {
		return types.EmptyJID, false, false
	}
	now := time.Now()
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.jidByPhone == nil {
		return types.EmptyJID, false, false
	}
	for _, key := range keys {
		entry, ok := s.jidByPhone[key]
		if !ok {
			continue
		}
		ttl := jidPositiveTTL
		if !entry.Present {
			ttl = jidNegativeTTL
		}
		if !entry.At.IsZero() && now.Sub(entry.At) > ttl {
			continue
		}
		if !entry.Present {
			return types.EmptyJID, false, true
		}
		if entry.JID.IsEmpty() && entry.JIDStr != "" {
			if parsed, err := types.ParseJID(entry.JIDStr); err == nil {
				return parsed.ToNonAD(), true, false
			}
			continue
		}
		if entry.JID.IsEmpty() {
			continue
		}
		return entry.JID, true, false
	}
	return types.EmptyJID, false, false
}

func (s *AgentSession) allowOnWhatsAppQuery() bool {
	if s == nil {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastOnWAQuery.IsZero() || time.Since(s.lastOnWAQuery) >= onWAMinInterval {
		s.lastOnWAQuery = time.Now()
		return true
	}
	return false
}

// resolveSendJIDCached evita IsOnWhatsApp en cada envío: usa JID de chats
// entrantes / envíos previos. IsOnWhatsApp es la consulta que más bloqueos genera.
func resolveSendJIDCached(s *AgentSession, _ *whatsmeow.Client, phone string) types.JID {
	phone = strings.TrimSpace(phone)
	if phone == "" {
		return types.EmptyJID
	}
	if strings.Contains(phone, "@") {
		jid := parseJID(phone)
		if jid.Server == types.HiddenUserServer {
			jid = normalizeLIDJID(jid)
		} else {
			jid = jid.ToNonAD()
		}
		if s != nil && !jid.IsEmpty() {
			s.rememberPeerJID(digitsOnlyPhone(phone), jid)
		}
		return jid
	}

	digits := digitsOnlyPhone(phone)
	if s != nil {
		if cached, ok, missing := s.lookupPeerJID(digits); missing {
			return parseJID(digits)
		} else if ok {
			return cached
		}
	}

	// Nunca IsOnWhatsApp en el send: Meta lo usa para detectar scrapers y banea
	// la sesión. El JID se arma con el número; el cache se llena con chats entrantes.
	return parseJID(digits)
}

func resolveSendJID(client *whatsmeow.Client, phone string) types.JID {
	return resolveSendJIDCached(nil, client, phone)
}

func (s *AgentSession) resolveSendJID(client *whatsmeow.Client, phone string) types.JID {
	return resolveSendJIDCached(s, client, phone)
}

// typingDelayForMessage calcula cuánto tiempo "escribir" antes de enviar.
// Usa el largo del texto como base y agrega jitter aleatorio para parecer
// más humano y reducir riesgo de bloqueo por Meta.
func typingDelayForMessage(msg *waE2E.Message) time.Duration {
	minMs := sendMinDelayMs
	maxMs := sendMaxDelayMs
	if minMs <= 0 {
		minMs = 800
	}
	if maxMs <= 0 || maxMs < minMs {
		maxMs = minMs + 2700
	}
	jitterRange := sendJitterMs
	if jitterRange < 0 {
		jitterRange = 0
	}

	if msg == nil {
		return withJitter(minMs, jitterRange, maxMs)
	}
	if msg.GetPollCreationMessage() != nil {
		return typingPollDelay
	}
	text := strings.TrimSpace(msg.GetConversation())
	if text == "" {
		if ext := msg.GetExtendedTextMessage(); ext != nil {
			text = strings.TrimSpace(ext.GetText())
		}
	}
	if text == "" {
		return withJitter(minMs, jitterRange, maxMs)
	}
	n := utf8.RuneCountInString(text)
	baseMs := minMs + n*20
	return withJitter(baseMs, jitterRange, maxMs)
}

// withJitter suma un valor aleatorio [0, jitterRange] al base y acota al máximo.
func withJitter(baseMs, jitterRange, maxMs int) time.Duration {
	ms := baseMs
	if jitterRange > 0 {
		ms += rand.Intn(jitterRange + 1)
	}
	if ms > maxMs {
		ms = maxMs
	}
	return time.Duration(ms) * time.Millisecond
}

func (s *AgentSession) sendWithHumanPacing(
	client *whatsmeow.Client,
	jid types.JID,
	msg *waE2E.Message,
	extra ...whatsmeow.SendRequestExtra,
) (whatsmeow.SendResponse, error) {
	ctx := context.Background()
	_ = client.SendChatPresence(ctx, jid, types.ChatPresenceComposing, types.ChatPresenceMediaText)
	time.Sleep(typingDelayForMessage(msg))

	var resp whatsmeow.SendResponse
	var err error
	if len(extra) > 0 {
		resp, err = client.SendMessage(ctx, jid, msg, extra...)
	} else {
		resp, err = client.SendMessage(ctx, jid, msg)
	}
	_ = client.SendChatPresence(ctx, jid, types.ChatPresencePaused, types.ChatPresenceMediaText)
	if err == nil && s != nil && !jid.IsEmpty() {
		if jid.Server == types.DefaultUserServer {
			s.rememberPeerJID(jid.User, jid)
		}
	}
	return resp, err
}
