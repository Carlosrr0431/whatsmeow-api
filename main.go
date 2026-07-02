package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/skip2/go-qrcode"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
)

type App struct {
	client     *whatsmeow.Client
	container  *sqlstore.Container
	qrCode     string
	qrChan     <-chan whatsmeow.QRChannelItem
	mu         sync.RWMutex
	connected  bool
	messages   []MessageEvent
	msgMu      sync.RWMutex
	maxMsgHist int
}

type MessageEvent struct {
	ID        string `json:"id"`
	From      string `json:"from"`
	To        string `json:"to"`
	Body      string `json:"body"`
	Type      string `json:"type"`
	Timestamp int64  `json:"timestamp"`
	IsGroup   bool   `json:"is_group"`
	IsFromMe  bool   `json:"is_from_me"`
	PushName  string `json:"push_name"`
}

type APIResponse struct {
	Success bool        `json:"success"`
	Message string      `json:"message,omitempty"`
	Data    interface{} `json:"data,omitempty"`
}

type SendMessageRequest struct {
	Phone   string `json:"phone"`
	Message string `json:"message"`
}

type SendImageRequest struct {
	Phone   string `json:"phone"`
	Image   string `json:"image"`
	Caption string `json:"caption"`
}

func NewApp() (*App, error) {
	dbPath := os.Getenv("DB_PATH")
	if dbPath == "" {
		dbPath = "file:whatsapp.db?_foreign_keys=on"
	}

	dbLog := waLog.Stdout("Database", "WARN", true)
	container, err := sqlstore.New(context.Background(), "sqlite3", dbPath, dbLog)
	if err != nil {
		return nil, fmt.Errorf("failed to create store: %v", err)
	}

	return &App{
		container:  container,
		maxMsgHist: 1000,
		messages:   make([]MessageEvent, 0),
	}, nil
}

func (app *App) eventHandler(evt interface{}) {
	switch v := evt.(type) {
	case *events.Message:
		if v.Message == nil {
			break
		}
		if v.Message.GetProtocolMessage() != nil || v.Message.GetSenderKeyDistributionMessage() != nil {
			break
		}

		senderStr := v.Info.Sender.String()
		chatStr := v.Info.Chat.String()
		// Use SenderAlt/RecipientAlt for LID resolution
		if v.Info.SenderAlt.User != "" {
			senderStr = v.Info.SenderAlt.String()
		}
		if v.Info.Chat.Server == "lid" && v.Info.RecipientAlt.User != "" {
			chatStr = v.Info.RecipientAlt.String()
		}

		msg := MessageEvent{
			ID:        v.Info.ID,
			From:      senderStr,
			To:        chatStr,
			Timestamp: v.Info.Timestamp.Unix(),
			IsGroup:   v.Info.IsGroup,
			IsFromMe:  v.Info.IsFromMe,
			PushName:  v.Info.PushName,
		}
		app.extractMessageContent(v.Message, &msg)
		if msg.Type == "" {
			msg.Type = "unknown"
		}
		fmt.Printf("[MSG] from=%s to=%s isFromMe=%v type=%s\n", msg.From, msg.To, msg.IsFromMe, msg.Type)

		app.msgMu.Lock()
		app.messages = append(app.messages, msg)
		if len(app.messages) > app.maxMsgHist {
			app.messages = app.messages[len(app.messages)-app.maxMsgHist:]
		}
		app.msgMu.Unlock()

	case *events.Connected:
		app.mu.Lock()
		app.connected = true
		app.mu.Unlock()
		fmt.Println("✓ WhatsApp connected")

	case *events.Disconnected:
		app.mu.Lock()
		app.connected = false
		app.mu.Unlock()
		fmt.Println("✗ WhatsApp disconnected")

	case *events.LoggedOut:
		app.mu.Lock()
		app.connected = false
		app.client = nil
		app.mu.Unlock()
		fmt.Println("✗ WhatsApp logged out")

	case *events.HistorySync:
		if v.Data == nil {
			break
		}
		count := 0
		for _, conv := range v.Data.GetConversations() {
			chatJID, err := types.ParseJID(conv.GetId())
			if err != nil {
				continue
			}
			for _, histMsg := range conv.GetMessages() {
				evt, err := app.client.ParseWebMessage(chatJID, histMsg.GetMessage())
				if err != nil || evt == nil {
					continue
				}
				if evt.Message == nil || evt.Message.GetProtocolMessage() != nil || evt.Message.GetSenderKeyDistributionMessage() != nil {
					continue
				}
				msg := MessageEvent{
					ID:        evt.Info.ID,
					From:      evt.Info.Sender.String(),
					To:        evt.Info.Chat.String(),
					Timestamp: evt.Info.Timestamp.Unix(),
					IsGroup:   evt.Info.IsGroup,
					IsFromMe:  evt.Info.IsFromMe,
					PushName:  evt.Info.PushName,
				}
				app.extractMessageContent(evt.Message, &msg)
				if msg.Type == "" {
					msg.Type = "unknown"
				}
				app.msgMu.Lock()
				app.messages = append(app.messages, msg)
				app.msgMu.Unlock()
				count++
			}
		}
		app.msgMu.Lock()
		if len(app.messages) > app.maxMsgHist {
			app.messages = app.messages[len(app.messages)-app.maxMsgHist:]
		}
		app.msgMu.Unlock()
		fmt.Printf("[HISTORY_SYNC] Loaded %d messages from history\n", count)
	}
}

func (app *App) handleStatus(w http.ResponseWriter, r *http.Request) {
	app.mu.RLock()
	defer app.mu.RUnlock()

	status := "disconnected"
	phone := ""
	if app.connected && app.client != nil {
		status = "connected"
		if app.client.Store.ID != nil {
			phone = app.client.Store.ID.User
		}
	}

	writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Data: map[string]interface{}{
			"status": status,
			"phone":  phone,
		},
	})
}

func (app *App) handleConnect(w http.ResponseWriter, r *http.Request) {
	app.mu.Lock()
	defer app.mu.Unlock()

	if app.connected && app.client != nil {
		writeJSON(w, http.StatusOK, APIResponse{
			Success: true,
			Message: "Already connected",
		})
		return
	}

	deviceStore, err := app.container.GetFirstDevice(context.Background())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, APIResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to get device: %v", err),
		})
		return
	}

	clientLog := waLog.Stdout("Client", "WARN", true)
	client := whatsmeow.NewClient(deviceStore, clientLog)
	client.AddEventHandler(app.eventHandler)
	app.client = client

	if client.Store.ID == nil {
		qrChan, _ := client.GetQRChannel(context.Background())
		err = client.Connect()
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, APIResponse{
				Success: false,
				Message: fmt.Sprintf("Failed to connect: %v", err),
			})
			return
		}

		app.qrChan = qrChan

		go func() {
			for evt := range qrChan {
				if evt.Event == "code" {
					app.mu.Lock()
					app.qrCode = evt.Code
					app.mu.Unlock()
					fmt.Printf("QR code updated: %s\n", evt.Code[:20]+"...")
				} else {
					app.mu.Lock()
					app.qrCode = ""
					app.mu.Unlock()
					fmt.Printf("QR channel event: %s\n", evt.Event)
				}
			}
		}()

		writeJSON(w, http.StatusOK, APIResponse{
			Success: true,
			Message: "QR code generated. Use GET /api/session/qr to retrieve it.",
		})
	} else {
		err = client.Connect()
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, APIResponse{
				Success: false,
				Message: fmt.Sprintf("Failed to connect: %v", err),
			})
			return
		}

		writeJSON(w, http.StatusOK, APIResponse{
			Success: true,
			Message: "Reconnecting with existing session",
		})
	}
}

func (app *App) handleGetQR(w http.ResponseWriter, r *http.Request) {
	app.mu.RLock()
	qr := app.qrCode
	app.mu.RUnlock()

	if qr == "" {
		writeJSON(w, http.StatusOK, APIResponse{
			Success: false,
			Message: "No QR code available. Either already connected or not initialized.",
		})
		return
	}

	format := r.URL.Query().Get("format")

	if format == "image" {
		png, err := qrcode.Encode(qr, qrcode.Medium, 512)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, APIResponse{
				Success: false,
				Message: "Failed to generate QR image",
			})
			return
		}
		w.Header().Set("Content-Type", "image/png")
		w.Write(png)
		return
	}

	png, _ := qrcode.Encode(qr, qrcode.Medium, 512)
	b64 := base64.StdEncoding.EncodeToString(png)

	writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Data: map[string]interface{}{
			"qr_code":    qr,
			"qr_image":   "data:image/png;base64," + b64,
			"expires_in": "60s",
		},
	})
}

func (app *App) handleDisconnect(w http.ResponseWriter, r *http.Request) {
	app.mu.Lock()
	defer app.mu.Unlock()

	if app.client == nil {
		writeJSON(w, http.StatusOK, APIResponse{
			Success: true,
			Message: "Not connected",
		})
		return
	}

	app.client.Disconnect()
	app.connected = false

	writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Message: "Disconnected successfully",
	})
}

func (app *App) handleLogout(w http.ResponseWriter, r *http.Request) {
	app.mu.Lock()
	defer app.mu.Unlock()

	if app.client == nil {
		writeJSON(w, http.StatusOK, APIResponse{
			Success: false,
			Message: "Not connected",
		})
		return
	}

	err := app.client.Logout(context.Background())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, APIResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to logout: %v", err),
		})
		return
	}

	app.client = nil
	app.connected = false

	writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Message: "Logged out successfully. Session deleted.",
	})
}

func (app *App) handleSendMessage(w http.ResponseWriter, r *http.Request) {
	app.mu.RLock()
	if !app.connected || app.client == nil {
		app.mu.RUnlock()
		writeJSON(w, http.StatusBadRequest, APIResponse{
			Success: false,
			Message: "WhatsApp not connected",
		})
		return
	}
	app.mu.RUnlock()

	var req SendMessageRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, APIResponse{
			Success: false,
			Message: "Invalid request body",
		})
		return
	}

	if req.Phone == "" || req.Message == "" {
		writeJSON(w, http.StatusBadRequest, APIResponse{
			Success: false,
			Message: "phone and message are required",
		})
		return
	}

	jid := parseJID(req.Phone)
	msg := &waE2E.Message{
		Conversation: proto.String(req.Message),
	}

	resp, err := app.client.SendMessage(context.Background(), jid, msg)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, APIResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to send message: %v", err),
		})
		return
	}

	app.storeSentMessage(resp.ID, jid.String(), req.Message, "text", resp.Timestamp.Unix())

	writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Message: "Message sent",
		Data: map[string]interface{}{
			"message_id": resp.ID,
			"timestamp":  resp.Timestamp.Unix(),
		},
	})
}

func (app *App) handleSendImage(w http.ResponseWriter, r *http.Request) {
	app.mu.RLock()
	if !app.connected || app.client == nil {
		app.mu.RUnlock()
		writeJSON(w, http.StatusBadRequest, APIResponse{
			Success: false,
			Message: "WhatsApp not connected",
		})
		return
	}
	app.mu.RUnlock()

	var req SendImageRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, APIResponse{
			Success: false,
			Message: "Invalid request body",
		})
		return
	}

	if req.Phone == "" || req.Image == "" {
		writeJSON(w, http.StatusBadRequest, APIResponse{
			Success: false,
			Message: "phone and image (base64) are required",
		})
		return
	}

	imageData, err := base64.StdEncoding.DecodeString(req.Image)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, APIResponse{
			Success: false,
			Message: "Invalid base64 image",
		})
		return
	}

	uploaded, err := app.client.Upload(context.Background(), imageData, whatsmeow.MediaImage)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, APIResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to upload image: %v", err),
		})
		return
	}

	jid := parseJID(req.Phone)
	msg := &waE2E.Message{
		ImageMessage: &waE2E.ImageMessage{
			Caption:       proto.String(req.Caption),
			URL:           proto.String(uploaded.URL),
			DirectPath:    proto.String(uploaded.DirectPath),
			MediaKey:      uploaded.MediaKey,
			Mimetype:      proto.String("image/jpeg"),
			FileEncSHA256: uploaded.FileEncSHA256,
			FileSHA256:    uploaded.FileSHA256,
			FileLength:    proto.Uint64(uint64(len(imageData))),
		},
	}

	resp, err := app.client.SendMessage(context.Background(), jid, msg)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, APIResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to send image: %v", err),
		})
		return
	}

	app.storeSentMessage(resp.ID, jid.String(), req.Caption, "image", resp.Timestamp.Unix())

	writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Message: "Image sent",
		Data: map[string]interface{}{
			"message_id": resp.ID,
			"timestamp":  resp.Timestamp.Unix(),
		},
	})
}

func (app *App) handleGetContacts(w http.ResponseWriter, r *http.Request) {
	app.mu.RLock()
	if !app.connected || app.client == nil {
		app.mu.RUnlock()
		writeJSON(w, http.StatusBadRequest, APIResponse{
			Success: false,
			Message: "WhatsApp not connected",
		})
		return
	}
	app.mu.RUnlock()

	contacts, err := app.client.Store.Contacts.GetAllContacts(context.Background())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, APIResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to get contacts: %v", err),
		})
		return
	}

	type ContactInfo struct {
		JID      string `json:"jid"`
		Name     string `json:"name"`
		FullName string `json:"full_name"`
		Phone    string `json:"phone"`
	}

	contactList := make([]ContactInfo, 0, len(contacts))
	for jid, info := range contacts {
		contactList = append(contactList, ContactInfo{
			JID:      jid.String(),
			Name:     info.PushName,
			FullName: info.FullName,
			Phone:    jid.User,
		})
	}

	writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Data: map[string]interface{}{
			"contacts": contactList,
			"total":    len(contactList),
		},
	})
}

func (app *App) handleGetGroups(w http.ResponseWriter, r *http.Request) {
	app.mu.RLock()
	if !app.connected || app.client == nil {
		app.mu.RUnlock()
		writeJSON(w, http.StatusBadRequest, APIResponse{
			Success: false,
			Message: "WhatsApp not connected",
		})
		return
	}
	app.mu.RUnlock()

	groups, err := app.client.GetJoinedGroups(context.Background())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, APIResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to get groups: %v", err),
		})
		return
	}

	type GroupInfo struct {
		JID          string `json:"jid"`
		Name         string `json:"name"`
		Topic        string `json:"topic"`
		Participants int    `json:"participants"`
		CreatedAt    int64  `json:"created_at"`
	}

	groupList := make([]GroupInfo, 0, len(groups))
	for _, g := range groups {
		groupList = append(groupList, GroupInfo{
			JID:          g.JID.String(),
			Name:         g.Name,
			Topic:        g.Topic,
			Participants: len(g.Participants),
			CreatedAt:    g.GroupCreated.Unix(),
		})
	}

	writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Data: map[string]interface{}{
			"groups": groupList,
			"total":  len(groupList),
		},
	})
}

func (app *App) handleGetMessages(w http.ResponseWriter, r *http.Request) {
	app.msgMu.RLock()
	defer app.msgMu.RUnlock()

	chatFilter := r.URL.Query().Get("chat")
	if chatFilter != "" {
		normalizedFilter := strings.Split(chatFilter, "@")[0]

		// Build a set of JID strings that represent this contact
		matchJIDs := map[string]bool{
			normalizedFilter:                       true,
			normalizedFilter + "@s.whatsapp.net":   true,
			normalizedFilter + "@lid":              true,
		}

		// Try to resolve LID<->phone mapping
		if app.client != nil && app.client.Store != nil {
			phoneJID := types.NewJID(normalizedFilter, types.DefaultUserServer)
			lid, err := app.client.Store.LIDs.GetLIDForPN(context.Background(), phoneJID)
			if err == nil && !lid.IsEmpty() {
				matchJIDs[lid.String()] = true
				matchJIDs[lid.User] = true
			}
		}

		filtered := make([]MessageEvent, 0)
		for _, msg := range app.messages {
			fromUser := strings.Split(msg.From, "@")[0]
			toUser := strings.Split(msg.To, "@")[0]
			if matchJIDs[fromUser] || matchJIDs[toUser] ||
				matchJIDs[msg.From] || matchJIDs[msg.To] ||
				strings.Contains(msg.From, normalizedFilter) || strings.Contains(msg.To, normalizedFilter) {
				filtered = append(filtered, msg)
			}
		}
		writeJSON(w, http.StatusOK, APIResponse{
			Success: true,
			Data: map[string]interface{}{
				"messages":        filtered,
				"total":           len(filtered),
				"chat":            chatFilter,
				"total_in_memory": len(app.messages),
			},
		})
		return
	}

	writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Data: map[string]interface{}{
			"messages": app.messages,
			"total":    len(app.messages),
		},
	})
}

func (app *App) handleGetProfilePic(w http.ResponseWriter, r *http.Request) {
	app.mu.RLock()
	if !app.connected || app.client == nil {
		app.mu.RUnlock()
		writeJSON(w, http.StatusBadRequest, APIResponse{
			Success: false,
			Message: "WhatsApp not connected",
		})
		return
	}
	app.mu.RUnlock()

	phone := r.URL.Query().Get("phone")
	if phone == "" {
		writeJSON(w, http.StatusBadRequest, APIResponse{
			Success: false,
			Message: "phone parameter is required",
		})
		return
	}

	jid := parseJID(phone)
	pic, err := app.client.GetProfilePictureInfo(context.Background(), jid, &whatsmeow.GetProfilePictureParams{})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, APIResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to get profile picture: %v", err),
		})
		return
	}

	if pic == nil {
		writeJSON(w, http.StatusOK, APIResponse{
			Success: true,
			Data: map[string]interface{}{
				"url": "",
			},
		})
		return
	}

	writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Data: map[string]interface{}{
			"url": pic.URL,
			"id":  pic.ID,
		},
	})
}

func (app *App) handleCheckNumber(w http.ResponseWriter, r *http.Request) {
	app.mu.RLock()
	if !app.connected || app.client == nil {
		app.mu.RUnlock()
		writeJSON(w, http.StatusBadRequest, APIResponse{
			Success: false,
			Message: "WhatsApp not connected",
		})
		return
	}
	app.mu.RUnlock()

	phone := r.URL.Query().Get("phone")
	if phone == "" {
		writeJSON(w, http.StatusBadRequest, APIResponse{
			Success: false,
			Message: "phone parameter is required",
		})
		return
	}

	phones := []string{phone}
	resp, err := app.client.IsOnWhatsApp(context.Background(), phones)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, APIResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to check number: %v", err),
		})
		return
	}

	if len(resp) == 0 {
		writeJSON(w, http.StatusOK, APIResponse{
			Success: true,
			Data: map[string]interface{}{
				"registered": false,
			},
		})
		return
	}

	writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Data: map[string]interface{}{
			"registered": resp[0].IsIn,
			"jid":        resp[0].JID.String(),
		},
	})
}

func (app *App) handleSendGroupMessage(w http.ResponseWriter, r *http.Request) {
	app.mu.RLock()
	if !app.connected || app.client == nil {
		app.mu.RUnlock()
		writeJSON(w, http.StatusBadRequest, APIResponse{
			Success: false,
			Message: "WhatsApp not connected",
		})
		return
	}
	app.mu.RUnlock()

	var req struct {
		GroupJID string `json:"group_jid"`
		Message  string `json:"message"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, APIResponse{
			Success: false,
			Message: "Invalid request body",
		})
		return
	}

	if req.GroupJID == "" || req.Message == "" {
		writeJSON(w, http.StatusBadRequest, APIResponse{
			Success: false,
			Message: "group_jid and message are required",
		})
		return
	}

	jid, err := types.ParseJID(req.GroupJID)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, APIResponse{
			Success: false,
			Message: fmt.Sprintf("Invalid group JID: %v", err),
		})
		return
	}

	msg := &waE2E.Message{
		Conversation: proto.String(req.Message),
	}

	resp, err := app.client.SendMessage(context.Background(), jid, msg)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, APIResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to send message: %v", err),
		})
		return
	}

	app.storeSentMessage(resp.ID, jid.String(), req.Message, "text", resp.Timestamp.Unix())

	writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Message: "Group message sent",
		Data: map[string]interface{}{
			"message_id": resp.ID,
			"timestamp":  resp.Timestamp.Unix(),
		},
	})
}

func (app *App) extractMessageContent(msg *waE2E.Message, evt *MessageEvent) {
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
		evt.Type = "image"
		evt.Body = msg.GetImageMessage().GetCaption()
	case msg.GetVideoMessage() != nil:
		evt.Type = "video"
		evt.Body = msg.GetVideoMessage().GetCaption()
	case msg.GetDocumentMessage() != nil:
		evt.Type = "document"
		evt.Body = msg.GetDocumentMessage().GetFileName()
	case msg.GetAudioMessage() != nil:
		evt.Type = "audio"
		if msg.GetAudioMessage().GetPTT() {
			evt.Type = "ptt"
		}
	case msg.GetStickerMessage() != nil:
		evt.Type = "sticker"
	case msg.GetContactMessage() != nil:
		evt.Type = "contact"
		evt.Body = msg.GetContactMessage().GetDisplayName()
	case msg.GetLocationMessage() != nil:
		evt.Type = "location"
		loc := msg.GetLocationMessage()
		evt.Body = fmt.Sprintf("📍 %.6f, %.6f", loc.GetDegreesLatitude(), loc.GetDegreesLongitude())
	}
}

func (app *App) storeSentMessage(id string, toJID string, body string, msgType string, timestamp int64) {
	myJID := ""
	if app.client != nil && app.client.Store.ID != nil {
		myJID = app.client.Store.ID.String()
	}
	msg := MessageEvent{
		ID:        id,
		From:      myJID,
		To:        toJID,
		Body:      body,
		Type:      msgType,
		Timestamp: timestamp,
		IsGroup:   strings.Contains(toJID, "@g.us"),
		IsFromMe:  true,
		PushName:  "Yo",
	}
	app.msgMu.Lock()
	app.messages = append(app.messages, msg)
	if len(app.messages) > app.maxMsgHist {
		app.messages = app.messages[len(app.messages)-app.maxMsgHist:]
	}
	app.msgMu.Unlock()
}

// --- Helpers ---

func parseJID(phone string) types.JID {
	phone = strings.TrimPrefix(phone, "+")
	phone = strings.ReplaceAll(phone, " ", "")
	phone = strings.ReplaceAll(phone, "-", "")

	if strings.Contains(phone, "@") {
		jid, _ := types.ParseJID(phone)
		return jid
	}

	return types.NewJID(phone, types.DefaultUserServer)
}

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-API-Key")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func authMiddleware(next http.Handler) http.Handler {
	apiKey := os.Getenv("API_KEY")
	if apiKey == "" {
		return next
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" || r.URL.Path == "/health" {
			next.ServeHTTP(w, r)
			return
		}

		key := r.Header.Get("X-API-Key")
		if key == "" {
			key = r.URL.Query().Get("api_key")
		}

		if key != apiKey {
			writeJSON(w, http.StatusUnauthorized, APIResponse{
				Success: false,
				Message: "Invalid or missing API key",
			})
			return
		}

		next.ServeHTTP(w, r)
	})
}

func main() {
	app, err := NewApp()
	if err != nil {
		fmt.Printf("Failed to initialize app: %v\n", err)
		os.Exit(1)
	}

	mux := http.NewServeMux()

	// Health check (public, no auth required)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, APIResponse{Success: true, Message: "ok"})
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, APIResponse{
			Success: true,
			Message: "WhatsApp API Service - Running",
			Data: map[string]interface{}{
				"version":   "1.0.0",
				"endpoints": []string{"/api/status", "/api/session/connect", "/api/session/qr", "/api/session/disconnect", "/api/session/logout", "/api/messages/send", "/api/messages/send-image", "/api/messages/send-group", "/api/messages/history", "/api/contacts", "/api/groups", "/api/check-number", "/api/profile-pic"},
			},
		})
	})

	// Session endpoints
	mux.HandleFunc("/api/status", app.handleStatus)
	mux.HandleFunc("/api/session/connect", app.handleConnect)
	mux.HandleFunc("/api/session/qr", app.handleGetQR)
	mux.HandleFunc("/api/session/disconnect", app.handleDisconnect)
	mux.HandleFunc("/api/session/logout", app.handleLogout)

	// Message endpoints
	mux.HandleFunc("/api/messages/send", app.handleSendMessage)
	mux.HandleFunc("/api/messages/send-image", app.handleSendImage)
	mux.HandleFunc("/api/messages/send-group", app.handleSendGroupMessage)
	mux.HandleFunc("/api/messages/history", app.handleGetMessages)

	// Contact & Group endpoints
	mux.HandleFunc("/api/contacts", app.handleGetContacts)
	mux.HandleFunc("/api/groups", app.handleGetGroups)

	// Utility endpoints
	mux.HandleFunc("/api/check-number", app.handleCheckNumber)
	mux.HandleFunc("/api/profile-pic", app.handleGetProfilePic)

	handler := corsMiddleware(authMiddleware(mux))

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// Auto-connect on startup if session exists
	go func() {
		time.Sleep(2 * time.Second)
		deviceStore, err := app.container.GetFirstDevice(context.Background())
		if err == nil && deviceStore.ID != nil {
			fmt.Println("Found existing session, auto-connecting...")
			clientLog := waLog.Stdout("Client", "WARN", true)
			client := whatsmeow.NewClient(deviceStore, clientLog)
			client.AddEventHandler(app.eventHandler)
			app.mu.Lock()
			app.client = client
			app.mu.Unlock()
			err = client.Connect()
			if err != nil {
				fmt.Printf("Auto-connect failed: %v\n", err)
			}
		}
	}()

	server := &http.Server{
		Addr:         ":" + port,
		Handler:      handler,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	fmt.Printf("🚀 WhatsApp API Server starting on port %s\n", port)
	fmt.Printf("📡 Endpoints available at http://localhost:%s/\n", port)

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("Server error: %v\n", err)
			os.Exit(1)
		}
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

	fmt.Println("\nShutting down...")
	app.mu.RLock()
	if app.client != nil {
		app.client.Disconnect()
	}
	app.mu.RUnlock()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	server.Shutdown(ctx)
	fmt.Println("Server stopped")
}
