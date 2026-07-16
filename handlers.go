package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/skip2/go-qrcode"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

type App struct {
	manager *SessionManager
}

func NewApp() (*App, error) {
	dataDir := strings.TrimSpace(os.Getenv("DATA_DIR"))
	if dataDir == "" {
		dataDir = "/app/data"
	}
	manager, err := NewSessionManager(dataDir, strings.TrimSpace(os.Getenv("WEBHOOK_SECRET")))
	if err != nil {
		return nil, err
	}
	return &App{manager: manager}, nil
}

func agentCodeFromQuery(r *http.Request) string {
	return strings.TrimSpace(r.URL.Query().Get("agent_code"))
}

func requireAgentCode(w http.ResponseWriter, r *http.Request) (string, bool) {
	code := agentCodeFromQuery(r)
	if code == "" {
		writeJSON(w, http.StatusBadRequest, APIResponse{
			Success: false,
			Message: "agent_code is required (query param or JSON body)",
		})
		return "", false
	}
	return code, true
}

func (app *App) sessionFromRequest(w http.ResponseWriter, r *http.Request, bodyAgentCode string) (*AgentSession, bool) {
	code := agentCodeFromQuery(r)
	if code == "" {
		code = strings.TrimSpace(bodyAgentCode)
	}
	if code == "" {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "agent_code is required"})
		return nil, false
	}
	s, ok := app.manager.GetSession(code)
	if !ok {
		writeJSON(w, http.StatusNotFound, APIResponse{
			Success: false,
			Message: fmt.Sprintf("Session not registered for agent %s. Configure webhook first.", code),
		})
		return nil, false
	}
	return s, true
}

func (app *App) handleStatus(w http.ResponseWriter, r *http.Request) {
	code := agentCodeFromQuery(r)
	if code == "" {
		sessions := app.manager.ListStatus()
		writeJSON(w, http.StatusOK, APIResponse{
			Success: true,
			Data: map[string]interface{}{
				"sessions": sessions,
				"total":    len(sessions),
			},
		})
		return
	}

	s, ok := app.manager.GetSession(code)
	if !ok {
		writeJSON(w, http.StatusNotFound, APIResponse{Success: false, Message: "Session not found"})
		return
	}
	info := s.StatusInfo()
	writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Data: map[string]interface{}{
			"status":      info.Status,
			"phone":       info.Phone,
			"has_session": info.HasSession,
			"agent_code":  info.AgentCode,
			"webhook_url": info.WebhookURL,
		},
	})
}

func (app *App) handleConnect(w http.ResponseWriter, r *http.Request) {
	var req struct {
		AgentCode     string `json:"agent_code"`
		WebhookURL    string `json:"webhook_url"`
		WebhookSecret string `json:"webhook_secret"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	if req.AgentCode == "" {
		req.AgentCode = agentCodeFromQuery(r)
	}
	if req.AgentCode == "" {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "agent_code is required"})
		return
	}

	if _, err := app.manager.EnsureSession(req.AgentCode, req.WebhookURL, req.WebhookSecret); err != nil {
		writeJSON(w, http.StatusInternalServerError, APIResponse{Success: false, Message: err.Error()})
		return
	}

	s, ok := app.manager.GetSession(req.AgentCode)
	if !ok {
		writeJSON(w, http.StatusInternalServerError, APIResponse{Success: false, Message: "Failed to init session"})
		return
	}

	needsQR, err := s.Connect()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, APIResponse{Success: false, Message: fmt.Sprintf("Failed to connect: %v", err)})
		return
	}

	msg := "Reconnecting with existing session"
	if needsQR {
		msg = "QR code generated. Use GET /api/session/qr?agent_code=" + req.AgentCode
	}
	writeJSON(w, http.StatusOK, APIResponse{Success: true, Message: msg, Data: map[string]interface{}{"needs_qr": needsQR}})
}

func (app *App) handleGetQR(w http.ResponseWriter, r *http.Request) {
	code, ok := requireAgentCode(w, r)
	if !ok {
		return
	}
	s, ok := app.manager.GetSession(code)
	if !ok {
		writeJSON(w, http.StatusNotFound, APIResponse{Success: false, Message: "Session not found"})
		return
	}

	qr := s.GetQR()
	if qr == "" {
		writeJSON(w, http.StatusOK, APIResponse{Success: false, Message: "No QR code available"})
		return
	}

	if r.URL.Query().Get("format") == "image" {
		png, err := qrcode.Encode(qr, qrcode.Medium, 512)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, APIResponse{Success: false, Message: "Failed to generate QR image"})
			return
		}
		w.Header().Set("Content-Type", "image/png")
		w.Write(png)
		return
	}

	png, _ := qrcode.Encode(qr, qrcode.Medium, 512)
	writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Data: map[string]interface{}{
			"qr_code":    qr,
			"qr_image":   "data:image/png;base64," + base64.StdEncoding.EncodeToString(png),
			"agent_code": code,
			"expires_in": "60s",
		},
	})
}

func (app *App) handleDisconnect(w http.ResponseWriter, r *http.Request) {
	var req struct {
		AgentCode string `json:"agent_code"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	if req.AgentCode == "" {
		req.AgentCode = agentCodeFromQuery(r)
	}
	s, ok := app.sessionFromRequest(w, r, req.AgentCode)
	if !ok {
		return
	}
	s.Disconnect()
	writeJSON(w, http.StatusOK, APIResponse{Success: true, Message: "Disconnected successfully"})
}

func (app *App) handleLogout(w http.ResponseWriter, r *http.Request) {
	var req struct {
		AgentCode string `json:"agent_code"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	if req.AgentCode == "" {
		req.AgentCode = agentCodeFromQuery(r)
	}
	s, ok := app.sessionFromRequest(w, r, req.AgentCode)
	if !ok {
		return
	}
	if err := s.Logout(context.Background()); err != nil {
		writeJSON(w, http.StatusInternalServerError, APIResponse{Success: false, Message: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, APIResponse{Success: true, Message: "Logged out successfully. Session deleted."})
}

func (app *App) handleSendMessage(w http.ResponseWriter, r *http.Request) {
	var req struct {
		AgentCode string `json:"agent_code"`
		Phone     string `json:"phone"`
		Message   string `json:"message"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "Invalid request body"})
		return
	}
	s, ok := app.sessionFromRequest(w, r, req.AgentCode)
	if !ok {
		return
	}
	client, connected := s.Client()
	if !connected {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "WhatsApp not connected for this agent"})
		return
	}
	if req.Phone == "" || req.Message == "" {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "phone and message are required"})
		return
	}

	jid := parseJID(req.Phone)
	msg := &waE2E.Message{Conversation: proto.String(req.Message)}
	resp, err := client.SendMessage(context.Background(), jid, msg)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, APIResponse{Success: false, Message: err.Error()})
		return
	}
	s.storeSentMessage(resp.ID, jid.String(), req.Message, "text", resp.Timestamp.Unix())
	writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Message: "Message sent",
		Data:    map[string]interface{}{"message_id": resp.ID, "timestamp": resp.Timestamp.Unix()},
	})
}

func (app *App) handleSendImage(w http.ResponseWriter, r *http.Request) {
	var req struct {
		AgentCode string `json:"agent_code"`
		Phone     string `json:"phone"`
		Image     string `json:"image"`
		Caption   string `json:"caption"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "Invalid request body"})
		return
	}
	app.sendMediaInternal(w, r, mediaSendRequest{
		AgentCode: req.AgentCode,
		Phone:     req.Phone,
		Media:     req.Image,
		Caption:   req.Caption,
		Type:      "image",
		Mimetype:  "image/jpeg",
	})
}

type mediaSendRequest struct {
	AgentCode string
	Phone     string
	Media     string
	Caption   string
	Type      string
	Mimetype  string
	Filename  string
}

func (app *App) handleSendMedia(w http.ResponseWriter, r *http.Request) {
	var req struct {
		AgentCode string `json:"agent_code"`
		Phone     string `json:"phone"`
		Media     string `json:"media"`
		Image     string `json:"image"` // alias compat send-image
		Caption   string `json:"caption"`
		Type      string `json:"type"`
		Mimetype  string `json:"mimetype"`
		Filename  string `json:"filename"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "Invalid request body"})
		return
	}
	media := req.Media
	if media == "" {
		media = req.Image
	}
	app.sendMediaInternal(w, r, mediaSendRequest{
		AgentCode: req.AgentCode,
		Phone:     req.Phone,
		Media:     media,
		Caption:   req.Caption,
		Type:      req.Type,
		Mimetype:  req.Mimetype,
		Filename:  req.Filename,
	})
}

func (app *App) sendMediaInternal(w http.ResponseWriter, r *http.Request, req mediaSendRequest) {
	s, ok := app.sessionFromRequest(w, r, req.AgentCode)
	if !ok {
		return
	}
	client, connected := s.Client()
	if !connected {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "WhatsApp not connected for this agent"})
		return
	}
	if req.Phone == "" || req.Media == "" {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "phone and media are required"})
		return
	}

	mediaType := strings.ToLower(strings.TrimSpace(req.Type))
	mime := strings.TrimSpace(req.Mimetype)
	if mediaType == "" {
		switch {
		case strings.HasPrefix(mime, "image/"):
			mediaType = "image"
		case strings.HasPrefix(mime, "video/"):
			mediaType = "video"
		case strings.HasPrefix(mime, "audio/"):
			mediaType = "audio"
		default:
			mediaType = "document"
		}
	}

	data, err := base64.StdEncoding.DecodeString(req.Media)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "Invalid base64 media"})
		return
	}

	var uploadType whatsmeow.MediaType
	switch mediaType {
	case "image":
		uploadType = whatsmeow.MediaImage
		if mime == "" {
			mime = "image/jpeg"
		}
	case "video":
		uploadType = whatsmeow.MediaVideo
		if mime == "" {
			mime = "video/mp4"
		}
	case "audio":
		uploadType = whatsmeow.MediaAudio
		if mime == "" {
			mime = "audio/ogg; codecs=opus"
		}
	default:
		mediaType = "document"
		uploadType = whatsmeow.MediaDocument
		if mime == "" {
			mime = "application/octet-stream"
		}
	}

	uploaded, err := client.Upload(context.Background(), data, uploadType)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, APIResponse{Success: false, Message: err.Error()})
		return
	}

	jid := parseJID(req.Phone)
	var msg *waE2E.Message
	switch mediaType {
	case "image":
		msg = &waE2E.Message{
			ImageMessage: &waE2E.ImageMessage{
				Caption:       proto.String(req.Caption),
				URL:           proto.String(uploaded.URL),
				DirectPath:    proto.String(uploaded.DirectPath),
				MediaKey:      uploaded.MediaKey,
				Mimetype:      proto.String(mime),
				FileEncSHA256: uploaded.FileEncSHA256,
				FileSHA256:    uploaded.FileSHA256,
				FileLength:    proto.Uint64(uint64(len(data))),
			},
		}
	case "video":
		msg = &waE2E.Message{
			VideoMessage: &waE2E.VideoMessage{
				Caption:       proto.String(req.Caption),
				URL:           proto.String(uploaded.URL),
				DirectPath:    proto.String(uploaded.DirectPath),
				MediaKey:      uploaded.MediaKey,
				Mimetype:      proto.String(mime),
				FileEncSHA256: uploaded.FileEncSHA256,
				FileSHA256:    uploaded.FileSHA256,
				FileLength:    proto.Uint64(uint64(len(data))),
			},
		}
	case "audio":
		msg = &waE2E.Message{
			AudioMessage: &waE2E.AudioMessage{
				URL:           proto.String(uploaded.URL),
				DirectPath:    proto.String(uploaded.DirectPath),
				MediaKey:      uploaded.MediaKey,
				Mimetype:      proto.String(mime),
				FileEncSHA256: uploaded.FileEncSHA256,
				FileSHA256:    uploaded.FileSHA256,
				FileLength:    proto.Uint64(uint64(len(data))),
			},
		}
	default:
		fileName := req.Filename
		if fileName == "" {
			fileName = "archivo"
		}
		msg = &waE2E.Message{
			DocumentMessage: &waE2E.DocumentMessage{
				Caption:       proto.String(req.Caption),
				Title:         proto.String(fileName),
				FileName:      proto.String(fileName),
				URL:           proto.String(uploaded.URL),
				DirectPath:    proto.String(uploaded.DirectPath),
				MediaKey:      uploaded.MediaKey,
				Mimetype:      proto.String(mime),
				FileEncSHA256: uploaded.FileEncSHA256,
				FileSHA256:    uploaded.FileSHA256,
				FileLength:    proto.Uint64(uint64(len(data))),
			},
		}
	}

	resp, err := client.SendMessage(context.Background(), jid, msg)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, APIResponse{Success: false, Message: err.Error()})
		return
	}

	body := req.Caption
	if body == "" {
		body = req.Filename
	}
	s.storeSentMessage(resp.ID, jid.String(), body, mediaType, resp.Timestamp.Unix())
	writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Message: "Media sent",
		Data: map[string]interface{}{
			"message_id": resp.ID,
			"timestamp":  resp.Timestamp.Unix(),
			"type":       mediaType,
		},
	})
}

func (app *App) handleGetContacts(w http.ResponseWriter, r *http.Request) {
	code, ok := requireAgentCode(w, r)
	if !ok {
		return
	}
	s, ok := app.manager.GetSession(code)
	if !ok {
		writeJSON(w, http.StatusNotFound, APIResponse{Success: false, Message: "Session not found"})
		return
	}
	client, connected := s.Client()
	if !connected {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "WhatsApp not connected"})
		return
	}

	contacts, err := client.Store.Contacts.GetAllContacts(context.Background())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, APIResponse{Success: false, Message: err.Error()})
		return
	}

	type ContactInfo struct {
		JID      string `json:"jid"`
		Name     string `json:"name"`
		FullName string `json:"full_name"`
		Phone    string `json:"phone"`
		LID      string `json:"lid,omitempty"`
	}

	contactList := make([]ContactInfo, 0, len(contacts))
	seenPhones := make(map[string]bool)
	for jid, info := range contacts {
		phone := jid.User
		lid := ""
		displayJID := jid.String()
		if jid.Server == types.HiddenUserServer {
			lid = jid.User
			if pn, err := client.Store.LIDs.GetPNForLID(context.Background(), jid); err == nil && !pn.IsEmpty() {
				phone = pn.User
				displayJID = pn.String()
			}
		}
		if jid.Server == types.HiddenUserServer && seenPhones[phone] {
			continue
		}
		if phone != "" {
			seenPhones[phone] = true
		}
		contactList = append(contactList, ContactInfo{
			JID: displayJID, Name: info.PushName, FullName: info.FullName, Phone: phone, LID: lid,
		})
	}
	writeJSON(w, http.StatusOK, APIResponse{Success: true, Data: map[string]interface{}{"contacts": contactList, "total": len(contactList)}})
}

func (app *App) handleGetGroups(w http.ResponseWriter, r *http.Request) {
	code, ok := requireAgentCode(w, r)
	if !ok {
		return
	}
	s, ok := app.manager.GetSession(code)
	if !ok {
		writeJSON(w, http.StatusNotFound, APIResponse{Success: false, Message: "Session not found"})
		return
	}
	client, connected := s.Client()
	if !connected {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "WhatsApp not connected"})
		return
	}
	groups, err := client.GetJoinedGroups(context.Background())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, APIResponse{Success: false, Message: err.Error()})
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
			JID: g.JID.String(), Name: g.Name, Topic: g.Topic,
			Participants: len(g.Participants), CreatedAt: g.GroupCreated.Unix(),
		})
	}
	writeJSON(w, http.StatusOK, APIResponse{Success: true, Data: map[string]interface{}{"groups": groupList, "total": len(groupList)}})
}

func (app *App) handleGetMessages(w http.ResponseWriter, r *http.Request) {
	code, ok := requireAgentCode(w, r)
	if !ok {
		return
	}
	s, ok := app.manager.GetSession(code)
	if !ok {
		writeJSON(w, http.StatusNotFound, APIResponse{Success: false, Message: "Session not found"})
		return
	}
	msgs := s.Messages()
	writeJSON(w, http.StatusOK, APIResponse{Success: true, Data: map[string]interface{}{"messages": msgs, "total": len(msgs)}})
}

func (app *App) handleDownloadMedia(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, APIResponse{Success: false, Message: "Method not allowed"})
		return
	}
	code, ok := requireAgentCode(w, r)
	if !ok {
		return
	}
	s, ok := app.manager.GetSession(code)
	if !ok {
		writeJSON(w, http.StatusNotFound, APIResponse{Success: false, Message: "Session not found"})
		return
	}
	client, connected := s.Client()
	if !connected {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "WhatsApp not connected"})
		return
	}

	id := strings.TrimPrefix(r.URL.Path, "/api/messages/media/")
	if idx := strings.Index(id, "/"); idx >= 0 {
		id = id[:idx]
	}
	id = strings.TrimSpace(id)
	if id == "" {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "message id required"})
		return
	}

	raw, ok := s.getRawMedia(id)
	if !ok {
		if evt, found := s.getMessageByID(id); found {
			if rebuilt := messageEventToProto(evt); rebuilt != nil {
				raw = rebuilt
				ok = true
				fmt.Printf("[MEDIA][%s] id=%s fallback=message_history type=%s\n", code, id, evt.Type)
			}
		}
	}
	if !ok {
		writeJSON(w, http.StatusNotFound, APIResponse{Success: false, Message: "Media not found or expired from cache"})
		return
	}

	unwrapped := unwrapMessage(raw)
	ctx, cancel := context.WithTimeout(r.Context(), 90*time.Second)
	defer cancel()

	var (
		data []byte
		mime string
		err  error
	)

	switch {
	case unwrapped.GetImageMessage() != nil:
		im := unwrapped.GetImageMessage()
		data, err = client.Download(ctx, im)
		mime = im.GetMimetype()
	case unwrapped.GetVideoMessage() != nil:
		vm := unwrapped.GetVideoMessage()
		data, err = client.Download(ctx, vm)
		mime = vm.GetMimetype()
	case unwrapped.GetAudioMessage() != nil:
		am := unwrapped.GetAudioMessage()
		data, err = client.Download(ctx, am)
		mime = am.GetMimetype()
	case unwrapped.GetDocumentMessage() != nil:
		dm := unwrapped.GetDocumentMessage()
		data, err = client.Download(ctx, dm)
		mime = dm.GetMimetype()
	case unwrapped.GetStickerMessage() != nil:
		sm := unwrapped.GetStickerMessage()
		data, err = client.Download(ctx, sm)
		mime = sm.GetMimetype()
	default:
		writeJSON(w, http.StatusNotFound, APIResponse{Success: false, Message: "No downloadable media in message"})
		return
	}

	if err != nil {
		writeJSON(w, http.StatusInternalServerError, APIResponse{Success: false, Message: err.Error()})
		return
	}

	if mime == "" {
		switch r.URL.Query().Get("type") {
		case "image", "sticker":
			mime = "image/jpeg"
		case "video":
			mime = "video/mp4"
		case "audio", "ptt":
			mime = "audio/ogg"
		case "document":
			mime = "application/octet-stream"
		default:
			mime = http.DetectContentType(data)
		}
	}

	w.Header().Set("Content-Type", mime)
	w.Header().Set("Cache-Control", "private, max-age=3600")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func (app *App) handleGetProfilePic(w http.ResponseWriter, r *http.Request) {
	code, ok := requireAgentCode(w, r)
	if !ok {
		return
	}
	s, ok := app.manager.GetSession(code)
	if !ok {
		writeJSON(w, http.StatusNotFound, APIResponse{Success: false, Message: "Session not found"})
		return
	}
	client, connected := s.Client()
	if !connected {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "WhatsApp not connected"})
		return
	}
	phone := r.URL.Query().Get("phone")
	if phone == "" {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "phone parameter is required"})
		return
	}
	jid := parseJID(phone)
	pic, err := client.GetProfilePictureInfo(context.Background(), jid, &whatsmeow.GetProfilePictureParams{})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, APIResponse{Success: false, Message: err.Error()})
		return
	}
	if pic == nil {
		writeJSON(w, http.StatusOK, APIResponse{Success: true, Data: map[string]interface{}{"url": ""}})
		return
	}
	writeJSON(w, http.StatusOK, APIResponse{Success: true, Data: map[string]interface{}{"url": pic.URL, "id": pic.ID}})
}

func (app *App) handleCheckNumber(w http.ResponseWriter, r *http.Request) {
	code, ok := requireAgentCode(w, r)
	if !ok {
		return
	}
	s, ok := app.manager.GetSession(code)
	if !ok {
		writeJSON(w, http.StatusNotFound, APIResponse{Success: false, Message: "Session not found"})
		return
	}
	client, connected := s.Client()
	if !connected {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "WhatsApp not connected"})
		return
	}
	phone := r.URL.Query().Get("phone")
	if phone == "" {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "phone parameter is required"})
		return
	}
	resp, err := client.IsOnWhatsApp(context.Background(), []string{phone})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, APIResponse{Success: false, Message: err.Error()})
		return
	}
	if len(resp) == 0 {
		writeJSON(w, http.StatusOK, APIResponse{Success: true, Data: map[string]interface{}{"registered": false}})
		return
	}
	writeJSON(w, http.StatusOK, APIResponse{Success: true, Data: map[string]interface{}{"registered": resp[0].IsIn, "jid": resp[0].JID.String()}})
}

func (app *App) handleSendGroupMessage(w http.ResponseWriter, r *http.Request) {
	var req struct {
		AgentCode string `json:"agent_code"`
		GroupJID  string `json:"group_jid"`
		Message   string `json:"message"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "Invalid request body"})
		return
	}
	s, ok := app.sessionFromRequest(w, r, req.AgentCode)
	if !ok {
		return
	}
	client, connected := s.Client()
	if !connected {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "WhatsApp not connected"})
		return
	}
	jid, err := types.ParseJID(req.GroupJID)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "Invalid group JID"})
		return
	}
	msg := &waE2E.Message{Conversation: proto.String(req.Message)}
	resp, err := client.SendMessage(context.Background(), jid, msg)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, APIResponse{Success: false, Message: err.Error()})
		return
	}
	s.storeSentMessage(resp.ID, jid.String(), req.Message, "text", resp.Timestamp.Unix())
	writeJSON(w, http.StatusOK, APIResponse{Success: true, Data: map[string]interface{}{"message_id": resp.ID}})
}

func (app *App) handleWebhookConfig(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		code := agentCodeFromQuery(r)
		if code == "" {
			registry := app.manager.ListRegistry()
			sessions := app.manager.ListStatus()
			writeJSON(w, http.StatusOK, APIResponse{
				Success: true,
				Data: map[string]interface{}{
					"sessions": sessions,
					"registry": registry,
				},
			})
			return
		}
		s, ok := app.manager.GetSession(code)
		if !ok {
			entry := app.manager.ListRegistry()[code]
			if entry == nil {
				writeJSON(w, http.StatusNotFound, APIResponse{Success: false, Message: "Agent not registered"})
				return
			}
			writeJSON(w, http.StatusOK, APIResponse{
				Success: true,
				Data: map[string]interface{}{
					"agent_code":  entry.AgentCode,
					"webhook_url": entry.WebhookURL,
					"webhook_secret": func() string {
						if entry.WebhookSecret != "" {
							return "***"
						}
						return ""
					}(),
				},
			})
			return
		}
		info := s.StatusInfo()
		writeJSON(w, http.StatusOK, APIResponse{
			Success: true,
			Data: map[string]interface{}{
				"agent_code":  info.AgentCode,
				"webhook_url": info.WebhookURL,
				"webhook_secret": func() string {
					s.mu.RLock()
					defer s.mu.RUnlock()
					if s.WebhookSecret != "" {
						return "***"
					}
					return ""
				}(),
				"status": info.Status,
				"phone":  info.Phone,
			},
		})

	case http.MethodPost:
		var req struct {
			AgentCode     string `json:"agent_code"`
			WebhookURL    string `json:"webhook_url"`
			WebhookSecret string `json:"webhook_secret"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "Invalid JSON"})
			return
		}
		if req.AgentCode == "" {
			writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "agent_code is required"})
			return
		}
		if err := app.manager.UpdateWebhook(req.AgentCode, req.WebhookURL, req.WebhookSecret); err != nil {
			writeJSON(w, http.StatusInternalServerError, APIResponse{Success: false, Message: err.Error()})
			return
		}
		s, ok := app.manager.GetSession(req.AgentCode)
		data := map[string]interface{}{
			"agent_code":  req.AgentCode,
			"webhook_url": req.WebhookURL,
		}
		if ok && s != nil {
			info := s.StatusInfo()
			data["agent_code"] = info.AgentCode
			data["webhook_url"] = info.WebhookURL
		}
		writeJSON(w, http.StatusOK, APIResponse{
			Success: true,
			Message: "Webhook config updated",
			Data:    data,
		})

	default:
		writeJSON(w, http.StatusMethodNotAllowed, APIResponse{Success: false, Message: "Method not allowed"})
	}
}

func (app *App) handleRevokeMessage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, APIResponse{Success: false, Message: "Method not allowed"})
		return
	}
	var req struct {
		AgentCode string `json:"agent_code"`
		Phone     string `json:"phone"`
		MessageID string `json:"message_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "Invalid request body"})
		return
	}
	s, ok := app.sessionFromRequest(w, r, req.AgentCode)
	if !ok {
		return
	}
	client, connected := s.Client()
	if !connected {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "WhatsApp not connected"})
		return
	}
	if req.Phone == "" || req.MessageID == "" {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "phone and message_id are required"})
		return
	}
	jid := parseJID(req.Phone)
	_, err := client.SendMessage(context.Background(), jid, client.BuildRevoke(jid, types.EmptyJID, req.MessageID))
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, APIResponse{Success: false, Message: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, APIResponse{Success: true, Message: "Message revoked", Data: map[string]interface{}{"message_id": req.MessageID}})
}

func (app *App) handleEditMessage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, APIResponse{Success: false, Message: "Method not allowed"})
		return
	}
	var req struct {
		AgentCode string `json:"agent_code"`
		Phone     string `json:"phone"`
		MessageID string `json:"message_id"`
		Message   string `json:"message"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "Invalid request body"})
		return
	}
	s, ok := app.sessionFromRequest(w, r, req.AgentCode)
	if !ok {
		return
	}
	client, connected := s.Client()
	if !connected {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "WhatsApp not connected"})
		return
	}
	if req.Phone == "" || req.MessageID == "" || req.Message == "" {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "phone, message_id and message are required"})
		return
	}
	jid := parseJID(req.Phone)
	editMsg := client.BuildEdit(jid, req.MessageID, &waE2E.Message{Conversation: proto.String(req.Message)})
	_, err := client.SendMessage(context.Background(), jid, editMsg)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, APIResponse{Success: false, Message: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, APIResponse{Success: true, Message: "Message edited", Data: map[string]interface{}{"message_id": req.MessageID}})
}

func (app *App) handleSendReaction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, APIResponse{Success: false, Message: "Method not allowed"})
		return
	}
	var req struct {
		AgentCode string `json:"agent_code"`
		Phone     string `json:"phone"`
		MessageID string `json:"message_id"`
		Reaction  string `json:"reaction"`
		FromMe    bool   `json:"from_me"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "Invalid request body"})
		return
	}
	s, ok := app.sessionFromRequest(w, r, req.AgentCode)
	if !ok {
		return
	}
	client, connected := s.Client()
	if !connected {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "WhatsApp not connected"})
		return
	}
	if req.Phone == "" || req.MessageID == "" {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "phone and message_id are required"})
		return
	}
	jid := parseJID(req.Phone)
	sender := types.EmptyJID
	if req.FromMe {
		s.mu.RLock()
		if client.Store.ID != nil {
			sender = *client.Store.ID
		}
		s.mu.RUnlock()
	}
	reactionMsg := client.BuildReaction(jid, sender, req.MessageID, req.Reaction)
	_, err := client.SendMessage(context.Background(), jid, reactionMsg)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, APIResponse{Success: false, Message: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, APIResponse{Success: true, Message: "Reaction sent"})
}

func (app *App) handlePNFromLID(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, APIResponse{Success: false, Message: "Method not allowed"})
		return
	}
	code, ok := requireAgentCode(w, r)
	if !ok {
		return
	}
	s, ok := app.manager.GetSession(code)
	if !ok {
		writeJSON(w, http.StatusNotFound, APIResponse{Success: false, Message: "Session not found"})
		return
	}
	client, connected := s.Client()
	if !connected {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "WhatsApp not connected"})
		return
	}

	lidStr := strings.TrimPrefix(r.URL.Path, "/api/pn-from-lid/")
	lidStr = strings.Trim(lidStr, "/")
	if lidStr == "" {
		lidStr = r.URL.Query().Get("lid")
	}
	if lidStr == "" {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "lid is required"})
		return
	}

	jid, err := types.ParseJID(lidStr)
	if err != nil || jid.Server != types.HiddenUserServer {
		lidUser := strings.TrimSuffix(strings.TrimSuffix(lidStr, "@lid"), "@")
		lidUser = stripDeviceFromUser(lidUser)
		jid = types.NewJID(lidUser, types.HiddenUserServer)
	}
	jid = normalizeLIDJID(jid)

	pn, err := client.Store.LIDs.GetPNForLID(context.Background(), jid)
	if err != nil || pn.IsEmpty() {
		writeJSON(w, http.StatusNotFound, APIResponse{
			Success: false,
			Message: fmt.Sprintf("PN not found for LID %s", jid.String()),
		})
		return
	}

	writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Data: map[string]interface{}{
			"lid":   jid.User,
			"phone": pn.User,
			"pn":    pn.User,
			"jid":   pn.String(),
		},
	})
}
