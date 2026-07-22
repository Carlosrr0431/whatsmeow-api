package main

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"
)

const maxListRows = 8

type sendListRow struct {
	ID          string `json:"id"`
	Title       string `json:"title"`
	Description string `json:"description"`
}

type sendListSection struct {
	Title string        `json:"title"`
	Rows  []sendListRow `json:"rows"`
}

type sendListRequest struct {
	AgentCode   string            `json:"agent_code"`
	Number      string            `json:"number"`
	Title       string            `json:"title"`
	Description string            `json:"description"`
	Footer      string            `json:"footer"`
	ButtonText  string            `json:"buttonText"`
	Sections    []sendListSection `json:"sections"`
}

func buildListMessage(req sendListRequest) (*waE2E.Message, string, []waBinary.Node, error) {
	if len(req.Sections) == 0 {
		return nil, "", nil, fmt.Errorf("sections array is required")
	}

	totalRows := 0
	sections := make([]*waE2E.ListMessage_Section, 0, len(req.Sections))
	for si, sec := range req.Sections {
		if len(sec.Rows) == 0 {
			return nil, "", nil, fmt.Errorf("sections[%d].rows no puede estar vacío", si)
		}
		rows := make([]*waE2E.ListMessage_Row, 0, len(sec.Rows))
		for ri, row := range sec.Rows {
			title := strings.TrimSpace(row.Title)
			if title == "" {
				return nil, "", nil, fmt.Errorf("sections[%d].rows[%d].title es requerido", si, ri)
			}
			id := strings.TrimSpace(row.ID)
			if id == "" {
				id = randomButtonID("row")
			}
			// WhatsApp: title ≤ 24, description ≤ 72 aprox.
			if len([]rune(title)) > 24 {
				r := []rune(title)
				title = string(r[:24])
			}
			desc := strings.TrimSpace(row.Description)
			if len([]rune(desc)) > 72 {
				r := []rune(desc)
				desc = string(r[:72])
			}
			rows = append(rows, &waE2E.ListMessage_Row{
				RowID:       proto.String(id),
				Title:       proto.String(title),
				Description: proto.String(desc),
			})
			totalRows++
			if totalRows > maxListRows {
				return nil, "", nil, fmt.Errorf("máximo %d filas en la lista", maxListRows)
			}
		}
		secTitle := strings.TrimSpace(sec.Title)
		if secTitle == "" {
			secTitle = "Opciones"
		}
		sections = append(sections, &waE2E.ListMessage_Section{
			Title: proto.String(secTitle),
			Rows:  rows,
		})
	}

	btnText := strings.TrimSpace(req.ButtonText)
	if btnText == "" {
		btnText = "Ver opciones"
	}
	title := strings.TrimSpace(req.Title)
	if title == "" {
		title = "Opciones"
	}
	description := strings.TrimSpace(req.Description)
	footer := strings.TrimSpace(req.Footer)

	btnSecret := make([]byte, 32)
	if _, err := cryptorand.Read(btnSecret); err != nil {
		return nil, "", nil, fmt.Errorf("message secret: %w", err)
	}
	ctxInfo := &waE2E.MessageContextInfo{
		DeviceListMetadataVersion: proto.Int32(2),
		DeviceListMetadata:        &waE2E.DeviceListMetadata{},
		MessageSecret:             btnSecret,
	}

	listMsg := &waE2E.ListMessage{
		Title:       proto.String(title),
		Description: proto.String(description),
		FooterText:  proto.String(footer),
		ButtonText:  proto.String(btnText),
		ListType:    waE2E.ListMessage_SINGLE_SELECT.Enum(),
		Sections:    sections,
	}

	// Mismo envoltorio que sendButtons (mejora entrega en clientes actuales).
	msg := &waE2E.Message{
		DocumentWithCaptionMessage: &waE2E.FutureProofMessage{
			Message: &waE2E.Message{
				ListMessage: listMsg,
			},
		},
		MessageContextInfo: ctxInfo,
	}

	bizNodes := []waBinary.Node{
		{
			Tag: "biz",
			Content: []waBinary.Node{{
				Tag: "list",
				Attrs: waBinary.Attrs{
					"type": "single_select",
					"v":    "2",
				},
			}},
		},
		{
			Tag:   "bot",
			Attrs: waBinary.Attrs{"biz_bot": "1"},
		},
	}

	return msg, "ListMessage", bizNodes, nil
}

func (app *App) handleV2SendList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, APIResponse{Success: false, Message: "Method not allowed"})
		return
	}

	instance := strings.TrimPrefix(r.URL.Path, "/v2/message/sendList/")
	if idx := strings.Index(instance, "/"); idx >= 0 {
		instance = instance[:idx]
	}
	instance = strings.TrimSpace(instance)
	if instance == "" {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "instance (agent_code) is required in URL path"})
		return
	}

	var req sendListRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "Invalid request body"})
		return
	}
	if strings.TrimSpace(req.AgentCode) == "" {
		req.AgentCode = instance
	}
	if req.AgentCode != instance {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "agent_code in body must match instance in URL"})
		return
	}

	s, ok := app.manager.GetSession(req.AgentCode)
	if !ok {
		writeJSON(w, http.StatusNotFound, APIResponse{Success: false, Message: "Session not found"})
		return
	}
	client, connected := s.Client()
	if !connected {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "WhatsApp not connected for this agent"})
		return
	}

	phone := strings.TrimSpace(req.Number)
	if phone == "" {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "number is required"})
		return
	}

	msg, msgType, bizNodes, err := buildListMessage(req)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: err.Error()})
		return
	}

	jid := parseJID(phone)
	resp, err := client.SendMessage(context.Background(), jid, msg, whatsmeow.SendRequestExtra{
		AdditionalNodes: &bizNodes,
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, APIResponse{Success: false, Message: err.Error()})
		return
	}

	bodyPreview := strings.TrimSpace(req.Description)
	if bodyPreview == "" {
		bodyPreview = req.Title
	}
	s.storeSentMessage(resp.ID, jid.String(), bodyPreview, msgType, resp.Timestamp.Unix())

	rowCount := 0
	for _, sec := range req.Sections {
		rowCount += len(sec.Rows)
	}
	fmt.Printf("[LIST][%s] → %s type=%s id=%s rows=%d\n", req.AgentCode, phone, msgType, resp.ID, rowCount)

	writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Message: "List sent",
		Data: map[string]interface{}{
			"message_id":   resp.ID,
			"timestamp":    resp.Timestamp.Unix(),
			"message_type": msgType,
			"instance":     req.AgentCode,
			"rows":         rowCount,
		},
	})
}
