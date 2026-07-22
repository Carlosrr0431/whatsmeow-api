package main

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

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
	Header      string `json:"header"`
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

func truncateRunes(s string, max int) string {
	r := []rune(strings.TrimSpace(s))
	if len(r) <= max {
		return string(r)
	}
	return string(r[:max])
}

// buildListMessage usa NativeFlow single_select (mismo patrón que sendButtons).
// ListMessage clásico suele llegar sin botón en clientes actuales.
func buildListMessage(req sendListRequest) (*waE2E.Message, string, []waBinary.Node, error) {
	if len(req.Sections) == 0 {
		return nil, "", nil, fmt.Errorf("sections array is required")
	}

	btnText := strings.TrimSpace(req.ButtonText)
	if btnText == "" {
		btnText = "Ver opciones"
	}
	btnText = truncateRunes(btnText, 20)

	totalRows := 0
	nfSections := make([]map[string]interface{}, 0, len(req.Sections))
	for si, sec := range req.Sections {
		if len(sec.Rows) == 0 {
			return nil, "", nil, fmt.Errorf("sections[%d].rows no puede estar vacío", si)
		}
		rows := make([]map[string]interface{}, 0, len(sec.Rows))
		for ri, row := range sec.Rows {
			title := truncateRunes(row.Title, 24)
			if title == "" {
				return nil, "", nil, fmt.Errorf("sections[%d].rows[%d].title es requerido", si, ri)
			}
			id := strings.TrimSpace(row.ID)
			if id == "" {
				id = randomButtonID("row")
			}
			desc := truncateRunes(row.Description, 72)
			header := truncateRunes(row.Header, 20)
			if header == "" {
				header = fmt.Sprintf("%d", totalRows+1)
			}
			rows = append(rows, map[string]interface{}{
				"header":      header,
				"title":       title,
				"description": desc,
				"id":          id,
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
		nfSections = append(nfSections, map[string]interface{}{
			"title": secTitle,
			"rows":  rows,
		})
	}

	params := map[string]interface{}{
		"title":    btnText,
		"sections": nfSections,
	}
	paramsJSON, err := json.Marshal(params)
	if err != nil {
		return nil, "", nil, err
	}

	btnSecret := make([]byte, 32)
	if _, err := cryptorand.Read(btnSecret); err != nil {
		return nil, "", nil, fmt.Errorf("message secret: %w", err)
	}
	ctxInfo := &waE2E.MessageContextInfo{
		DeviceListMetadataVersion: proto.Int32(2),
		DeviceListMetadata:        &waE2E.DeviceListMetadata{},
		MessageSecret:             btnSecret,
	}

	bodyText := strings.TrimSpace(req.Description)
	if bodyText == "" {
		bodyText = strings.TrimSpace(req.Title)
	}
	if bodyText == "" {
		bodyText = "Elegí una opción de la lista."
	}
	// Evitar pedir "tocá Ver opciones" si el botón no llega: el botón nativo ya lo muestra.
	footer := strings.TrimSpace(req.Footer)

	templateID := strconv.FormatInt(time.Now().UnixMilli(), 10)
	messageParamsJSON := `{"from":"api","templateId":` + templateID + `}`

	msg := &waE2E.Message{
		DocumentWithCaptionMessage: &waE2E.FutureProofMessage{
			Message: &waE2E.Message{
				InteractiveMessage: &waE2E.InteractiveMessage{
					Body: &waE2E.InteractiveMessage_Body{Text: proto.String(bodyText)},
					Footer: &waE2E.InteractiveMessage_Footer{
						Text: proto.String(footer),
					},
					InteractiveMessage: &waE2E.InteractiveMessage_NativeFlowMessage_{
						NativeFlowMessage: &waE2E.InteractiveMessage_NativeFlowMessage{
							Buttons: []*waE2E.InteractiveMessage_NativeFlowMessage_NativeFlowButton{
								{
									Name:             proto.String("single_select"),
									ButtonParamsJSON: proto.String(string(paramsJSON)),
								},
							},
							MessageParamsJSON: proto.String(messageParamsJSON),
							MessageVersion:    proto.Int32(1),
						},
					},
				},
			},
		},
		MessageContextInfo: ctxInfo,
	}

	bizNodes := []waBinary.Node{
		{
			Tag: "biz",
			Content: []waBinary.Node{{
				Tag: "interactive",
				Attrs: waBinary.Attrs{
					"type": "native_flow",
					"v":    "1",
				},
				Content: []waBinary.Node{{
					Tag: "native_flow",
					Attrs: waBinary.Attrs{
						"name": "single_select",
					},
				}},
			}},
		},
		{
			Tag:   "bot",
			Attrs: waBinary.Attrs{"biz_bot": "1"},
		},
	}

	return msg, "InteractiveMessage_single_select", bizNodes, nil
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
