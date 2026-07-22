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

type sendButtonItem struct {
	Type        string  `json:"type"`
	DisplayText string  `json:"displayText"`
	ID          string  `json:"id"`
	URL         string  `json:"url"`
	PhoneNumber string  `json:"phoneNumber"`
	CopyCode    string  `json:"copyCode"`
	Currency    string  `json:"currency"`
	Name        string  `json:"name"`
	KeyType     string  `json:"keyType"`
	Key         string  `json:"key"`
	Amount      float64 `json:"amount"`
}

type sendButtonsRequest struct {
	AgentCode   string           `json:"agent_code"`
	Number      string           `json:"number"`
	Title       string           `json:"title"`
	Description string           `json:"description"`
	Footer      string           `json:"footer"`
	Buttons     []sendButtonItem `json:"buttons"`
}

func mapPixKeyType(keyType string) string {
	switch strings.ToLower(strings.TrimSpace(keyType)) {
	case "phone":
		return "PHONE"
	case "email":
		return "EMAIL"
	case "cpf":
		return "CPF"
	case "cnpj":
		return "CNPJ"
	case "random", "evp":
		return "EVP"
	default:
		return strings.ToUpper(keyType)
	}
}

func randomButtonID(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
}

func buildButtonsMessage(req sendButtonsRequest) (*waE2E.Message, string, []waBinary.Node, error) {
	if len(req.Buttons) == 0 {
		return nil, "", nil, fmt.Errorf("buttons array is required")
	}

	hasReply, hasPix, hasOther := false, false, false
	replyCount := 0
	for _, b := range req.Buttons {
		switch strings.ToLower(strings.TrimSpace(b.Type)) {
		case "reply":
			hasReply = true
			replyCount++
		case "pix":
			hasPix = true
		default:
			hasOther = true
		}
	}

	if hasReply {
		if replyCount > 3 {
			return nil, "", nil, fmt.Errorf("máximo 3 botones reply")
		}
		if hasOther || hasPix {
			return nil, "", nil, fmt.Errorf("reply no se puede mezclar con url/call/pix")
		}
	}
	if hasPix && len(req.Buttons) > 1 {
		return nil, "", nil, fmt.Errorf("pix debe ir solo (1 botón)")
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

	nativeButtons := make([]*waE2E.InteractiveMessage_NativeFlowMessage_NativeFlowButton, 0, len(req.Buttons))
	for i, b := range req.Buttons {
		btnType := strings.ToLower(strings.TrimSpace(b.Type))
		display := strings.TrimSpace(b.DisplayText)
		if display == "" {
			return nil, "", nil, fmt.Errorf("buttons[%d].displayText es requerido", i)
		}

		var name string
		var params map[string]interface{}

		switch btnType {
		case "reply":
			id := strings.TrimSpace(b.ID)
			if id == "" {
				id = randomButtonID("btn")
			}
			name = "quick_reply"
			params = map[string]interface{}{"display_text": display, "id": id}
		case "url":
			url := strings.TrimSpace(b.URL)
			if url == "" {
				return nil, "", nil, fmt.Errorf("buttons[%d].url es requerido", i)
			}
			id := strings.TrimSpace(b.ID)
			if id == "" {
				id = randomButtonID("url")
			}
			name = "cta_url"
			params = map[string]interface{}{
				"display_text": display,
				"url":          url,
				"merchant_url": url,
				"id":           id,
			}
		case "call":
			phone := strings.TrimSpace(b.PhoneNumber)
			if phone == "" {
				return nil, "", nil, fmt.Errorf("buttons[%d].phoneNumber es requerido", i)
			}
			id := strings.TrimSpace(b.ID)
			if id == "" {
				id = randomButtonID("call")
			}
			name = "cta_call"
			params = map[string]interface{}{
				"display_text":  display,
				"phone_number":  phone,
				"id":            id,
			}
		case "copy":
			copyCode := strings.TrimSpace(b.CopyCode)
			if copyCode == "" {
				copyCode = strings.TrimSpace(b.ID)
			}
			id := strings.TrimSpace(b.ID)
			if id == "" {
				id = randomButtonID("copy")
			}
			name = "cta_copy"
			params = map[string]interface{}{
				"display_text": display,
				"id":           id,
				"copy_code":    copyCode,
			}
		case "pix":
			key := strings.TrimSpace(b.Key)
			merchant := strings.TrimSpace(b.Name)
			if key == "" || merchant == "" {
				return nil, "", nil, fmt.Errorf("pix requiere name y key")
			}
			currency := strings.TrimSpace(b.Currency)
			if currency == "" {
				currency = "BRL"
			}
			name = "payment_info"
			refID := randomButtonID("pix")
			params = map[string]interface{}{
				"currency":     currency,
				"total_amount": map[string]interface{}{"value": 0, "offset": 100},
				"reference_id": refID,
				"type":         "physical-goods",
				"order": map[string]interface{}{
					"status":     "pending",
					"subtotal":   map[string]interface{}{"value": 0, "offset": 100},
					"order_type": "ORDER",
					"items": []map[string]interface{}{
						{
							"name":        "",
							"amount":      map[string]interface{}{"value": 0, "offset": 100},
							"quantity":    0,
							"sale_amount": map[string]interface{}{"value": 0, "offset": 100},
						},
					},
				},
				"payment_settings": []map[string]interface{}{
					{
						"type": "pix_static_code",
						"pix_static_code": map[string]string{
							"merchant_name": merchant,
							"key":           key,
							"key_type":      mapPixKeyType(b.KeyType),
						},
					},
				},
				"share_payment_status": false,
			}
		default:
			return nil, "", nil, fmt.Errorf("tipo de botón no soportado: %s", b.Type)
		}

		paramsJSON, err := json.Marshal(params)
		if err != nil {
			return nil, "", nil, err
		}
		nativeButtons = append(nativeButtons, &waE2E.InteractiveMessage_NativeFlowMessage_NativeFlowButton{
			Name:             proto.String(name),
			ButtonParamsJSON: proto.String(string(paramsJSON)),
		})
	}

	var msg *waE2E.Message
	var msgType string
	var bizNativeName string

	// Reply (y resto): NativeFlow. ButtonsMessage clásico lo rechaza WA con 405.
	if hasPix {
		paymentParams := `{"native_flow_name":"order_details","version":1}`
		var body *waE2E.InteractiveMessage_Body
		if t := strings.TrimSpace(req.Title); t != "" {
			body = &waE2E.InteractiveMessage_Body{Text: proto.String(t)}
		} else if d := strings.TrimSpace(req.Description); d != "" {
			body = &waE2E.InteractiveMessage_Body{Text: proto.String(d)}
		}
		msg = &waE2E.Message{
			DocumentWithCaptionMessage: &waE2E.FutureProofMessage{
				Message: &waE2E.Message{
					InteractiveMessage: &waE2E.InteractiveMessage{
						Body: body,
						InteractiveMessage: &waE2E.InteractiveMessage_NativeFlowMessage_{
							NativeFlowMessage: &waE2E.InteractiveMessage_NativeFlowMessage{
								Buttons:           nativeButtons,
								MessageParamsJSON: proto.String(paymentParams),
								MessageVersion:    proto.Int32(1),
							},
						},
					},
				},
			},
			MessageContextInfo: ctxInfo,
		}
		msgType = "InteractiveMessage"
		bizNativeName = "payment_info"
	} else {
		body := strings.TrimSpace(req.Description)
		if req.Title != "" {
			body = "*" + req.Title + "*"
			if strings.TrimSpace(req.Description) != "" {
				body += "\n\n" + req.Description
			}
		}
		templateID := strconv.FormatInt(time.Now().UnixMilli(), 10)
		messageParamsJSON := `{"from":"api","templateId":` + templateID + `}`
		msgVersion := int32(1)
		// Sin DocumentWithCaption: ese wrapper hace que WA responda 405 en reply.
		interactive := &waE2E.InteractiveMessage{
			Body: &waE2E.InteractiveMessage_Body{Text: proto.String(body)},
			Footer: &waE2E.InteractiveMessage_Footer{
				Text: proto.String(req.Footer),
			},
			InteractiveMessage: &waE2E.InteractiveMessage_NativeFlowMessage_{
				NativeFlowMessage: &waE2E.InteractiveMessage_NativeFlowMessage{
					Buttons:           nativeButtons,
					MessageParamsJSON: proto.String(messageParamsJSON),
					MessageVersion:    proto.Int32(msgVersion),
				},
			},
		}
		msg = &waE2E.Message{
			InteractiveMessage: interactive,
			MessageContextInfo: ctxInfo,
		}
		msgType = "InteractiveMessage"
		if hasReply && !hasOther {
			bizNativeName = "quick_reply"
		} else {
			bizNativeName = "mixed"
		}
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
						"v":    "9",
						"name": bizNativeName,
					},
				}},
			}},
		},
	}

	return msg, msgType, bizNodes, nil
}

func (app *App) handleV2SendButtons(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, APIResponse{Success: false, Message: "Method not allowed"})
		return
	}

	instance := strings.TrimPrefix(r.URL.Path, "/v2/message/sendButtons/")
	if idx := strings.Index(instance, "/"); idx >= 0 {
		instance = instance[:idx]
	}
	instance = strings.TrimSpace(instance)
	if instance == "" {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "instance (agent_code) is required in URL path"})
		return
	}

	var req sendButtonsRequest
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

	msg, msgType, bizNodes, err := buildButtonsMessage(req)
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

	fmt.Printf("[BUTTONS][%s] → %s type=%s id=%s buttons=%d\n", req.AgentCode, phone, msgType, resp.ID, len(req.Buttons))

	writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Message: "Buttons sent",
		Data: map[string]interface{}{
			"message_id": resp.ID,
			"timestamp":  resp.Timestamp.Unix(),
			"message_type": msgType,
			"instance":   req.AgentCode,
		},
	})
}
