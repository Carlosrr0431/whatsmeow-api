package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

const maxPollOptions = 8

type sendPollRequest struct {
	AgentCode string   `json:"agent_code"`
	Number    string   `json:"number"`
	Name      string   `json:"name"`
	Options   []string `json:"options"`
	// MaxSelections: 1 = una sola opción (recomendado para elegir propiedad)
	MaxSelections int `json:"max_selections"`
}

func (app *App) handleV2SendPoll(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, APIResponse{Success: false, Message: "Method not allowed"})
		return
	}

	instance := strings.TrimPrefix(r.URL.Path, "/v2/message/sendPoll/")
	if idx := strings.Index(instance, "/"); idx >= 0 {
		instance = instance[:idx]
	}
	instance = strings.TrimSpace(instance)
	if instance == "" {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "instance (agent_code) is required in URL path"})
		return
	}

	var req sendPollRequest
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

	name := strings.TrimSpace(req.Name)
	if name == "" {
		name = "Elegí una opción"
	}
	opts := make([]string, 0, len(req.Options))
	for _, o := range req.Options {
		o = strings.TrimSpace(o)
		if o == "" {
			continue
		}
		// WA poll option ≤ ~100 chars
		r := []rune(o)
		if len(r) > 100 {
			o = string(r[:100])
		}
		opts = append(opts, o)
		if len(opts) >= maxPollOptions {
			break
		}
	}
	if len(opts) < 2 {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "se requieren al menos 2 options"})
		return
	}

	maxSel := req.MaxSelections
	if maxSel <= 0 {
		maxSel = 1
	}
	if maxSel > len(opts) {
		maxSel = len(opts)
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

	msg := client.BuildPollCreation(name, opts, maxSel)
	jid := parseJID(phone)
	resp, err := client.SendMessage(context.Background(), jid, msg)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, APIResponse{Success: false, Message: err.Error()})
		return
	}

	s.storeSentMessage(resp.ID, jid.String(), name, "PollCreation", resp.Timestamp.Unix())
	s.storePollOptions(resp.ID, opts)
	fmt.Printf("[POLL][%s] → %s id=%s options=%d\n", req.AgentCode, phone, resp.ID, len(opts))

	writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Message: "Poll sent",
		Data: map[string]interface{}{
			"message_id": resp.ID,
			"timestamp":  resp.Timestamp.Unix(),
			"instance":   req.AgentCode,
			"options":    len(opts),
		},
	})
}
