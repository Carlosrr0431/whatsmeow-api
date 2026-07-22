package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"go.mau.fi/whatsmeow/types"
)

type MessageEvent struct {
	ID            string `json:"id"`
	From          string `json:"from"`
	To            string `json:"to"`
	Body          string `json:"body"`
	Type          string `json:"type"`
	Timestamp     int64  `json:"timestamp"`
	IsGroup       bool   `json:"is_group"`
	IsFromMe      bool   `json:"is_from_me"`
	PushName      string `json:"push_name"`
	ChatJID       string `json:"chat_jid"`
	SenderPN      string `json:"sender_pn,omitempty"`
	SenderLID     string `json:"sender_lid,omitempty"`
	MediaURL      string `json:"media_url,omitempty"`
	Mimetype      string `json:"mimetype,omitempty"`
	MediaKey      string `json:"media_key,omitempty"`
	DirectPath    string `json:"direct_path,omitempty"`
	FileEncSHA256 string `json:"file_enc_sha256,omitempty"`
	FileSHA256    string `json:"file_sha256,omitempty"`
	FileLength    uint64 `json:"file_length,omitempty"`
	FileName      string `json:"file_name,omitempty"`
	Width         uint32 `json:"width,omitempty"`
	Height        uint32 `json:"height,omitempty"`
	Seconds       uint32 `json:"seconds,omitempty"`
	HasMedia           bool   `json:"has_media,omitempty"`
	ReactionTargetID   string `json:"reaction_target_id,omitempty"`
	IsAnimatedSticker  bool   `json:"is_animated_sticker,omitempty"`
	ButtonID           string `json:"button_id,omitempty"`
}

type APIResponse struct {
	Success bool        `json:"success"`
	Message string      `json:"message,omitempty"`
	Data    interface{} `json:"data,omitempty"`
}

// receiptTypeToStatus mapea recibos de WhatsApp al read_status del CRM (0-5).
func receiptTypeToStatus(receiptType types.ReceiptType) int {
	switch receiptType {
	case types.ReceiptTypeSender:
		return 2
	case types.ReceiptTypeDelivered:
		return 3
	case types.ReceiptTypeRead, types.ReceiptTypeReadSelf:
		return 4
	case types.ReceiptTypePlayed, types.ReceiptTypePlayedSelf:
		return 5
	case types.ReceiptTypeRetry:
		return 1
	default:
		return -1
	}
}

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
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-API-Key, Authorization")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" || r.URL.Path == "/" {
			next.ServeHTTP(w, r)
			return
		}

		apiKey := os.Getenv("API_KEY")
		if apiKey == "" {
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

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, APIResponse{Success: true, Message: "ok"})
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, APIResponse{
			Success: true,
			Message: "WhatsApp API Service - Multi-session",
			Data: map[string]interface{}{
				"version": "2.0.0",
				"multi_session": true,
				"endpoints": []string{
					"/api/status",
					"/api/status?agent_code=AGENT",
					"/api/session/connect",
					"/api/session/qr?agent_code=AGENT",
					"/api/messages/send",
					"/api/messages/send-image",
					"/api/messages/send-media",
					"/api/webhook/config",
					"/v2/message/sendButtons/{instance}",
					"/v2/message/sendList/{instance}",
				},
			},
		})
	})

	mux.HandleFunc("/api/status", app.handleStatus)
	mux.HandleFunc("/api/session/connect", app.handleConnect)
	mux.HandleFunc("/api/session/qr", app.handleGetQR)
	mux.HandleFunc("/api/session/disconnect", app.handleDisconnect)
	mux.HandleFunc("/api/session/logout", app.handleLogout)
	mux.HandleFunc("/api/messages/send", app.handleSendMessage)
	mux.HandleFunc("/api/messages/send-image", app.handleSendImage)
	mux.HandleFunc("/api/messages/send-media", app.handleSendMedia)
	mux.HandleFunc("/api/messages/send-group", app.handleSendGroupMessage)
	mux.HandleFunc("/api/messages/history", app.handleGetMessages)
	mux.HandleFunc("/api/messages/media/", app.handleDownloadMedia)
	mux.HandleFunc("/api/messages/revoke", app.handleRevokeMessage)
	mux.HandleFunc("/api/messages/edit", app.handleEditMessage)
	mux.HandleFunc("/api/messages/reaction", app.handleSendReaction)
	mux.HandleFunc("/v2/message/sendButtons/", app.handleV2SendButtons)
	mux.HandleFunc("/v2/message/sendList/", app.handleV2SendList)
	mux.HandleFunc("/api/contacts", app.handleGetContacts)
	mux.HandleFunc("/api/groups", app.handleGetGroups)
	mux.HandleFunc("/api/check-number", app.handleCheckNumber)
	mux.HandleFunc("/api/profile-pic", app.handleGetProfilePic)
	mux.HandleFunc("/api/pn-from-lid/", app.handlePNFromLID)
	mux.HandleFunc("/api/webhook/config", app.handleWebhookConfig)

	handler := corsMiddleware(authMiddleware(mux))

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	go func() {
		delay := time.Duration(autoConnectDelaySec) * time.Second
		if delay > 0 {
			time.Sleep(delay)
		}
		app.manager.AutoConnectAll()
	}()

	server := &http.Server{
		Addr:         ":" + port,
		Handler:      handler,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 120 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	fmt.Printf("🚀 WhatsApp API Server (multi-session) starting on port %s\n", port)

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
	app.manager.DisconnectAll()
	if shutdownWaitSec > 0 {
		time.Sleep(time.Duration(shutdownWaitSec) * time.Second)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	server.Shutdown(ctx)
	fmt.Println("Server stopped")
}
