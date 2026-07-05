package main

import (
	"fmt"
	"strings"

	waLog "go.mau.fi/whatsmeow/util/log"
)

// filteredClientLogger baja a verbose los EOF de websocket (reconexión automática de whatsmeow).
type filteredClientLogger struct {
	inner     waLog.Logger
	agentCode string
}

func newFilteredClientLogger(agentCode string) waLog.Logger {
	base := waLog.Stdout("Client-"+safeAgentDir(agentCode), clientLogLevel, true)
	return &filteredClientLogger{inner: base, agentCode: agentCode}
}

func (l *filteredClientLogger) Sub(module string) waLog.Logger {
	return &filteredClientLogger{inner: l.inner.Sub(module), agentCode: l.agentCode}
}

func (l *filteredClientLogger) Debugf(msg string, args ...interface{}) {
	l.inner.Debugf(msg, args...)
}

func (l *filteredClientLogger) Infof(msg string, args ...interface{}) {
	l.inner.Infof(msg, args...)
}

func (l *filteredClientLogger) Warnf(msg string, args ...interface{}) {
	l.inner.Warnf(msg, args...)
}

func (l *filteredClientLogger) Errorf(msg string, args ...interface{}) {
	if isBenignWhatsAppSocketError(msg, args...) {
		logVerbose("[CLIENT][%s] %s\n", l.agentCode, formatLogMsg(msg, args...))
		return
	}
	l.inner.Errorf(msg, args...)
}

func formatLogMsg(msg string, args ...interface{}) string {
	if len(args) == 0 {
		return msg
	}
	return fmt.Sprintf(msg, args...)
}

func isBenignWhatsAppSocketError(msg string, args ...interface{}) bool {
	text := strings.ToLower(formatLogMsg(msg, args...))
	if strings.Contains(text, "websocket") && strings.Contains(text, "eof") {
		return true
	}
	if strings.Contains(text, "failed to read frame header") && strings.Contains(text, "eof") {
		return true
	}
	if strings.Contains(text, "failed to get reader") && strings.Contains(text, "eof") {
		return true
	}
	return false
}
