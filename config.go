package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Límites en memoria (configurables vía env en Railway).
var (
	maxMsgHistLimit        int
	maxRawMediaLimit       int
	skipGroupsAndBroadcast bool
	verboseLogs            bool
	clientLogLevel         string
	sqliteBusyTimeoutMS    int
	autoConnectStaggerSec    int
	autoConnectDelaySec      int
	reconnectWatchSec        int
	shutdownWaitSec          int
	sessionStatusDebounceSec int

	// Pausa de escritura antes de enviar mensajes (cumplimiento políticas Meta).
	sendMinDelayMs int
	sendMaxDelayMs int
	sendJitterMs   int

	// Reintento de webhook ante errores 5xx.
	webhookRetryMax     int
	webhookRetryDelayMs int
)

func initRuntimeConfig() {
	maxMsgHistLimit = envInt("MAX_MSG_HISTORY", 100)
	maxRawMediaLimit = envInt("MAX_RAW_MEDIA_CACHE", 120)
	skipGroupsAndBroadcast = envBool("SKIP_GROUPS", true)
	verboseLogs = envBool("VERBOSE_LOGS", false)
	sqliteBusyTimeoutMS = envInt("SQLITE_BUSY_TIMEOUT_MS", 15000)
	autoConnectStaggerSec = envInt("AUTO_CONNECT_STAGGER_SEC", 20)
	autoConnectDelaySec = envInt("AUTO_CONNECT_DELAY_SEC", 5)
	reconnectWatchSec = envInt("RECONNECT_WATCH_SEC", 15)
	shutdownWaitSec = envInt("SHUTDOWN_WAIT_SEC", 2)
	sessionStatusDebounceSec = envInt("SESSION_STATUS_DEBOUNCE_SEC", 15)
	sendMinDelayMs = envInt("SEND_MIN_DELAY_MS", 800)
	sendMaxDelayMs = envInt("SEND_MAX_DELAY_MS", 3500)
	sendJitterMs = envInt("SEND_JITTER_MS", 400)
	webhookRetryMax = envInt("WEBHOOK_RETRY_MAX", 2)
	webhookRetryDelayMs = envInt("WEBHOOK_RETRY_DELAY_MS", 1500)
	clientLogLevel = strings.TrimSpace(strings.ToUpper(os.Getenv("CLIENT_LOG_LEVEL")))
	if clientLogLevel == "" {
		if verboseLogs {
			clientLogLevel = "WARN"
		} else {
			clientLogLevel = "ERROR"
		}
	}
}

func envInt(key string, def int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return def
	}
	return n
}

func envBool(key string, def bool) bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	if v == "" {
		return def
	}
	switch v {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return def
	}
}

func logVerbose(format string, args ...interface{}) {
	if verboseLogs {
		fmt.Printf(format, args...)
	}
}
