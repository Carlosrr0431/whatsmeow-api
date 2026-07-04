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
	autoConnectStaggerSec  int
)

func initRuntimeConfig() {
	maxMsgHistLimit = envInt("MAX_MSG_HISTORY", 100)
	maxRawMediaLimit = envInt("MAX_RAW_MEDIA_CACHE", 120)
	skipGroupsAndBroadcast = envBool("SKIP_GROUPS", true)
	verboseLogs = envBool("VERBOSE_LOGS", false)
	sqliteBusyTimeoutMS = envInt("SQLITE_BUSY_TIMEOUT_MS", 15000)
	autoConnectStaggerSec = envInt("AUTO_CONNECT_STAGGER_SEC", 3)
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
