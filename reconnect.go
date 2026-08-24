package main

import "time"

// Backoff de reconexión (sin keep-alive artificial).
// Tras un corte: esperar → reintentar → escalar. No login en ráfaga.
var reconnectBackoff = []time.Duration{
	30 * time.Second,
	1 * time.Minute,
	2 * time.Minute,
	5 * time.Minute,
	10 * time.Minute,
	15 * time.Minute,
}

const (
	reconnectFlapWindow     = 2 * time.Minute
	ensureConnectedWait     = 3 * time.Second
	ensureConnectedStep     = 200 * time.Millisecond
	maxReconnectEscalations = 8
	// Solo reemplazar un cliente muerto. Nunca cortar una sesión con websocket vivo.
	replaceDeadClientAfter = 15 * time.Minute
	// Otra conexión tomó el socket (p. ej. overlap de redeploy). No pelear de inmediato.
	streamReplacedBackoff = 2 * time.Minute
)

// shouldReplaceWhatsAppClient: false si la sesión está viva o el WS apenas parpadeó.
func shouldReplaceWhatsAppClient(live, clientExists, neverConnected bool, disconnectAge time.Duration) bool {
	if live {
		return false
	}
	if !clientExists {
		return true
	}
	if neverConnected {
		return true
	}
	return disconnectAge >= replaceDeadClientAfter
}

func reconnectDelayAfterAttempt(failedAttempts int) time.Duration {
	if failedAttempts < 0 {
		failedAttempts = 0
	}
	if failedAttempts >= len(reconnectBackoff) {
		return reconnectBackoff[len(reconnectBackoff)-1]
	}
	return reconnectBackoff[failedAttempts]
}

func reconnectAttemptCapped(failedAttempts int) int {
	if failedAttempts < 0 {
		return 0
	}
	if failedAttempts > maxReconnectEscalations {
		return maxReconnectEscalations
	}
	return failedAttempts
}

func shouldSkipAutoReconnect(loggedOut, alreadyConnected, reconnecting bool, now, nextReconnectAt time.Time) bool {
	if loggedOut || alreadyConnected || reconnecting {
		return true
	}
	if !nextReconnectAt.IsZero() && now.Before(nextReconnectAt) {
		return true
	}
	return false
}

func nextReconnectAtAfterDisconnect(now, lastConnectedAt, currentNext time.Time, failedAttempts int, connectedDuration time.Duration) (time.Time, int) {
	attempt := failedAttempts
	if !lastConnectedAt.IsZero() && connectedDuration > 0 && connectedDuration < reconnectFlapWindow {
		attempt = reconnectAttemptCapped(attempt + 1)
	}
	delay := reconnectDelayAfterAttempt(attempt)
	next := now.Add(delay)
	if !currentNext.IsZero() && currentNext.After(next) {
		next = currentNext
	}
	return next, attempt
}
