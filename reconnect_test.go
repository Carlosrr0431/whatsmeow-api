package main

import (
	"testing"
	"time"
)

func TestReconnectDelayAfterAttempt(t *testing.T) {
	want := []time.Duration{
		30 * time.Second,
		time.Minute,
		2 * time.Minute,
		5 * time.Minute,
		10 * time.Minute,
		15 * time.Minute,
		15 * time.Minute,
	}
	for i, d := range want {
		got := reconnectDelayAfterAttempt(i)
		if got != d {
			t.Fatalf("attempt %d: got %s want %s", i, got, d)
		}
	}
}

func TestShouldSkipAutoReconnect(t *testing.T) {
	now := time.Date(2026, 8, 24, 19, 0, 0, 0, time.UTC)
	later := now.Add(30 * time.Second)

	if !shouldSkipAutoReconnect(true, false, false, now, time.Time{}) {
		t.Fatal("logged out should skip")
	}
	if !shouldSkipAutoReconnect(false, true, false, now, time.Time{}) {
		t.Fatal("already connected should skip")
	}
	if !shouldSkipAutoReconnect(false, false, true, now, time.Time{}) {
		t.Fatal("in-flight reconnect should skip")
	}
	if !shouldSkipAutoReconnect(false, false, false, now, later) {
		t.Fatal("backoff not elapsed should skip")
	}
	if shouldSkipAutoReconnect(false, false, false, now, time.Time{}) {
		t.Fatal("first retry with no deadline should run")
	}
	if shouldSkipAutoReconnect(false, false, false, later, now) {
		t.Fatal("deadline in the past should run")
	}
}

func TestNextReconnectAtAfterDisconnect_FirstDropWaits30s(t *testing.T) {
	now := time.Date(2026, 8, 24, 19, 0, 0, 0, time.UTC)
	lastConnected := now.Add(-30 * time.Minute)
	next, attempt := nextReconnectAtAfterDisconnect(now, lastConnected, time.Time{}, 0, 30*time.Minute)
	if attempt != 0 {
		t.Fatalf("attempt=%d want 0", attempt)
	}
	if next.Sub(now) != 30*time.Second {
		t.Fatalf("delay=%s want 30s", next.Sub(now))
	}
}

func TestNextReconnectAtAfterDisconnect_FlapEscalates(t *testing.T) {
	now := time.Date(2026, 8, 24, 19, 0, 0, 0, time.UTC)
	lastConnected := now.Add(-10 * time.Second)
	next, attempt := nextReconnectAtAfterDisconnect(now, lastConnected, time.Time{}, 0, 10*time.Second)
	if attempt != 1 {
		t.Fatalf("attempt=%d want 1", attempt)
	}
	if next.Sub(now) != time.Minute {
		t.Fatalf("delay=%s want 1m", next.Sub(now))
	}
}

func TestStreamReplacedBackoff(t *testing.T) {
	if streamReplacedBackoff != 2*time.Minute {
		t.Fatalf("streamReplacedBackoff=%s want 2m", streamReplacedBackoff)
	}
}

func TestShouldReplaceWhatsAppClient_NeverTouchesLiveSession(t *testing.T) {
	if shouldReplaceWhatsAppClient(true, true, false, 0) {
		t.Fatal("sesión viva no se reemplaza")
	}
	if shouldReplaceWhatsAppClient(true, true, false, time.Hour) {
		t.Fatal("sesión viva no se reemplaza aunque el corte sea viejo")
	}
	if shouldReplaceWhatsAppClient(false, true, false, 30*time.Second) {
		t.Fatal("corte breve: dejar AutoReconnect, no Disconnect")
	}
	if !shouldReplaceWhatsAppClient(false, false, true, 0) {
		t.Fatal("sin cliente (arrancando proceso) sí se crea uno")
	}
	if !shouldReplaceWhatsAppClient(false, true, true, 0) {
		t.Fatal("primer connect fallido (nunca hubo sesión viva) sí se reintenta")
	}
	if !shouldReplaceWhatsAppClient(false, true, false, 15*time.Minute) {
		t.Fatal("cliente muerto rato largo sí se puede reemplazar")
	}
}

func TestNextReconnectAtAfterDisconnect_DoesNotShrinkExistingBackoff(t *testing.T) {
	now := time.Date(2026, 8, 24, 19, 0, 0, 0, time.UTC)
	current := now.Add(10 * time.Minute)
	next, _ := nextReconnectAtAfterDisconnect(now, now.Add(-30*time.Minute), current, 0, 30*time.Minute)
	if !next.Equal(current) {
		t.Fatalf("got %s want keep %s", next, current)
	}
}
