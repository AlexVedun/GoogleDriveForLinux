package secretstore

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/zalando/go-keyring"
)

func TestSystemStoreRoundTripAndDelete(t *testing.T) {
	backend := &memoryBackend{items: make(map[string]string)}
	store, err := NewWithBackend("google-drive-linux", backend)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := store.Set(ctx, "primary", []byte("opaque-token-json")); err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(ctx, "primary")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "opaque-token-json" {
		t.Fatalf("Get() = %q, want stored bytes", got)
	}
	if backend.lastService != "google-drive-linux" || backend.lastUser != "primary" {
		t.Fatalf("backend key = (%q, %q), want service and account key", backend.lastService, backend.lastUser)
	}
	if err := store.Delete(ctx, "primary"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(ctx, "primary"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get() after Delete() error = %v, want ErrNotFound", err)
	}
	if err := store.Delete(ctx, "primary"); err != nil {
		t.Fatalf("Delete() of missing secret = %v, want idempotent success", err)
	}
}

func TestSystemStoreMapsBackendFailuresWithoutExposingDetails(t *testing.T) {
	backend := &memoryBackend{items: make(map[string]string), getError: errors.New("dbus address leaked-secret")}
	store, err := NewWithBackend("google-drive-linux", backend)
	if err != nil {
		t.Fatal(err)
	}

	_, err = store.Get(context.Background(), "primary")
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Get() error = %v, want ErrUnavailable", err)
	}
	if strings.Contains(err.Error(), "leaked-secret") || strings.Contains(err.Error(), "dbus address") {
		t.Fatalf("Get() exposed backend details: %v", err)
	}
	if backend.calls != 1 {
		t.Fatalf("backend calls = %d, want one; no fallback store should be used", backend.calls)
	}
}

func TestSystemStoreMapsMissingKeyringEntry(t *testing.T) {
	backend := &memoryBackend{items: make(map[string]string), getError: keyring.ErrNotFound}
	store, err := NewWithBackend("google-drive-linux", backend)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(context.Background(), "primary"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get() error = %v, want ErrNotFound", err)
	}
}

func TestSystemStoreHonorsContextDeadline(t *testing.T) {
	started := make(chan struct{})
	finish := make(chan struct{})
	backend := &memoryBackend{
		items:      make(map[string]string),
		getStarted: started,
		getWait:    finish,
	}
	store, err := NewWithBackend("google-drive-linux", backend)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := store.Get(ctx, "primary")
		done <- err
	}()
	<-started
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Get() error = %v, want context deadline", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Get() did not return when its context expired")
	}
	close(finish)
}

type memoryBackend struct {
	items       map[string]string
	getError    error
	setError    error
	deleteErr   error
	getStarted  chan struct{}
	getWait     chan struct{}
	calls       int
	lastService string
	lastUser    string
}

func (b *memoryBackend) Get(service, user string) (string, error) {
	b.record(service, user)
	if b.getStarted != nil {
		close(b.getStarted)
		<-b.getWait
	}
	if b.getError != nil {
		return "", b.getError
	}
	value, ok := b.items[user]
	if !ok {
		return "", keyring.ErrNotFound
	}
	return value, nil
}

func (b *memoryBackend) Set(service, user, password string) error {
	b.record(service, user)
	if b.setError != nil {
		return b.setError
	}
	b.items[user] = password
	return nil
}

func (b *memoryBackend) Delete(service, user string) error {
	b.record(service, user)
	if b.deleteErr != nil {
		return b.deleteErr
	}
	if _, ok := b.items[user]; !ok {
		return keyring.ErrNotFound
	}
	delete(b.items, user)
	return nil
}

func (b *memoryBackend) record(service, user string) {
	b.calls++
	b.lastService = service
	b.lastUser = user
}
