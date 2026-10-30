package secretstore

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/zalando/go-keyring"
)

var (
	ErrNotFound    = errors.New("secret was not found")
	ErrUnavailable = errors.New("system secret store is unavailable")
)

type Store interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value []byte) error
	Delete(ctx context.Context, key string) error
}

type Backend interface {
	Get(service, user string) (string, error)
	Set(service, user, password string) error
	Delete(service, user string) error
}

type SystemStore struct {
	service string
	backend Backend
}

type systemBackend struct{}

func NewSystemStore(service string) (*SystemStore, error) {
	return NewWithBackend(service, systemBackend{})
}

func NewWithBackend(service string, backend Backend) (*SystemStore, error) {
	if strings.TrimSpace(service) == "" {
		return nil, errors.New("secret service name must not be empty")
	}
	if backend == nil {
		return nil, errors.New("secret store backend must not be nil")
	}
	return &SystemStore{service: service, backend: backend}, nil
}

func (s *SystemStore) Get(ctx context.Context, key string) ([]byte, error) {
	if err := validate(ctx, key); err != nil {
		return nil, err
	}
	result, err := invoke(ctx, func() (string, error) { return s.backend.Get(s.service, key) })
	if errors.Is(err, keyring.ErrNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, unavailable("read")
	}
	return []byte(result), nil
}

func (s *SystemStore) Set(ctx context.Context, key string, value []byte) error {
	if err := validate(ctx, key); err != nil {
		return err
	}
	_, err := invoke(ctx, func() (struct{}, error) {
		return struct{}{}, s.backend.Set(s.service, key, string(value))
	})
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return unavailable("write")
	}
	return nil
}

func (s *SystemStore) Delete(ctx context.Context, key string) error {
	if err := validate(ctx, key); err != nil {
		return err
	}
	_, err := invoke(ctx, func() (struct{}, error) {
		return struct{}{}, s.backend.Delete(s.service, key)
	})
	if errors.Is(err, keyring.ErrNotFound) {
		return nil
	}
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return unavailable("delete")
	}
	return nil
}

func (systemBackend) Get(service, user string) (string, error) {
	return keyring.Get(service, user)
}

func (systemBackend) Set(service, user, password string) error {
	return keyring.Set(service, user, password)
}

func (systemBackend) Delete(service, user string) error {
	return keyring.Delete(service, user)
}

func validate(ctx context.Context, key string) error {
	if ctx == nil {
		return errors.New("context must not be nil")
	}
	if strings.TrimSpace(key) == "" {
		return errors.New("secret key must not be empty")
	}
	return ctx.Err()
}

func unavailable(operation string) error {
	return fmt.Errorf("%w: could not %s credential", ErrUnavailable, operation)
}

type invocationResult[T any] struct {
	value T
	err   error
}

func invoke[T any](ctx context.Context, operation func() (T, error)) (T, error) {
	result := make(chan invocationResult[T], 1)
	go func() {
		value, err := operation()
		result <- invocationResult[T]{value: value, err: err}
	}()
	select {
	case completed := <-result:
		return completed.value, completed.err
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
}
