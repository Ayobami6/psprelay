package internal

import "context"

// Service defines the domain health verification port.
type Service interface {
	CheckHealth(ctx context.Context) error
}

type serviceImpl struct {
	store WebhookStore
}

// NewService constructs a health check service backed by an optional WebhookStore.
func NewService(store ...WebhookStore) Service {
	var s WebhookStore
	if len(store) > 0 {
		s = store[0]
	}
	return &serviceImpl{store: s}
}

func (s *serviceImpl) CheckHealth(ctx context.Context) error {
	if s.store != nil {
		return s.store.Ping(ctx)
	}
	return nil
}
