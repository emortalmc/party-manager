package event

import (
	"context"
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo"
	"party-manager/internal/repository/model"
	"time"
)

type Service struct {
	repo ReadWriter
}

func NewService(repo ReadWriter) *Service {
	return &Service{
		repo: repo,
	}
}

var ErrEventAlreadyExists = errors.New("event already exists")

func (s *Service) CreateEvent(ctx context.Context, event *model.Event) error {
	if err := s.repo.CreateEvent(ctx, event); err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return ErrEventAlreadyExists
		}

		return fmt.Errorf("failed to create event: %w", err)
	}

	return nil
}

func (s *Service) UpdateEvent(ctx context.Context, eventId string, displayTime *time.Time, startTime *time.Time) (*model.Event, error) {
	return s.repo.UpdateEvent(ctx, eventId, displayTime, startTime)
}

func (s *Service) DeleteCurrentEvent(ctx context.Context) error {
	return s.repo.DeleteCurrentEvent(ctx)
}

func (s *Service) DeleteEvent(ctx context.Context, eventId string) error {
	// todo process if it's a current event or not. If it is, we need to send out an end message
	return s.repo.DeleteEvent(ctx, eventId)
}
