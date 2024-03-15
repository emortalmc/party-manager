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
	repo  ReadWriter
	notif KafkaWriter
}

func NewService(repo ReadWriter, notif KafkaWriter) *Service {
	return &Service{
		repo:  repo,
		notif: notif,
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
	e, err := s.repo.GetLiveEvent(ctx)
	if err != nil {
		return fmt.Errorf("failed to delete current event: %w", err)
	}

	return s.DeleteEvent(ctx, e)
}

func (s *Service) DeleteEventByID(ctx context.Context, eventId string) error {
	e, err := s.repo.GetEventByID(ctx, eventId)
	if err != nil {
		return fmt.Errorf("failed to get event: %w", err)

	}

	return s.DeleteEvent(ctx, e)
}

func (s *Service) DeleteEvent(ctx context.Context, event *model.Event) error {
	if err := s.repo.DeleteEvent(ctx, event.ID); err != nil {
		return fmt.Errorf("failed to delete event: %w", err)
	}

	s.notif.DeleteEvent(ctx, event)

	return nil
}
