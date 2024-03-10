package app

import (
	"context"
	"go.uber.org/zap"
	"os/signal"
	"party-manager/internal/app/event"
	"party-manager/internal/app/party"
	"party-manager/internal/config"
	"party-manager/internal/grpc"
	"party-manager/internal/kafka/consumer"
	"party-manager/internal/kafka/writer"
	"party-manager/internal/repository"
	"sync"
	"syscall"
)

func Run(cfg *config.Config, logger *zap.SugaredLogger) {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	wg := &sync.WaitGroup{}

	delayedWg := &sync.WaitGroup{}
	delayedCtx, delayedCancel := context.WithCancel(ctx)

	repo, err := repository.NewMongoRepository(delayedCtx, logger, delayedWg, &cfg.MongoDB)
	if err != nil {
		logger.Fatalw("failed to connect to mongo", err)
	}

	notif := writer.NewKafkaNotifier(delayedCtx, delayedWg, &cfg.Kafka, logger)
	if err != nil {
		logger.Fatalw("failed to create kafka notifier", err)
	}

	partySvc := party.NewService(logger, repo, notif)
	eventSvc := event.NewService(repo)
	event.NewScheduler(ctx, wg, logger, repo, notif, partySvc)

	consumer.NewConsumer(ctx, wg, &cfg.Kafka, logger, partySvc)

	grpc.RunServices(ctx, logger, wg, cfg, repo, partySvc, eventSvc)

	wg.Wait()
	logger.Info("stopped services")

	logger.Info("shutting down repository and kafka")
	delayedCancel()
	delayedWg.Wait()
}
