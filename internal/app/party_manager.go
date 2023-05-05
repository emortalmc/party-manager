package app

import (
	"context"
	"go.uber.org/zap"
	"os/signal"
	"party-manager/internal/config"
	"party-manager/internal/kafka"
	"party-manager/internal/repository"
	"party-manager/internal/service"
	"sync"
	"syscall"
)

func Run(cfg *config.Config, logger *zap.SugaredLogger) {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	wg := &sync.WaitGroup{}

	delayedWg := &sync.WaitGroup{}
	delayedCtx, delayedCancel := context.WithCancel(ctx)

	repo, err := repository.NewMongoRepository(delayedCtx, logger, delayedWg, cfg.MongoDB)
	if err != nil {
		logger.Fatalw("failed to connect to mongo", err)
	}

	notif := kafka.NewKafkaNotifier(delayedCtx, delayedWg, cfg.Kafka, logger)
	if err != nil {
		logger.Fatalw("failed to create kafka notifier", err)
	}

	kafka.NewConsumer(ctx, wg, cfg.Kafka, logger, notif, repo)

	service.RunServices(ctx, logger, wg, cfg, repo, notif)

	wg.Wait()
	logger.Info("stopped services")

	logger.Info("shutting down repository and kafka")
	delayedCancel()
	delayedWg.Wait()
}
