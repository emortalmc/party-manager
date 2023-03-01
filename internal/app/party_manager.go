package app

import (
	"context"
	"fmt"
	"github.com/emortalmc/proto-specs/gen/go/grpc/party"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	"github.com/rabbitmq/amqp091-go"
	"github.com/zakshearman/go-grpc-health/pkg/health"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"net"
	"party-manager/internal/config"
	"party-manager/internal/rabbitmq/notifier"
	"party-manager/internal/repository"
	"party-manager/internal/service"
	"time"
)

func Run(ctx context.Context, cfg *config.Config, logger *zap.SugaredLogger) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Port))
	if err != nil {
		logger.Fatalw("failed to listen", err)
	}

	repo, err := repository.NewMongoRepository(ctx, cfg.MongoDB)
	if err != nil {
		logger.Fatalw("failed to connect to mongo", err)
	}

	// create rabbitmq connection
	rabbitConn, err := amqp091.Dial(fmt.Sprintf("amqp://%s:%s@%s:5672/", cfg.RabbitMQ.Username, cfg.RabbitMQ.Password, cfg.RabbitMQ.Host))
	if err != nil {
		logger.Fatalw("failed to connect to rabbitmq", err)
		return
	}

	notif, err := notifier.NewRabbitMqNotifier(logger, rabbitConn)
	if err != nil {
		logger.Fatalw("failed to create rabbitmq notifier", err)
		return
	}

	s := grpc.NewServer(grpc.ChainUnaryInterceptor(
		grpc_zap.UnaryServerInterceptor(logger.Desugar(), grpc_zap.WithLevels(func(code codes.Code) zapcore.Level {
			if code != codes.Internal && code != codes.Unavailable && code != codes.Unknown {
				return zapcore.DebugLevel
			} else {
				return zapcore.ErrorLevel
			}
		})),
	))

	partySvc := service.NewPartyService(notif, repo)
	partySettingsSvc := service.NewPartySettingsService(repo)

	party.RegisterPartyServiceServer(s, partySvc)
	party.RegisterPartySettingsServiceServer(s, partySettingsSvc)

	// Create health probe
	healthSvc := health.NewHealthService()
	healthSvc.AddProbe("repository", func(ctx context.Context) health.HealthStatus {
		err := repo.HealthCheck(ctx, time.Second*5)
		if err != nil {
			logger.Errorw("failed to health check repository", err)
			return health.HealthStatusUnhealthy
		}
		return health.HealthStatusHealthy
	})

	// Register health probe
	health.RegisterHealthServiceServer(s, healthSvc)

	logger.Infow("starting server", "port", cfg.Port)

	// Blocking
	err = s.Serve(lis)
	if err != nil {
		logger.Fatalw("failed to serve", err)
	}
}
