package service

import (
	"context"
	"fmt"
	"github.com/emortalmc/proto-specs/gen/go/grpc/party"
	grpczap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	"github.com/zakshearman/go-grpc-health/pkg/health"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"net"
	"party-manager/internal/config"
	"party-manager/internal/kafka"
	"party-manager/internal/repository"
	"sync"
)

func RunServices(ctx context.Context, logger *zap.SugaredLogger, wg *sync.WaitGroup, cfg *config.Config,
	repo repository.Repository, notif kafka.Notifier) {

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Port))
	if err != nil {
		logger.Fatalw("failed to listen", "error", err)
	}

	s := grpc.NewServer(grpc.ChainUnaryInterceptor(
		grpczap.UnaryServerInterceptor(logger.Desugar(), grpczap.WithLevels(func(code codes.Code) zapcore.Level {
			if code != codes.Internal && code != codes.Unavailable && code != codes.Unknown {
				return zapcore.DebugLevel
			} else {
				return zapcore.ErrorLevel
			}
		})),
	))

	if cfg.Development {
		reflection.Register(s)
	}

	partySvc := newPartyService(notif, repo)
	partySettingsSvc := newPartySettingsService(repo)

	party.RegisterPartyServiceServer(s, partySvc)
	party.RegisterPartySettingsServiceServer(s, partySettingsSvc)

	// Create health probe
	healthSvc := health.NewHealthService()
	healthSvc.AddProbe("repository", repo.HealthCheck)

	// Register health probe
	health.RegisterHealthServiceServer(s, healthSvc)

	logger.Infow("listening on port", "port", cfg.Port)

	go func() {
		if err := s.Serve(lis); err != nil {
			logger.Fatalw("failed to serve", "error", err)
			return
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		s.GracefulStop()
	}()
}
