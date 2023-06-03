package kafka

import (
	"context"
	"fmt"
	"github.com/emortalmc/proto-specs/gen/go/message/common"
	"github.com/emortalmc/proto-specs/gen/go/nongenerated/kafkautils"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"party-manager/internal/config"
	"party-manager/internal/repository"
	"party-manager/internal/repository/model"
	"sync"
)

const connectionTopic = "mc-connections"

type consumer struct {
	logger *zap.SugaredLogger

	notif Notifier
	repo  repository.Repository

	reader *kafka.Reader
}

func NewConsumer(ctx context.Context, wg *sync.WaitGroup, config *config.KafkaConfig, logger *zap.SugaredLogger,
	notif Notifier, repo repository.Repository) {

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{fmt.Sprintf("%s:%d", config.Host, config.Port)},
		GroupID:     "party-manager",
		GroupTopics: []string{connectionTopic},

		ErrorLogger: kafka.LoggerFunc(func(format string, args ...interface{}) {
			logger.Errorw(fmt.Sprintf(format, args...))
		}),
	})

	c := &consumer{
		logger: logger,
		notif:  notif,
		repo:   repo,
		reader: reader,
	}

	handler := kafkautils.NewConsumerHandler(logger, reader)
	handler.RegisterHandler(&common.PlayerConnectMessage{}, c.handlePlayerConnect)
	handler.RegisterHandler(&common.PlayerDisconnectMessage{}, c.handlePlayerDisconnect)

	logger.Infow("started listening for kafka messages", "topics", reader.Config().GroupTopics)

	wg.Add(1)
	go func() {
		defer wg.Done()
		handler.Run(ctx) // Run is blocking until the context is cancelled
		if err := reader.Close(); err != nil {
			logger.Errorw("failed to close kafka reader", err)
		}
	}()
}

// handlePlayerConnect creates a new party for a player
// when they join the server.
func (c *consumer) handlePlayerConnect(ctx context.Context, _ *kafka.Message, uncastMsg proto.Message) {
	m := uncastMsg.(*common.PlayerConnectMessage)

	playerId, err := uuid.Parse(m.PlayerId)
	if err != nil {
		c.logger.Errorw("failed to parse player id", err)
		return
	}

	// create a party for the player
	party := model.NewParty(playerId, m.PlayerUsername)

	err = c.repo.CreateParty(ctx, party)
	if err != nil {
		c.logger.Errorw("failed to create party", err)
		return
	}

	c.notif.PartyCreated(ctx, party)
}

func (c *consumer) handlePlayerDisconnect(ctx context.Context, _ *kafka.Message, uncastMsg proto.Message) {
	m := uncastMsg.(*common.PlayerDisconnectMessage)

	playerId, err := uuid.Parse(m.PlayerId)
	if err != nil {
		c.logger.Errorw("failed to parse player id", err)
		return
	}

	party, err := c.repo.GetPartyByMemberId(ctx, playerId)
	if err != nil {
		c.logger.Errorw("failed to get party by member id", err)
		return
	}

	if playerId == party.LeaderId {
		if err := c.repo.DeleteParty(ctx, party.Id); err != nil {
			c.logger.Errorw("failed to delete party", err)
			return
		}

		if err := c.repo.DeletePartyInvitesByPartyId(ctx, party.Id); err != nil {
			c.logger.Errorw("failed to delete party invites", err)
			return
		}

		c.notif.PartyDeleted(ctx, party)
		return
	}

	if err := c.repo.RemoveMemberFromParty(ctx, party.Id, playerId); err != nil {
		c.logger.Errorw("failed to remove member from party", err)
		return
	}

	partyMember, ok := party.GetMember(playerId)
	if !ok {
		c.logger.Errorw("party member not found", "partyId", party.Id, "playerId", playerId)
		return
	}

	c.notif.PartyPlayerLeft(ctx, party.Id, partyMember)
}