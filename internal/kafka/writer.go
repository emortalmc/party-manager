package kafka

import (
	"context"
	"fmt"
	partymsg "github.com/emortalmc/proto-specs/gen/go/message/party"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"party-manager/internal/config"
	"party-manager/internal/repository/model"
	"sync"
	"time"
)

const writeTopic = "party-manager"

// Notifier is an interface for sending Kafka messages into the party-manager topic.
//   - The context used should be independent of a request as kafka messages are sent lazily. Using a request context
//     will result in the context being cancelled before the message is sent.
type Notifier interface {
	PartyCreated(ctx context.Context, party *model.Party)
	PartyDeleted(ctx context.Context, party *model.Party)
	PartyEmptied(ctx context.Context, party *model.Party)
	PartyOpenChanged(ctx context.Context, partyId primitive.ObjectID, open bool)
	PartyInviteCreated(ctx context.Context, invite *model.PartyInvite)
	PartyPlayerJoined(ctx context.Context, partyId primitive.ObjectID, player *model.PartyMember)
	PartyPlayerLeft(ctx context.Context, partyId primitive.ObjectID, player *model.PartyMember)
	PartyPlayerKicked(ctx context.Context, partyId primitive.ObjectID, kicked *model.PartyMember, kicker *model.PartyMember)
	PartyLeaderChanged(ctx context.Context, partyId primitive.ObjectID, newLeader *model.PartyMember)

	PartySettingsChanged(ctx context.Context, playerId uuid.UUID, settings *model.PartySettings)
}

type kafkaNotifier struct {
	logger *zap.SugaredLogger
	w      *kafka.Writer
}

func NewKafkaNotifier(ctx context.Context, wg *sync.WaitGroup, cfg *config.KafkaConfig, logger *zap.SugaredLogger) Notifier {
	w := &kafka.Writer{
		Addr:         kafka.TCP(fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)),
		Topic:        writeTopic,
		Balancer:     &kafka.LeastBytes{},
		Async:        true,
		BatchTimeout: 100 * time.Millisecond,
		ErrorLogger:  kafka.LoggerFunc(logger.Errorw),
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		if err := w.Close(); err != nil {
			logger.Errorw("failed to close kafka writer", "err", err)
		}
	}()

	return &kafkaNotifier{
		logger: logger,
		w:      w,
	}
}

func (k *kafkaNotifier) PartyCreated(ctx context.Context, party *model.Party) {
	msg := &partymsg.PartyCreatedMessage{Party: party.ToProto()}

	if err := k.writeMessage(ctx, msg); err != nil {
		k.logger.Errorw("failed to write message", "err", err)
		return
	}
}

func (k *kafkaNotifier) PartyDeleted(ctx context.Context, party *model.Party) {
	msg := &partymsg.PartyDeletedMessage{Party: party.ToProto()}

	if err := k.writeMessage(ctx, msg); err != nil {
		k.logger.Errorw("failed to write message", "err", err)
		return
	}
}

func (k *kafkaNotifier) PartyEmptied(ctx context.Context, party *model.Party) {
	msg := &partymsg.PartyEmptiedMessage{Party: party.ToProto()}

	if err := k.writeMessage(ctx, msg); err != nil {
		k.logger.Errorw("failed to write message", "err", err)
		return
	}
}

func (k *kafkaNotifier) PartyOpenChanged(ctx context.Context, partyId primitive.ObjectID, open bool) {
	msg := &partymsg.PartyOpenChangedMessage{PartyId: partyId.Hex(), Open: open}

	if err := k.writeMessage(ctx, msg); err != nil {
		k.logger.Errorw("failed to write message", "err", err)
		return
	}
}

func (k *kafkaNotifier) PartyInviteCreated(ctx context.Context, invite *model.PartyInvite) {
	msg := &partymsg.PartyInviteCreatedMessage{Invite: invite.ToProto()}

	if err := k.writeMessage(ctx, msg); err != nil {
		k.logger.Errorw("failed to write message", "err", err)
		return
	}
}

func (k *kafkaNotifier) PartyPlayerJoined(ctx context.Context, partyId primitive.ObjectID, player *model.PartyMember) {
	msg := &partymsg.PartyPlayerJoinedMessage{PartyId: partyId.Hex(), Member: player.ToProto()}

	if err := k.writeMessage(ctx, msg); err != nil {
		k.logger.Errorw("failed to write message", "err", err)
		return
	}
}

func (k *kafkaNotifier) PartyPlayerLeft(ctx context.Context, partyId primitive.ObjectID, player *model.PartyMember) {
	msg := &partymsg.PartyPlayerLeftMessage{PartyId: partyId.Hex(), Member: player.ToProto()}

	if err := k.writeMessage(ctx, msg); err != nil {
		k.logger.Errorw("failed to write message", "err", err)
		return
	}
}

func (k *kafkaNotifier) PartyPlayerKicked(ctx context.Context, partyId primitive.ObjectID, kicked *model.PartyMember, kicker *model.PartyMember) {
	msg := &partymsg.PartyPlayerLeftMessage{PartyId: partyId.Hex(), Member: kicked.ToProto(), KickedBy: kicker.ToProto()}

	if err := k.writeMessage(ctx, msg); err != nil {
		k.logger.Errorw("failed to write message", "err", err)
		return
	}
}

func (k *kafkaNotifier) PartyLeaderChanged(ctx context.Context, partyId primitive.ObjectID, newLeader *model.PartyMember) {
	msg := &partymsg.PartyLeaderChangedMessage{PartyId: partyId.Hex(), NewLeader: newLeader.ToProto()}

	if err := k.writeMessage(ctx, msg); err != nil {
		k.logger.Errorw("failed to write message", "err", err)
		return
	}
}

func (k *kafkaNotifier) PartySettingsChanged(ctx context.Context, playerId uuid.UUID, settings *model.PartySettings) {
	msg := &partymsg.PartySettingsChangedMessage{PlayerId: playerId.String(), Settings: settings.ToProto()}

	if err := k.writeMessage(ctx, msg); err != nil {
		k.logger.Errorw("failed to write message", "err", err)
		return
	}
}

func (k *kafkaNotifier) writeMessage(ctx context.Context, msg proto.Message) error {
	bytes, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal proto to bytes: %s", err)
	}

	return k.w.WriteMessages(ctx, kafka.Message{
		Headers: []kafka.Header{{Key: "X-Proto-Type", Value: []byte(msg.ProtoReflect().Descriptor().FullName())}},
		Value:   bytes,
	})
}
