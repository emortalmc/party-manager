package notifier

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
)

const topic = "party-manager"

type kafkaNotifier struct {
	logger *zap.SugaredLogger
	w      *kafka.Writer
}

// TODO check if the broker is alive before we create this notifier
func NewKafkaNotifier(logger *zap.SugaredLogger, cfg *config.KafkaConfig) Notifier {
	w := &kafka.Writer{
		Addr:      kafka.TCP(fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)),
		Topic:     topic,
		Balancer:  &kafka.LeastBytes{},
		BatchSize: 1,
	}

	return &kafkaNotifier{
		logger: logger,
		w:      w,
	}
}

func (k *kafkaNotifier) PartyCreated(ctx context.Context, party *model.Party) {
	msg := &partymsg.PartyCreatedMessage{Party: party.ToProto()}
	bytes, err := proto.Marshal(msg)
	if err != nil {
		k.logger.Errorw("failed to marshal message", "err", err)
		return
	}

	err = k.w.WriteMessages(ctx, kafka.Message{
		Value:   bytes,
		Headers: []kafka.Header{{Key: "X-Proto-Type", Value: []byte(msg.ProtoReflect().Descriptor().FullName())}},
	})
	if err != nil {
		k.logger.Errorw("failed to write message", "err", err)
		return
	}
}

func (k *kafkaNotifier) PartyDeleted(ctx context.Context, party *model.Party) {
	msg := &partymsg.PartyDeletedMessage{Party: party.ToProto()}
	bytes, err := proto.Marshal(msg)
	if err != nil {
		k.logger.Errorw("failed to marshal message", "err", err)
		return
	}

	err = k.w.WriteMessages(ctx, kafka.Message{
		Value:   bytes,
		Headers: []kafka.Header{{Key: "X-Proto-Type", Value: []byte(msg.ProtoReflect().Descriptor().FullName())}},
	})
	if err != nil {
		k.logger.Errorw("failed to write message", "err", err)
		return
	}
}

func (k *kafkaNotifier) PartyEmptied(ctx context.Context, party *model.Party) {
	msg := &partymsg.PartyEmptiedMessage{Party: party.ToProto()}
	bytes, err := proto.Marshal(msg)
	if err != nil {
		k.logger.Errorw("failed to marshal message", "err", err)
		return
	}

	err = k.w.WriteMessages(ctx, kafka.Message{
		Value:   bytes,
		Headers: []kafka.Header{{Key: "X-Proto-Type", Value: []byte(msg.ProtoReflect().Descriptor().FullName())}},
	})
	if err != nil {
		k.logger.Errorw("failed to write message", "err", err)
		return
	}
}

func (k *kafkaNotifier) PartyOpenChanged(ctx context.Context, partyId primitive.ObjectID, open bool) {
	msg := &partymsg.PartyOpenChangedMessage{PartyId: partyId.Hex(), Open: open}
	bytes, err := proto.Marshal(msg)
	if err != nil {
		k.logger.Errorw("failed to marshal message", "err", err)
		return
	}

	err = k.w.WriteMessages(ctx, kafka.Message{
		Value:   bytes,
		Headers: []kafka.Header{{Key: "X-Proto-Type", Value: []byte(msg.ProtoReflect().Descriptor().FullName())}},
	})
	if err != nil {
		k.logger.Errorw("failed to write message", "err", err)
		return
	}
}

func (k *kafkaNotifier) PartyInviteCreated(ctx context.Context, invite *model.PartyInvite) {
	msg := &partymsg.PartyInviteCreatedMessage{Invite: invite.ToProto()}
	bytes, err := proto.Marshal(msg)
	if err != nil {
		k.logger.Errorw("failed to marshal message", "err", err)
		return
	}

	err = k.w.WriteMessages(ctx, kafka.Message{
		Value:   bytes,
		Headers: []kafka.Header{{Key: "X-Proto-Type", Value: []byte(msg.ProtoReflect().Descriptor().FullName())}},
	})
	if err != nil {
		k.logger.Errorw("failed to write message", "err", err)
		return
	}
}

func (k *kafkaNotifier) PartyPlayerJoined(ctx context.Context, partyId primitive.ObjectID, player *model.PartyMember) {
	msg := &partymsg.PartyPlayerJoinedMessage{PartyId: partyId.Hex(), Member: player.ToProto()}
	bytes, err := proto.Marshal(msg)
	if err != nil {
		k.logger.Errorw("failed to marshal message", "err", err)
		return
	}

	err = k.w.WriteMessages(ctx, kafka.Message{
		Value:   bytes,
		Headers: []kafka.Header{{Key: "X-Proto-Type", Value: []byte(msg.ProtoReflect().Descriptor().FullName())}},
	})
	if err != nil {
		k.logger.Errorw("failed to write message", "err", err)
		return
	}
}

func (k *kafkaNotifier) PartyPlayerLeft(ctx context.Context, partyId primitive.ObjectID, player *model.PartyMember) {
	msg := &partymsg.PartyPlayerLeftMessage{PartyId: partyId.Hex(), Member: player.ToProto()}
	bytes, err := proto.Marshal(msg)
	if err != nil {
		k.logger.Errorw("failed to marshal message", "err", err)
		return
	}

	err = k.w.WriteMessages(ctx, kafka.Message{
		Value:   bytes,
		Headers: []kafka.Header{{Key: "X-Proto-Type", Value: []byte(msg.ProtoReflect().Descriptor().FullName())}},
	})
	if err != nil {
		k.logger.Errorw("failed to write message", "err", err)
		return
	}
}

func (k *kafkaNotifier) PartyPlayerKicked(ctx context.Context, partyId primitive.ObjectID, kicked *model.PartyMember, kicker *model.PartyMember) {
	msg := &partymsg.PartyPlayerLeftMessage{PartyId: partyId.Hex(), Member: kicked.ToProto(), KickedBy: kicker.ToProto()}
	bytes, err := proto.Marshal(msg)
	if err != nil {
		k.logger.Errorw("failed to marshal message", "err", err)
		return
	}

	err = k.w.WriteMessages(ctx, kafka.Message{
		Value:   bytes,
		Headers: []kafka.Header{{Key: "X-Proto-Type", Value: []byte(msg.ProtoReflect().Descriptor().FullName())}},
	})
	if err != nil {
		k.logger.Errorw("failed to write message", "err", err)
		return
	}
}

func (k *kafkaNotifier) PartyLeaderChanged(ctx context.Context, partyId primitive.ObjectID, newLeader *model.PartyMember) {
	msg := &partymsg.PartyLeaderChangedMessage{PartyId: partyId.Hex(), NewLeader: newLeader.ToProto()}
	bytes, err := proto.Marshal(msg)
	if err != nil {
		k.logger.Errorw("failed to marshal message", "err", err)
		return
	}

	err = k.w.WriteMessages(ctx, kafka.Message{
		Value:   bytes,
		Headers: []kafka.Header{{Key: "X-Proto-Type", Value: []byte(msg.ProtoReflect().Descriptor().FullName())}},
	})
	if err != nil {
		k.logger.Errorw("failed to write message", "err", err)
		return
	}
}

func (k *kafkaNotifier) PartySettingsChanged(ctx context.Context, playerId uuid.UUID, settings *model.PartySettings) {
	msg := &partymsg.PartySettingsChangedMessage{PlayerId: playerId.String(), Settings: settings.ToProto()}
	bytes, err := proto.Marshal(msg)
	if err != nil {
		k.logger.Errorw("failed to marshal message", "err", err)
		return
	}

	err = k.w.WriteMessages(ctx, kafka.Message{
		Value:   bytes,
		Headers: []kafka.Header{{Key: "X-Proto-Type", Value: []byte(msg.ProtoReflect().Descriptor().FullName())}},
	})
	if err != nil {
		k.logger.Errorw("failed to write message", "err", err)
		return
	}
}
