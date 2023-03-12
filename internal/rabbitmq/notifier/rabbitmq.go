package notifier

import (
	"context"
	pbmsg "github.com/emortalmc/proto-specs/gen/go/message/party"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"party-manager/internal/repository/model"
	"time"
)

const exchange = "party-manager"

// routingKeyFormat in the format of <action>
//const routingKeyFormat = "%s"

type rabbitMqNotifier struct {
	Notifier

	logger  *zap.SugaredLogger
	channel *amqp.Channel
}

func NewRabbitMqNotifier(logger *zap.SugaredLogger, conn *amqp.Connection) (Notifier, error) {
	channel, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	return &rabbitMqNotifier{
		logger:  logger,
		channel: channel,
	}, nil
}

func (r *rabbitMqNotifier) PartyCreated(ctx context.Context, party *model.Party) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	msg := &pbmsg.PartyCreatedMessage{
		Party: party.ToProto(),
	}

	body, err := proto.Marshal(msg)
	if err != nil {
		r.logger.Errorw("failed to marshal party created message", err)
		return
	}

	err = r.channel.PublishWithContext(ctx, exchange, "party_created", false, false, amqp.Publishing{
		ContentType: "application/x-protobuf",
		Timestamp:   party.Id.Timestamp(),
		Type:        string(msg.ProtoReflect().Descriptor().FullName()),
		Body:        body,
	})

	if err != nil {
		r.logger.Errorw("failed to publish party created message", err)
		return
	}
}

func (r *rabbitMqNotifier) PartyDeleted(ctx context.Context, party *model.Party) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	msg := &pbmsg.PartyDeletedMessage{
		Party: party.ToProto(),
	}

	body, err := proto.Marshal(msg)
	if err != nil {
		r.logger.Errorw("failed to marshal party deleted message", err)
		return
	}

	err = r.channel.PublishWithContext(ctx, exchange, "party_deleted", false, false, amqp.Publishing{
		ContentType: "application/x-protobuf",
		Timestamp:   party.Id.Timestamp(),
		Type:        string(msg.ProtoReflect().Descriptor().FullName()),
		Body:        body,
	})

	if err != nil {
		r.logger.Errorw("failed to publish party deleted message", err)
		return
	}
}

func (r *rabbitMqNotifier) PartyEmptied(ctx context.Context, party *model.Party) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	msg := &pbmsg.PartyEmptiedMessage{
		Party: party.ToProto(),
	}

	body, err := proto.Marshal(msg)
	if err != nil {
		r.logger.Errorw("failed to marshal party emptied message", err)
		return
	}

	err = r.channel.PublishWithContext(ctx, exchange, "party_emptied", false, false, amqp.Publishing{
		ContentType: "application/x-protobuf",
		Timestamp:   time.Now(),
		Type:        string(msg.ProtoReflect().Descriptor().FullName()),
		Body:        body,
	})

	if err != nil {
		r.logger.Errorw("failed to publish party emptied message", err)
		return
	}
}

func (r *rabbitMqNotifier) PartyOpenChanged(ctx context.Context, partyId primitive.ObjectID, open bool) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	msg := &pbmsg.PartyOpenChangedMessage{
		PartyId: partyId.Hex(),
		Open:    open,
	}

	body, err := proto.Marshal(msg)
	if err != nil {
		r.logger.Errorw("failed to marshal party open changed message", err)
		return
	}

	err = r.channel.PublishWithContext(ctx, exchange, "party_open_changed", false, false, amqp.Publishing{
		ContentType: "application/x-protobuf",
		Timestamp:   time.Now(),
		Type:        string(msg.ProtoReflect().Descriptor().FullName()),
		Body:        body,
	})

	if err != nil {
		r.logger.Errorw("failed to publish party open changed message", err)
		return
	}
}

func (r *rabbitMqNotifier) PartyInviteCreated(ctx context.Context, invite *model.PartyInvite) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	msg := &pbmsg.PartyInviteCreatedMessage{
		Invite: invite.ToProto(),
	}

	body, err := proto.Marshal(msg)
	if err != nil {
		r.logger.Errorw("failed to marshal party invite created message", err)
		return
	}

	err = r.channel.PublishWithContext(ctx, exchange, "party_invite_created", false, false, amqp.Publishing{
		ContentType: "application/x-protobuf",
		Timestamp:   time.Now(),
		Type:        string(msg.ProtoReflect().Descriptor().FullName()),
		Body:        body,
	})

	if err != nil {
		r.logger.Errorw("failed to publish party invite created message", err)
		return
	}
}

func (r *rabbitMqNotifier) PartyPlayerJoined(ctx context.Context, partyId primitive.ObjectID, player *model.PartyMember) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	msg := &pbmsg.PartyPlayerJoinedMessage{
		PartyId: partyId.Hex(),
		Member:  player.ToProto(),
	}

	body, err := proto.Marshal(msg)
	if err != nil {
		r.logger.Errorw("failed to marshal party player joined message", err)
		return
	}

	err = r.channel.PublishWithContext(ctx, exchange, "party_player_joined", false, false, amqp.Publishing{
		ContentType: "application/x-protobuf",
		Timestamp:   time.Now(),
		Type:        string(msg.ProtoReflect().Descriptor().FullName()),
		Body:        body,
	})

	if err != nil {
		r.logger.Errorw("failed to publish party player joined message", err)
		return
	}
}

func (r *rabbitMqNotifier) PartyPlayerLeft(ctx context.Context, partyId primitive.ObjectID, player *model.PartyMember) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	msg := &pbmsg.PartyPlayerLeftMessage{
		PartyId: partyId.Hex(),
		Member:  player.ToProto(),
	}

	body, err := proto.Marshal(msg)
	if err != nil {
		r.logger.Errorw("failed to marshal party player left message", err)
		return
	}

	err = r.channel.PublishWithContext(ctx, exchange, "party_player_left", false, false, amqp.Publishing{
		ContentType: "application/x-protobuf",
		Timestamp:   time.Now(),
		Type:        string(msg.ProtoReflect().Descriptor().FullName()),
		Body:        body,
	})

	if err != nil {
		r.logger.Errorw("failed to publish party player left message", err)
		return
	}
}

func (r *rabbitMqNotifier) PartyPlayerKicked(ctx context.Context, partyId primitive.ObjectID, target *model.PartyMember, kicker *model.PartyMember) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	msg := &pbmsg.PartyPlayerLeftMessage{
		PartyId:  partyId.Hex(),
		Member:   target.ToProto(),
		KickedBy: kicker.ToProto(),
	}

	body, err := proto.Marshal(msg)
	if err != nil {
		r.logger.Errorw("failed to marshal party player kicked message", err)
		return
	}

	err = r.channel.PublishWithContext(ctx, exchange, "party_player_left", false, false, amqp.Publishing{
		ContentType: "application/x-protobuf",
		Timestamp:   time.Now(),
		Type:        string(msg.ProtoReflect().Descriptor().FullName()),
		Body:        body,
	})

	if err != nil {
		r.logger.Errorw("failed to publish party player kicked message", err)
		return
	}
}

func (r *rabbitMqNotifier) PartyLeaderChanged(ctx context.Context, partyId primitive.ObjectID, player *model.PartyMember) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	msg := &pbmsg.PartyLeaderChangedMessage{
		PartyId:   partyId.Hex(),
		NewLeader: player.ToProto(),
	}

	body, err := proto.Marshal(msg)
	if err != nil {
		r.logger.Errorw("failed to marshal party leader changed message", err)
		return
	}

	err = r.channel.PublishWithContext(ctx, exchange, "party_leader_changed", false, false, amqp.Publishing{
		ContentType: "application/x-protobuf",
		Timestamp:   time.Now(),
		Type:        string(msg.ProtoReflect().Descriptor().FullName()),
		Body:        body,
	})

	if err != nil {
		r.logger.Errorw("failed to publish party leader changed message", err)
		return
	}
}

func (r *rabbitMqNotifier) PartySettingsChanged(ctx context.Context, playerId uuid.UUID, settings *model.PartySettings) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	msg := &pbmsg.PartySettingsChangedMessage{
		PlayerId: playerId.String(),
		Settings: settings.ToProto(),
	}

	body, err := proto.Marshal(msg)
	if err != nil {
		r.logger.Errorw("failed to marshal party settings changed message", err)
		return
	}

	err = r.channel.PublishWithContext(ctx, exchange, "party_settings_changed", false, false, amqp.Publishing{
		ContentType: "application/x-protobuf",
		Timestamp:   time.Now(),
		Type:        string(msg.ProtoReflect().Descriptor().FullName()),
		Body:        body,
	})

	if err != nil {
		r.logger.Errorw("failed to publish party settings changed message", err)
		return
	}
}
