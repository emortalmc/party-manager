package listener

import (
	"context"
	"github.com/emortalmc/proto-specs/gen/go/message/common"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"party-manager/internal/rabbitmq/notifier"
	"party-manager/internal/repository"
	"party-manager/internal/repository/model"
)

const queueName = "party-manager"

type listener struct {
	logger *zap.SugaredLogger
	ctx    context.Context

	channel *amqp.Channel
	notif   notifier.Notifier
	repo    repository.Repository
}

func NewListener(logger *zap.SugaredLogger, rabbitMqConn *amqp.Connection, notif notifier.Notifier, repo repository.Repository) error {
	channel, err := rabbitMqConn.Channel()
	if err != nil {
		return err
	}

	l := &listener{
		logger: logger,
		ctx:    context.TODO(),

		channel: channel,
		notif:   notif,
		repo:    repo,
	}

	go l.listen()
	return nil
}

func (l *listener) listen() {
	msgs, err := l.channel.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		l.logger.Errorw("failed to consume messages", err)
		return
	}

	l.logger.Infow("listening for messages from rabbitmq", "queue", queueName)

	for msg := range msgs {
		ok := l.handle(msg)
		if ok {
			err = msg.Ack(false)
		} else {
			err = msg.Reject(false)
		}

		if err != nil {
			l.logger.Errorw("failed to ack or reject message", err)
		}
	}
}

func (l *listener) handle(msg amqp.Delivery) (ok bool) {
	switch msg.Type {
	case string((&common.PlayerDisconnectMessage{}).ProtoReflect().Descriptor().FullName()):
		l.handlePlayerDisconnect(msg)
		return true
	case string((&common.PlayerConnectMessage{}).ProtoReflect().Descriptor().FullName()):
		l.handlePlayerConnect(msg)
		return true
	default:
		l.logger.Warnw("received unknown message type", "routingKey", msg.RoutingKey, "type", msg.Type)
		return false
	}
}

// handlePlayerConnect creates a new party for a player
// when they join the server.
func (l *listener) handlePlayerConnect(delivery amqp.Delivery) {
	msg := &common.PlayerConnectMessage{}

	err := proto.Unmarshal(delivery.Body, msg)
	if err != nil {
		l.logger.Errorw("failed to unmarshal message", err)
		return
	}

	playerId, err := uuid.Parse(msg.PlayerId)
	if err != nil {
		l.logger.Errorw("failed to parse player id", err)
		return
	}

	// create a party for the player
	party := model.NewParty(playerId, msg.PlayerUsername)

	err = l.repo.CreateParty(l.ctx, party)
	if err != nil {
		l.logger.Errorw("failed to create party", err)
		return
	}

	l.notif.PartyCreated(l.ctx, party)
}

func (l *listener) handlePlayerDisconnect(delivery amqp.Delivery) {
	msg := &common.PlayerDisconnectMessage{}

	err := proto.Unmarshal(delivery.Body, msg)
	if err != nil {
		l.logger.Errorw("failed to unmarshal message", err)
		return
	}

	playerId, err := uuid.Parse(msg.PlayerId)
	if err != nil {
		l.logger.Errorw("failed to parse player id", err)
		return
	}

	party, err := l.repo.GetPartyByMemberId(l.ctx, playerId)
	if err != nil {
		l.logger.Errorw("failed to get party by member id", err)
		return
	}

	if playerId == party.LeaderId {
		err = l.repo.DeleteParty(l.ctx, party.Id)
		if err != nil {
			l.logger.Errorw("failed to delete party", err)
			return
		}

		err = l.repo.DeletePartyInvitesByPartyId(l.ctx, party.Id)
		if err != nil {
			l.logger.Errorw("failed to delete party invites", err)
			return
		}

		l.notif.PartyEmptied(l.ctx, party)
		return
	}

	err = l.repo.RemoveMemberFromParty(l.ctx, party.Id, playerId)
	if err != nil {
		l.logger.Errorw("failed to remove member from party", err)
		return
	}

	partyMember, ok := party.GetMember(playerId)
	if !ok {
		l.logger.Errorw("party member not found", "partyId", party.Id, "playerId", playerId)
		return
	}

	l.notif.PartyPlayerLeft(l.ctx, party.Id, partyMember)
}
