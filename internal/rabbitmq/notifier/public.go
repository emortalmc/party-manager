package notifier

import (
	"context"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"party-manager/internal/repository/model"
	"time"
)

type Notifier interface {
	HealthCheck(ctx context.Context, timeout time.Duration) error

	PartyCreated(ctx context.Context, party *model.Party)
	PartyDeleted(ctx context.Context, party *model.Party)
	PartyEmptied(ctx context.Context, party *model.Party)
	PartyInviteCreated(ctx context.Context, invite *model.PartyInvite)
	PartyPlayerJoined(ctx context.Context, partyId primitive.ObjectID, player *model.PartyMember)
	PartyPlayerLeft(ctx context.Context, partyId primitive.ObjectID, player *model.PartyMember)
	PartyPlayerKicked(ctx context.Context, partyId primitive.ObjectID, kicked *model.PartyMember, kicker *model.PartyMember)
	PartyLeaderChanged(ctx context.Context, partyId primitive.ObjectID, newLeader *model.PartyMember)

	PartySettingsChanged(ctx context.Context, playerId uuid.UUID, settings *model.PartySettings)
}
