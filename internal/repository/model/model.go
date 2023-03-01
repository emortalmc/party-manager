package model

import (
	pb "github.com/emortalmc/proto-specs/gen/go/model/party"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

type Party struct {
	Id primitive.ObjectID `bson:"_id"`

	LeaderId uuid.UUID      `bson:"leaderId"`
	Members  []*PartyMember `bson:"members"`
}

func (p *Party) ToProto() *pb.Party {
	memberProtos := make([]*pb.PartyMember, len(p.Members))
	for i, member := range p.Members {
		memberProtos[i] = member.ToProto()
	}

	return &pb.Party{
		Id:       p.Id.Hex(),
		LeaderId: p.LeaderId.String(),
		Members:  memberProtos,
	}
}

func (p *Party) ContainsMember(id uuid.UUID) bool {
	for _, member := range p.Members {
		if member.PlayerId == id {
			return true
		}
	}

	return false
}

type PartyMember struct {
	PlayerId uuid.UUID `bson:"playerId"`
	Username string    `bson:"username"`
}

func (m *PartyMember) ToProto() *pb.PartyMember {
	return &pb.PartyMember{
		Id:       m.PlayerId.String(),
		Username: m.Username,
	}
}

type PartyInvite struct {
	Id      primitive.ObjectID `bson:"_id"`
	PartyId primitive.ObjectID `bson:"partyId"`

	InviterId       uuid.UUID `bson:"inviterId"`
	InviterUsername string    `bson:"inviterUsername"`

	TargetId       uuid.UUID `bson:"targetId"`
	TargetUsername string    `bson:"targetUsername"`

	// ExpiresAt is the time at which the invite expires.
	// Note: This depends on an expiry index being created on MongoDB that reads the expiresAt field.
	ExpiresAt time.Time `bson:"expiresAt"`
}

func (i *PartyInvite) ToProto() *pb.PartyInvite {
	return &pb.PartyInvite{
		PartyId: i.PartyId.Hex(),

		TargetId:       i.TargetId.String(),
		TargetUsername: i.TargetUsername,

		SenderId:       i.InviterId.String(),
		SenderUsername: i.InviterUsername,

		ExpiresAt: timestamppb.New(i.ExpiresAt),
	}
}

type PartySettings struct {
	PlayerId uuid.UUID `bson:"_id"`

	DequeueOnDisconnect bool `bson:"dequeueOnDisconnect"`
	AllowMemberDequeue  bool `bson:"allowMemberDequeue"`
	AllowMemberInvite   bool `bson:"allowMemberInvite"`
	Open                bool `bson:"openParty"`

	// InDb is whether the settings originated from the database or not.
	// If false, the default is probably returned.
	InDb bool `bson:"-"`
}

func NewPartySettings(playerId uuid.UUID) *PartySettings {
	return &PartySettings{
		PlayerId: playerId,

		DequeueOnDisconnect: false,
		AllowMemberDequeue:  false,
		AllowMemberInvite:   false,
		Open:                false,
	}
}

func (s *PartySettings) ToProto() *pb.PartySettings {
	return &pb.PartySettings{
		DequeueOnDisconnect: s.DequeueOnDisconnect,
		AllowMemberDequeue:  s.AllowMemberDequeue,
		AllowMemberInvite:   s.AllowMemberInvite,
		Open:                s.Open,
	}
}
