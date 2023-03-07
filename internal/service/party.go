package service

import (
	"context"
	"fmt"
	pb "github.com/emortalmc/proto-specs/gen/go/grpc/party"
	pbmodel "github.com/emortalmc/proto-specs/gen/go/model/party"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"party-manager/internal/rabbitmq/notifier"
	"party-manager/internal/repository"
	"party-manager/internal/repository/model"
	"time"
)

type partyService struct {
	pb.PartyServiceServer

	notif notifier.Notifier
	repo  repository.Repository
}

func NewPartyService(notifier notifier.Notifier, repo repository.Repository) pb.PartyServiceServer {
	return &partyService{
		notif: notifier,
		repo:  repo,
	}
}

var (
	partyNotFoundErr    = status.New(codes.NotFound, "party not found").Err()
	playerNotInPartyErr = status.New(codes.NotFound, "player not in party").Err()

	createAlreadyInPartyErr = panicIfErr(status.New(codes.AlreadyExists, "player is already in a party").
				WithDetails(&pb.CreatePartyErrorResponse{ErrorType: pb.CreatePartyErrorResponse_ALREADY_IN_PARTY})).Err()
)

func invalidFieldErr(field string) error {
	return status.New(codes.InvalidArgument, fmt.Sprintf("invalid %s", field)).Err()
}

func (p *partyService) CreateParty(ctx context.Context, request *pb.CreatePartyRequest) (*pb.CreatePartyResponse, error) {
	pId, err := uuid.Parse(request.OwnerId)
	if err != nil {
		return nil, invalidFieldErr("owner_id")
	}

	isInParty, err := p.repo.IsInParty(ctx, pId)
	if err != nil {
		return nil, err
	}

	if isInParty {
		return nil, createAlreadyInPartyErr
	}

	party := &model.Party{
		LeaderId: pId,
		Members:  []*model.PartyMember{{PlayerId: pId, Username: request.OwnerUsername}},
	}

	// NOTE: The ID of the party is updated after the party is created by the repository.
	err = p.repo.CreateParty(ctx, party)
	if err != nil {
		return nil, err
	}

	p.notif.PartyCreated(ctx, party)

	return &pb.CreatePartyResponse{
		Party: party.ToProto(),
	}, nil
}

var (
	disbandNotLeaderErr = panicIfErr(status.New(codes.PermissionDenied, "player is not the party leader").
				WithDetails(&pb.DisbandPartyErrorResponse{ErrorType: pb.DisbandPartyErrorResponse_NOT_LEADER})).Err()

	disbandNotInPartyErr = panicIfErr(status.New(codes.NotFound, "player is not in a party").
				WithDetails(&pb.DisbandPartyErrorResponse{ErrorType: pb.DisbandPartyErrorResponse_NOT_IN_PARTY})).Err()
)

func (p *partyService) DisbandParty(ctx context.Context, request *pb.DisbandPartyRequest) (resp *pb.DisbandPartyResponse, err error) {
	playerId := uuid.Nil
	if request.GetPlayerId() != "" {
		playerId, err = uuid.Parse(request.GetPlayerId())
		if err != nil {
			return nil, invalidFieldErr("player id")
		}
	}

	var party *model.Party
	if request.GetPartyId() != "" {
		partyId, err := primitive.ObjectIDFromHex(request.GetPartyId())
		if err != nil {
			return nil, invalidFieldErr("party id")
		}
		party, err = p.repo.GetPartyById(ctx, partyId)
		if err != nil {
			if err == mongo.ErrNoDocuments {
				return nil, disbandNotInPartyErr
			}
			return nil, err
		}
	} else {
		party, err = p.repo.GetPartyByMemberId(ctx, playerId)
		if err != nil {
			if err == mongo.ErrNoDocuments {
				return nil, disbandNotInPartyErr
			}
			return nil, err
		}
	}

	// Perform permission checks as request is made on behalf of a player.
	if playerId != uuid.Nil {
		if party.LeaderId != playerId {
			return nil, disbandNotLeaderErr
		}
	}

	err = p.repo.DeleteParty(ctx, party.Id)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, disbandNotInPartyErr
		}
		return nil, err
	}

	err = p.repo.DeletePartyInvitesByPartyId(ctx, party.Id)
	if err != nil {
		return nil, err
	}

	p.notif.PartyDisbanded(ctx, party)

	return &pb.DisbandPartyResponse{}, nil
}

func (p *partyService) GetParty(ctx context.Context, request *pb.GetPartyRequest) (*pb.GetPartyResponse, error) {
	if request.GetPartyId() != "" {
		partyId, err := primitive.ObjectIDFromHex(request.GetPartyId())
		if err != nil {
			return nil, invalidFieldErr("party_id")
		}
		party, err := p.repo.GetPartyById(ctx, partyId)
		if err != nil {
			if err == mongo.ErrNoDocuments {
				return nil, partyNotFoundErr
			}
			return nil, err
		}
		return &pb.GetPartyResponse{
			Party: party.ToProto(),
		}, nil
	} else {
		playerId, err := uuid.Parse(request.GetPlayerId())
		if err != nil {
			return nil, invalidFieldErr("player_id")
		}

		party, err := p.repo.GetPartyByMemberId(ctx, playerId)
		if err != nil {
			if err == mongo.ErrNoDocuments {
				return nil, playerNotInPartyErr
			}
			return nil, err
		}

		return &pb.GetPartyResponse{
			Party: party.ToProto(),
		}, nil
	}
}

func (p *partyService) GetPartyInvites(ctx context.Context, request *pb.GetPartyInvitesRequest) (*pb.GetPartyInvitesResponse, error) {
	var partyId primitive.ObjectID
	var err error

	if request.GetPartyId() != "" {
		partyId, err = primitive.ObjectIDFromHex(request.GetPartyId())
		if err != nil {
			return nil, invalidFieldErr("party_id")
		}
	} else {
		playerId, err := uuid.Parse(request.GetPlayerId())
		if err != nil {
			return nil, invalidFieldErr("player_id")
		}

		partyId, err = p.repo.GetPartyIdByMemberId(ctx, playerId)
		if err != nil {
			if err == mongo.ErrNoDocuments {
				return nil, playerNotInPartyErr
			}
			return nil, err
		}
	}

	invites, err := p.repo.GetPartyInvitesByPartyId(ctx, partyId)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, partyNotFoundErr
		}
		return nil, err
	}

	inviteProtos := make([]*pbmodel.PartyInvite, 0, len(invites))
	for _, invite := range invites {
		inviteProtos = append(inviteProtos, invite.ToProto())
	}

	return &pb.GetPartyInvitesResponse{
		Invites: inviteProtos,
	}, nil
}

var (
	inviteMustBeLeaderErr = status.New(codes.PermissionDenied, "player must be leader").Err()

	inviteAlreadyInvitedErr = panicIfErr(status.New(codes.AlreadyExists, "player is already invited").
				WithDetails(&pb.InvitePlayerErrorResponse{ErrorType: pb.InvitePlayerErrorResponse_TARGET_ALREADY_INVITED})).Err()

	errInvitePartyIsOpen = panicIfErr(status.New(codes.FailedPrecondition, "party is open").
				WithDetails(&pb.InvitePlayerErrorResponse{ErrorType: pb.InvitePlayerErrorResponse_PARTY_IS_OPEN})).Err()

	inviteTargetInSelfPartyErr = panicIfErr(status.New(codes.AlreadyExists, "target is already in the party").
					WithDetails(&pb.InvitePlayerErrorResponse{ErrorType: pb.InvitePlayerErrorResponse_TARGET_ALREADY_IN_SELF_PARTY})).Err()

	inviteTargetInOtherPartyErr = panicIfErr(status.New(codes.AlreadyExists, "target is already in another party").
					WithDetails(&pb.InvitePlayerErrorResponse{ErrorType: pb.InvitePlayerErrorResponse_TARGET_ALREADY_IN_ANOTHER_PARTY})).Err()
)

// InvitePlayer invites a player to the party of the inviter
// If the inviter is not in a party, one is implicitly created
func (p *partyService) InvitePlayer(ctx context.Context, request *pb.InvitePlayerRequest) (*pb.InvitePlayerResponse, error) {
	issuerId, err := uuid.Parse(request.IssuerId)
	if err != nil {
		return nil, invalidFieldErr("issuer_id")
	}

	targetId, err := uuid.Parse(request.TargetId)
	if err != nil {
		return nil, invalidFieldErr("target_id")
	}

	// Get party of the inviter
	party, err := p.repo.GetPartyByMemberId(ctx, issuerId)
	if err != nil {
		if err != mongo.ErrNoDocuments {
			return nil, err
		}

		// implicitly create a party
		party = &model.Party{
			LeaderId: issuerId,
			Members: []*model.PartyMember{
				{
					PlayerId: issuerId,
					Username: request.IssuerUsername,
				},
			},
		}
		err = p.repo.CreateParty(ctx, party)
		if err != nil {
			return nil, err
		}

		p.notif.PartyCreated(ctx, party)
	}

	settings, err := p.getPartySettingsOrDefault(ctx, party.LeaderId)
	if err != nil {
		return nil, err
	}

	if settings.Open {
		return nil, errInvitePartyIsOpen
	}

	if party.LeaderId != issuerId {
		// TODO check permissions since they aren't the leader

		if !settings.AllowMemberInvite {
			return nil, inviteMustBeLeaderErr
		}
	}

	// Check if the target is already in a party
	targetPartyId, err := p.repo.GetPartyIdByMemberId(ctx, targetId)
	if err == nil {
		if targetPartyId == party.Id {
			return nil, inviteTargetInSelfPartyErr
		} else {
			return nil, inviteTargetInOtherPartyErr
		}
	}

	if err != mongo.ErrNoDocuments {
		return nil, err
	}

	// Check if the target is already invited
	inviteExists, err := p.repo.DoesPartyInviteExist(ctx, party.Id, targetId)
	if err != nil {
		return nil, err
	}
	if inviteExists {
		return nil, inviteAlreadyInvitedErr
	}

	// Create invite
	invite := &model.PartyInvite{
		PartyId: party.Id,

		InviterId:       issuerId,
		InviterUsername: request.IssuerUsername,

		TargetId:       targetId,
		TargetUsername: request.TargetUsername,

		ExpiresAt: time.Now().In(time.UTC).Add(5 * time.Minute),
	}

	// Save invite
	err = p.repo.CreatePartyInvite(ctx, invite)
	if err != nil {
		return nil, err
	}

	p.notif.PartyInviteCreated(ctx, invite)

	return &pb.InvitePlayerResponse{
		Invite: invite.ToProto(),
	}, nil
}

var (
	joinPartyAlreadyInPartyErr = panicIfErr(status.New(codes.AlreadyExists, "player is already in a party").
					WithDetails(&pb.JoinPartyErrorResponse{ErrorType: pb.JoinPartyErrorResponse_ALREADY_IN_PARTY})).Err()

	joinPartyPartyNotFoundErr = panicIfErr(status.New(codes.NotFound, "party not found").
					WithDetails(&pb.JoinPartyErrorResponse{ErrorType: pb.JoinPartyErrorResponse_PARTY_NOT_FOUND})).Err()

	joinPartyNotInvitedErr = panicIfErr(status.New(codes.PermissionDenied, "player is not invited to the party").
				WithDetails(&pb.JoinPartyErrorResponse{ErrorType: pb.JoinPartyErrorResponse_NOT_INVITED})).Err()
)

// TODO the database calls here are a bit messy. Can we clean them up?
func (p *partyService) JoinParty(ctx context.Context, request *pb.JoinPartyRequest) (*pb.JoinPartyResponse, error) {
	playerId, err := uuid.Parse(request.PlayerId)
	if err != nil {
		return nil, invalidFieldErr("player_id")
	}

	// Check if the player is already in a party
	isInParty, err := p.repo.IsInParty(ctx, playerId)
	if err != nil {
		return nil, err
	}

	if isInParty {
		return nil, joinPartyAlreadyInPartyErr
	}

	// Get the target party
	var party *model.Party
	if request.GetPartyId() != "" {
		targetPartyId, err := primitive.ObjectIDFromHex(request.GetPartyId())
		if err != nil {
			return nil, invalidFieldErr("party_id")
		}

		party, err = p.repo.GetPartyById(ctx, targetPartyId)
		if err != nil {
			return nil, err
		}
	} else if request.GetMemberId() != "" {
		targetMemberId, err := uuid.Parse(request.GetMemberId())
		if err != nil {
			return nil, invalidFieldErr("member_id")
		}

		party, err = p.repo.GetPartyByMemberId(ctx, targetMemberId)
		if err != nil {
			if err == mongo.ErrNoDocuments {
				return nil, joinPartyPartyNotFoundErr
			}
			return nil, err
		}
	} else {
		return nil, status.Error(codes.InvalidArgument, "party_id or member_id must be specified")
	}

	settings, err := p.getPartySettingsOrDefault(ctx, party.LeaderId)
	if err != nil {
		return nil, err
	}

	// Try revoke the invite if it exists, handle the error if it doesn't
	err = p.repo.DeletePartyInvite(ctx, party.Id, playerId)
	if err != nil {
		// ignore a mongo.ErrNoDocuments error if the party is open
		if err == mongo.ErrNoDocuments && !settings.Open {
			return nil, joinPartyNotInvitedErr
		} else if err != mongo.ErrNoDocuments {
			return nil, err
		}
	}

	newPartyMember := &model.PartyMember{
		PlayerId: playerId,
		Username: request.PlayerUsername,
	}

	// Add the player to the party
	err = p.repo.AddPartyMember(ctx, party.Id, newPartyMember)
	if err != nil {
		return nil, err
	}

	p.notif.PartyPlayerJoined(ctx, party.Id, newPartyMember)

	return &pb.JoinPartyResponse{
		Party: party.ToProto(),
	}, nil
}

var (
	leaveNotInPartyErr = panicIfErr(status.New(codes.NotFound, "player is not in a party").
				WithDetails(&pb.LeavePartyErrorResponse{ErrorType: pb.LeavePartyErrorResponse_NOT_IN_PARTY})).Err()

	leaveIsLeaderErr = panicIfErr(status.New(codes.FailedPrecondition, "player is the leader of the party").
				WithDetails(&pb.LeavePartyErrorResponse{ErrorType: pb.LeavePartyErrorResponse_CANNOT_LEAVE_AS_LEADER})).Err()
)

func (p *partyService) LeaveParty(ctx context.Context, request *pb.LeavePartyRequest) (*pb.LeavePartyResponse, error) {
	playerId, err := uuid.Parse(request.PlayerId)
	if err != nil {
		return nil, invalidFieldErr("player_id")
	}

	party, err := p.repo.GetPartyByMemberId(ctx, playerId)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, leaveNotInPartyErr
		}
		return nil, err
	}

	if party.LeaderId == playerId {
		return nil, leaveIsLeaderErr
	}

	// Get the party id
	err = p.repo.RemoveMemberFromSelfParty(ctx, playerId)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, leaveNotInPartyErr
		}
		return nil, err
	}

	member, ok := party.GetMember(playerId)
	if !ok {
		return nil, status.Error(codes.Internal, "Couldn't find player in party they should definitely be in")
	}

	p.notif.PartyPlayerLeft(ctx, party.Id, member)

	return &pb.LeavePartyResponse{}, nil
}

var (
	kickNotInPartyErr = panicIfErr(status.New(codes.NotFound, "issuer is not in a party").
				WithDetails(&pb.KickPlayerErrorResponse{ErrorType: pb.KickPlayerErrorResponse_SELF_NOT_IN_PARTY})).Err()

	kickNotLeaderErr = panicIfErr(status.New(codes.FailedPrecondition, "issuer is not the leader of the party").
				WithDetails(&pb.KickPlayerErrorResponse{ErrorType: pb.KickPlayerErrorResponse_SELF_NOT_LEADER})).Err()

	kickTargetIsLeaderErr = panicIfErr(status.New(codes.FailedPrecondition, "target is the leader of the party").
				WithDetails(&pb.KickPlayerErrorResponse{ErrorType: pb.KickPlayerErrorResponse_TARGET_IS_LEADER})).Err()

	kickTargetNotInPartyErr = panicIfErr(status.New(codes.FailedPrecondition, "target is not in the party").
				WithDetails(&pb.KickPlayerErrorResponse{ErrorType: pb.KickPlayerErrorResponse_TARGET_NOT_IN_PARTY})).Err()
)

func (p *partyService) KickPlayer(ctx context.Context, request *pb.KickPlayerRequest) (*pb.KickPlayerResponse, error) {
	issuerId, err := uuid.Parse(request.IssuerId)
	if err != nil {
		return nil, invalidFieldErr("issuer_id")
	}

	targetId, err := uuid.Parse(request.TargetId)
	if err != nil {
		return nil, invalidFieldErr("target_id")
	}

	// Get the party as we are going to do many checks
	party, err := p.repo.GetPartyByMemberId(ctx, issuerId)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, kickNotInPartyErr
		}
		return nil, err
	}

	// Check if the issuer is the party leader
	if party.LeaderId != issuerId {
		return nil, kickNotLeaderErr
	}

	// Check if the target is the party leader
	if party.LeaderId == targetId {
		return nil, kickTargetIsLeaderErr
	}

	// Check if the target is in the party
	if !party.ContainsMember(targetId) {
		return nil, kickTargetNotInPartyErr
	}

	err = p.repo.RemoveMemberFromParty(ctx, party.Id, targetId)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, kickTargetNotInPartyErr
		}
		return nil, err
	}

	var targetMember *model.PartyMember
	for _, member := range party.Members {
		if member.PlayerId == targetId {
			targetMember = member
			break
		}
	}

	var issuerMember *model.PartyMember
	for _, member := range party.Members {
		if member.PlayerId == issuerId {
			issuerMember = member
			break
		}
	}

	p.notif.PartyPlayerKicked(ctx, party.Id, targetMember, issuerMember)

	return &pb.KickPlayerResponse{}, nil
}

var (
	setLeaderSelfNotInPartyErr = panicIfErr(status.New(codes.NotFound, "issuer is not in a party").
					WithDetails(&pb.SetPartyLeaderErrorResponse{ErrorType: pb.SetPartyLeaderErrorResponse_SELF_NOT_IN_PARTY})).Err()

	setLeaderSelfNotLeaderErr = panicIfErr(status.New(codes.FailedPrecondition, "issuer is not the leader of the party").
					WithDetails(&pb.SetPartyLeaderErrorResponse{ErrorType: pb.SetPartyLeaderErrorResponse_SELF_NOT_LEADER})).Err()

	// setLeaderTargetNotInPartyErr this only means they are not in the same party, they may be in another party
	setLeaderTargetNotInPartyErr = panicIfErr(status.New(codes.FailedPrecondition, "target is not in the party").
					WithDetails(&pb.SetPartyLeaderErrorResponse{ErrorType: pb.SetPartyLeaderErrorResponse_TARGET_NOT_IN_PARTY})).Err()
)

func (p *partyService) SetPartyLeader(ctx context.Context, request *pb.SetPartyLeaderRequest) (*pb.SetPartyLeaderResponse, error) {
	issuerId, err := uuid.Parse(request.IssuerId)
	if err != nil {
		return nil, invalidFieldErr("issuer_id")
	}

	targetId, err := uuid.Parse(request.TargetId)
	if err != nil {
		return nil, invalidFieldErr("target_id")
	}

	// Get the party as we are going to do many checks
	party, err := p.repo.GetPartyByMemberId(ctx, issuerId)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, setLeaderSelfNotInPartyErr
		}
		return nil, err
	}

	// Check if the issuer is the party leader
	if party.LeaderId != issuerId {
		return nil, setLeaderSelfNotLeaderErr
	}

	// Check if the target is in the party
	if !party.ContainsMember(targetId) {
		return nil, setLeaderTargetNotInPartyErr
	}

	// Set the new leader
	err = p.repo.SetPartyLeader(ctx, party.Id, targetId)
	if err != nil {
		return nil, err
	}

	var newLeaderMember *model.PartyMember
	for _, member := range party.Members {
		if member.PlayerId == targetId {
			newLeaderMember = member
			break
		}
	}

	p.notif.PartyLeaderChanged(ctx, party.Id, newLeaderMember)

	return &pb.SetPartyLeaderResponse{}, nil
}

func (p *partyService) getPartySettingsOrDefault(ctx context.Context, playerId uuid.UUID) (*model.PartySettings, error) {
	settings, err := p.repo.GetPartySettings(ctx, playerId)
	if err != nil {
		if err != mongo.ErrNoDocuments {
			return nil, err
		}
		return model.NewPartySettings(playerId), nil
	}
	return settings, nil
}

func panicIfErr[T any](thing T, err error) T {
	if err != nil {
		panic(err)
	}
	return thing
}
