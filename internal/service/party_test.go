package service

import (
	"context"
	pb "github.com/emortalmc/proto-specs/gen/go/grpc/party"
	pbmodel "github.com/emortalmc/proto-specs/gen/go/model/party"
	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"party-manager/internal/rabbitmq/notifier"
	"party-manager/internal/repository"
	"party-manager/internal/repository/model"
	"testing"
	"time"
)

var randomUUID = uuid.New()

func TestPartyService_CreateParty(t *testing.T) {
	test := []struct {
		name string
		req  *pb.CreatePartyRequest

		isInPartyReq uuid.UUID
		isInPartyRes bool
		isInPartyErr error

		createPartyInput *model.Party
		createPartyErr   error

		wantResponse bool
		wantErr      error
	}{
		{
			name: "success",
			req: &pb.CreatePartyRequest{
				OwnerId:       randomUUID.String(),
				OwnerUsername: "test",
			},

			isInPartyReq: randomUUID,
			isInPartyRes: false,

			createPartyInput: &model.Party{
				LeaderId: randomUUID,
				Members:  []*model.PartyMember{{PlayerId: randomUUID, Username: "test"}},
			},

			wantResponse: true,
		},
		{
			name: "already in party",
			req: &pb.CreatePartyRequest{
				OwnerId:       randomUUID.String(),
				OwnerUsername: "test",
			},

			isInPartyReq: randomUUID,
			isInPartyRes: true,

			wantErr: createAlreadyInPartyErr,
		},
	}

	for _, tt := range test {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			// Create mock repo
			mockCntrl := gomock.NewController(t)
			defer mockCntrl.Finish()

			mockPartyId := primitive.NewObjectID()

			repo := repository.NewMockRepository(mockCntrl)
			repo.EXPECT().IsInParty(ctx, tt.isInPartyReq).Return(tt.isInPartyRes, tt.isInPartyErr)

			if tt.createPartyInput != nil {
				repo.EXPECT().CreateParty(ctx, tt.createPartyInput).DoAndReturn(func(_ context.Context, party *model.Party) error {
					if tt.createPartyErr == nil {
						party.Id = mockPartyId
					}
					return tt.createPartyErr
				})
			}

			notif := notifier.NewMockNotifier(mockCntrl)
			if tt.wantResponse {
				party := *tt.createPartyInput
				party.Id = mockPartyId
				notif.EXPECT().PartyCreated(ctx, &party)
			}

			s := NewPartyService(notif, repo)

			res, err := s.CreateParty(ctx, tt.req)
			assert.Equal(t, err, tt.wantErr)

			if tt.wantResponse {
				tt.createPartyInput.Id = mockPartyId
				assert.Equal(t, res.Party, tt.createPartyInput.ToProto())
			}
		})
	}
}

func TestPartyService_DisbandParty(t *testing.T) {
	partyId := primitive.NewObjectID()
	memberId := uuid.New()

	test := []struct {
		name string
		req  *pb.DisbandPartyRequest

		getPartyByIdReq primitive.ObjectID
		getPartyByIdRes *model.Party
		getPartyByIdErr error

		getPartyByMemberIdReq uuid.UUID
		getPartyByMemberIdRes *model.Party
		getPartyByMemberIdErr error

		deletePartyReq primitive.ObjectID
		deletePartyErr error

		wantErr error
	}{
		{
			name: "success_by_party_id",
			req: &pb.DisbandPartyRequest{
				Id: &pb.DisbandPartyRequest_PartyId{PartyId: partyId.Hex()},
			},

			getPartyByIdReq: partyId,
			getPartyByIdRes: &model.Party{Id: partyId},

			deletePartyReq: partyId,
		},
		{
			name: "success_by_member_id",
			req: &pb.DisbandPartyRequest{
				Id: &pb.DisbandPartyRequest_PlayerId{PlayerId: memberId.String()},
			},

			getPartyByMemberIdReq: memberId,
			getPartyByMemberIdRes: &model.Party{Id: partyId, LeaderId: memberId},

			deletePartyReq: partyId,
		},
		{
			name: "party_not_found_by_party_id",
			req: &pb.DisbandPartyRequest{
				Id: &pb.DisbandPartyRequest_PartyId{PartyId: partyId.Hex()},
			},

			getPartyByIdReq: partyId,
			getPartyByIdErr: mongo.ErrNoDocuments,

			wantErr: disbandNotInPartyErr,
		},
		{
			name: "party_not_found_by_member_id",
			req: &pb.DisbandPartyRequest{
				Id: &pb.DisbandPartyRequest_PlayerId{PlayerId: memberId.String()},
			},

			getPartyByMemberIdReq: memberId,
			getPartyByMemberIdErr: mongo.ErrNoDocuments,

			wantErr: disbandNotInPartyErr,
		},
	}

	for _, tt := range test {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			// Create mock repo
			mockCntrl := gomock.NewController(t)
			defer mockCntrl.Finish()

			repo := repository.NewMockRepository(mockCntrl)

			if tt.req.GetPartyId() != "" {
				repo.EXPECT().GetPartyById(ctx, tt.getPartyByIdReq).Return(tt.getPartyByIdRes, tt.getPartyByIdErr)
			}
			if tt.getPartyByMemberIdReq != uuid.Nil {
				repo.EXPECT().GetPartyByMemberId(ctx, tt.getPartyByMemberIdReq).Return(tt.getPartyByMemberIdRes, tt.getPartyByMemberIdErr)
			}
			if tt.deletePartyReq != primitive.NilObjectID {
				repo.EXPECT().DeleteParty(ctx, tt.deletePartyReq).Return(tt.deletePartyErr)
			}

			notif := notifier.NewMockNotifier(mockCntrl)
			// If there are no errors yet, expect a notification
			if tt.deletePartyErr == nil && tt.getPartyByIdErr == nil && tt.getPartyByMemberIdErr == nil {
				var party *model.Party
				if tt.getPartyByIdRes != nil {
					party = tt.getPartyByIdRes
				} else if tt.getPartyByMemberIdRes != nil {
					party = tt.getPartyByMemberIdRes
				}
				notif.EXPECT().PartyDisbanded(ctx, party)
			}

			s := NewPartyService(notif, repo)

			res, err := s.DisbandParty(ctx, tt.req)
			assert.Equalf(t, tt.wantErr, err, "wantErr: %+v, got: %+v", tt.wantErr, err)
			assert.Empty(t, res)
		})
	}
}

func TestPartyService_GetParty(t *testing.T) {
	partyId := primitive.NewObjectID()
	memberId := uuid.New()
	memberId2 := uuid.New()

	test := []struct {
		name string
		req  *pb.GetPartyRequest

		getPartyByIdReq primitive.ObjectID
		getPartyByIdRes *model.Party
		getPartyByIdErr error

		getPartyByMemberIdReq uuid.UUID
		getPartyByMemberIdRes *model.Party
		getPartyByMemberIdErr error

		wantRes *pb.GetPartyResponse
		wantErr error
	}{
		{
			name: "success_by_party_id",
			req: &pb.GetPartyRequest{
				Id: &pb.GetPartyRequest_PartyId{PartyId: partyId.Hex()},
			},

			getPartyByIdReq: partyId,
			getPartyByIdRes: &model.Party{
				Id:       partyId,
				LeaderId: memberId,
				Members:  []*model.PartyMember{{PlayerId: memberId, Username: "t"}, {PlayerId: memberId2, Username: "t2"}},
			},

			wantRes: &pb.GetPartyResponse{
				Party: &pbmodel.Party{
					Id:       partyId.Hex(),
					LeaderId: memberId.String(),
					Members:  []*pbmodel.PartyMember{{Id: memberId.String(), Username: "t"}, {Id: memberId2.String(), Username: "t2"}},
				},
			},
		},
		{
			name: "success_by_member_id",
			req: &pb.GetPartyRequest{
				Id: &pb.GetPartyRequest_PlayerId{PlayerId: memberId.String()},
			},

			getPartyByMemberIdReq: memberId,
			getPartyByMemberIdRes: &model.Party{
				Id:       partyId,
				LeaderId: memberId,
				Members:  []*model.PartyMember{{PlayerId: memberId, Username: "t"}, {PlayerId: memberId2, Username: "t2"}},
			},

			wantRes: &pb.GetPartyResponse{
				Party: &pbmodel.Party{
					Id:       partyId.Hex(),
					LeaderId: memberId.String(),
					Members:  []*pbmodel.PartyMember{{Id: memberId.String(), Username: "t"}, {Id: memberId2.String(), Username: "t2"}},
				},
			},
		},
		{
			name: "party_not_found_by_party_id",
			req: &pb.GetPartyRequest{
				Id: &pb.GetPartyRequest_PartyId{PartyId: partyId.Hex()},
			},

			getPartyByIdReq: partyId,
			getPartyByIdErr: mongo.ErrNoDocuments,

			wantErr: partyNotFoundErr,
		},
		{
			name: "party_not_found_by_member_id",
			req: &pb.GetPartyRequest{
				Id: &pb.GetPartyRequest_PlayerId{PlayerId: memberId.String()},
			},

			getPartyByMemberIdReq: memberId,
			getPartyByMemberIdErr: mongo.ErrNoDocuments,

			wantErr: playerNotInPartyErr,
		},
	}

	for _, tt := range test {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			// Create mock repo
			mockCntrl := gomock.NewController(t)
			defer mockCntrl.Finish()

			repo := repository.NewMockRepository(mockCntrl)

			if tt.getPartyByIdReq != primitive.NilObjectID {
				repo.EXPECT().GetPartyById(ctx, tt.getPartyByIdReq).Return(tt.getPartyByIdRes, tt.getPartyByIdErr)
			}
			if tt.getPartyByMemberIdReq != uuid.Nil {
				repo.EXPECT().GetPartyByMemberId(ctx, tt.getPartyByMemberIdReq).Return(tt.getPartyByMemberIdRes, tt.getPartyByMemberIdErr)
			}

			notif := notifier.NewMockNotifier(mockCntrl)

			s := NewPartyService(notif, repo)

			res, err := s.GetParty(ctx, tt.req)
			assert.Equalf(t, tt.wantErr, err, "wantErr: %+v, got: %+v", tt.wantErr, err)
			assert.Equalf(t, tt.wantRes, res, "wantRes: %+v, got: %+v", tt.wantRes, res)
		})
	}
}

func TestPartyService_GetPartyInvites(t *testing.T) {
	partyId := primitive.NewObjectID()
	memberId := uuid.New()
	memberId2 := uuid.New()

	validPartyInvite := &model.PartyInvite{
		Id:              primitive.NewObjectID(),
		InviterId:       memberId,
		InviterUsername: "t",
		TargetId:        memberId2,
		TargetUsername:  "t2",
		ExpiresAt:       time.Now().Add(time.Hour),
	}

	var test = []struct {
		name string
		req  *pb.GetPartyInvitesRequest

		getPartyIdByMemberIdReq uuid.UUID
		getPartyIdByMemberIdRes primitive.ObjectID
		getPartyIdByMemberIdErr error

		getPartyInvitesByPartyIdReq primitive.ObjectID
		getPartyInvitesByPartyIdRes []*model.PartyInvite
		getPartyInvitesByPartyIdErr error

		wantRes *pb.GetPartyInvitesResponse
		wantErr error
	}{
		{
			name: "success_by_party_id",
			req: &pb.GetPartyInvitesRequest{
				Id: &pb.GetPartyInvitesRequest_PartyId{PartyId: partyId.Hex()},
			},

			getPartyInvitesByPartyIdReq: partyId,
			getPartyInvitesByPartyIdRes: []*model.PartyInvite{validPartyInvite},

			wantRes: &pb.GetPartyInvitesResponse{
				Invites: []*pbmodel.PartyInvite{validPartyInvite.ToProto()},
			},
		},
		{
			name: "success_by_member_id",
			req: &pb.GetPartyInvitesRequest{
				Id: &pb.GetPartyInvitesRequest_PlayerId{PlayerId: memberId.String()},
			},

			getPartyIdByMemberIdReq: memberId,
			getPartyIdByMemberIdRes: partyId,

			getPartyInvitesByPartyIdReq: partyId,
			getPartyInvitesByPartyIdRes: []*model.PartyInvite{validPartyInvite},

			wantRes: &pb.GetPartyInvitesResponse{
				Invites: []*pbmodel.PartyInvite{validPartyInvite.ToProto()},
			},
		},
		{
			name: "party_not_found_by_party_id",
			req: &pb.GetPartyInvitesRequest{
				Id: &pb.GetPartyInvitesRequest_PartyId{PartyId: partyId.Hex()},
			},

			getPartyInvitesByPartyIdReq: partyId,
			getPartyInvitesByPartyIdErr: mongo.ErrNoDocuments,

			wantErr: partyNotFoundErr,
		},
		{
			name: "party_not_found_by_member_id",
			req: &pb.GetPartyInvitesRequest{
				Id: &pb.GetPartyInvitesRequest_PlayerId{PlayerId: memberId.String()},
			},

			getPartyIdByMemberIdReq: memberId,
			getPartyIdByMemberIdErr: mongo.ErrNoDocuments,

			wantErr: playerNotInPartyErr,
		},
	}

	for _, tt := range test {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			// Create mock repo
			mockCntrl := gomock.NewController(t)
			defer mockCntrl.Finish()

			repo := repository.NewMockRepository(mockCntrl)

			if tt.getPartyIdByMemberIdReq != uuid.Nil {
				repo.EXPECT().GetPartyIdByMemberId(ctx, tt.getPartyIdByMemberIdReq).Return(tt.getPartyIdByMemberIdRes, tt.getPartyIdByMemberIdErr)
			}
			if tt.getPartyInvitesByPartyIdReq != primitive.NilObjectID {
				repo.EXPECT().GetPartyInvitesByPartyId(ctx, tt.getPartyInvitesByPartyIdReq).Return(tt.getPartyInvitesByPartyIdRes, tt.getPartyInvitesByPartyIdErr)
			}

			notif := notifier.NewMockNotifier(mockCntrl)

			s := NewPartyService(notif, repo)

			res, err := s.GetPartyInvites(ctx, tt.req)
			assert.Equalf(t, tt.wantErr, err, "wantErr: %+v, got: %+v", tt.wantErr, err)
			assert.Equalf(t, tt.wantRes, res, "wantRes: %+v, got: %+v", tt.wantRes, res)
		})
	}
}

// TODO
//func TestPartyService_InvitePlayer(t *testing.T) {
//	playerIds := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
//	playerUsernames := []string{"t", "t2", "t3"}
//
//	partyId := primitive.NewObjectID()
//
//	var test = []struct {
//		name string
//		req  *pb.InvitePlayerRequest
//
//		getPartyByMemberIdReq uuid.UUID
//		getPartyByMemberIdRes *model.Party
//		getPartyByMemberIdErr error
//
//		// Note, will only ever get called if requester != leader
//		getPartySettingsRes *model.PartySettings
//		getPartySettingsErr error
//
//		getPartyIdOfTargetReq uuid.UUID
//		getPartyIdOfTargetRes primitive.ObjectID
//		getPartyIdOfTargetErr error
//
//		doesPartyInviteExistReqPartyId  primitive.ObjectID
//		doesPartyInviteExistReqTargetId uuid.UUID
//		doesPartyInviteExistRes         bool
//		doesPartyInviteExistErr         error
//
//		createPartyInviteReq *model.PartyInvite
//		createPartyInviteErr error
//
//		wantRes *pb.InvitePlayerResponse
//		wantErr error
//	}{
//		{
//			name: "success",
//			req: &pb.InvitePlayerRequest{
//				IssuerId:       playerIds[0].String(),
//				IssuerUsername: playerUsernames[0],
//
//				TargetId:       playerIds[0].String(),
//				TargetUsername: playerUsernames[0],
//			},
//
//			getPartyByMemberIdReq: playerIds[0],
//			getPartyByMemberIdRes: &model.Party{
//				PlayerId:       partyId,
//				LeaderId: playerIds[0],
//				Members:  []*model.PartyMember{{PlayerId: playerIds[0], Username: playerUsernames[0]}},
//			},
//
//			getPartyIdOfTargetReq: playerIds[1],
//			getPartyIdOfTargetRes: primitive.NilObjectID,
//
//			doesPartyInviteExistReqPartyId:  partyId,
//			doesPartyInviteExistReqTargetId: playerIds[1],
//			doesPartyInviteExistRes:         false,
//
//			createPartyInviteReq: &model.PartyInvite{
//				PartyId:  partyId,
//			}
//		},
//		// Player 0 is inviting player 1 to the party. Player 2 is the leader and the party has all invites enabled.
//		{
//			name: "success_member_inviting",
//			req: &pb.InvitePlayerRequest{
//				IssuerId:       playerIds[0].String(),
//				IssuerUsername: playerUsernames[0],
//
//				TargetId:       playerIds[1].String(),
//				TargetUsername: playerUsernames[1],
//			},
//
//			getPartyByMemberIdReq: playerIds[0],
//			getPartyByMemberIdRes: &model.Party{
//				PlayerId:       partyId,
//				LeaderId: playerIds[2],
//				Members:  []*model.PartyMember{{PlayerId: playerIds[0], Username: playerUsernames[0]}, {PlayerId: playerIds[2], Username: playerUsernames[2]}},
//			},
//
//			getPartySettingsReq: partyId,
//			getPartySettingsRes: &model.PartySettings{
//				AllowMemberInvite: true,
//			},
//
//			getPartyIdOfTargetReq: playerIds[1],
//			getPartyIdOfTargetRes: primitive.NilObjectID,
//
//			doesPartyInviteExistReq: &model.PartyInvite{},
//		},
//	}
//}
