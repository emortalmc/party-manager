package kafka

import (
	"party-manager/internal/repository/model"
	"party-manager/internal/utils"
	"testing"
)

func TestElectNewPartyLeader(t *testing.T) {
	uIds := utils.RandomUuidSlice(3)

	oneMemberParty := &model.Party{Members: []*model.PartyMember{{PlayerId: uIds[0]}}, LeaderId: uIds[0]}
	twoMemberParty := &model.Party{Members: []*model.PartyMember{{PlayerId: uIds[0]}, {PlayerId: uIds[1]}}, LeaderId: uIds[0]}
	threeMemberParty := &model.Party{Members: []*model.PartyMember{{PlayerId: uIds[0]}, {PlayerId: uIds[1]}, {PlayerId: uIds[2]}}, LeaderId: uIds[0]}

	tests := []struct {
		name    string
		repeats int

		party *model.Party

		acceptableResults []*model.PartyMember
		wantErr           error
	}{
		{
			name:    "one member",
			party:   oneMemberParty,
			wantErr: leaderElectionNotEnoughMembersErr,
		},
		{
			name:              "two members",
			party:             twoMemberParty,
			acceptableResults: []*model.PartyMember{twoMemberParty.Members[1]}, // Any member other than currentLeader
		},
		{
			name:              "three members",
			repeats:           1000,
			party:             threeMemberParty,
			acceptableResults: threeMemberParty.Members[1:],
		},
	}

	for _, test := range tests {
		for i := 0; i < test.repeats; i++ {
			t.Run(test.name, func(t *testing.T) {
				res, err := electNewPartyLeader(test.party)
				if err != test.wantErr {
					t.Errorf("got err %v, want %v", err, test.wantErr)
				}

				if test.wantErr != nil {
					return
				}

				for _, acceptableResult := range test.acceptableResults {
					if res == acceptableResult {
						return
					}
				}

				t.Errorf("got %v, want one of %v", res, test.acceptableResults)
			})
		}
	}
}
