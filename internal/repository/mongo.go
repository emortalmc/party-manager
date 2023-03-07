package repository

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"party-manager/internal/config"
	"party-manager/internal/repository/model"
	"party-manager/internal/repository/registrytypes"
	"time"
)

const (
	databaseName = "party-manager"

	partyCollectionName         = "party"
	partyInviteCollectionName   = "partyInvite"
	partySettingsCollectionName = "partySettings"
)

var (
	ErrAlreadyInParty = errors.New("player already in party")
	ErrNotInParty     = errors.New("player not in party")
	ErrIdMustBeNil    = errors.New("id must be nil")
	ErrAlreadyLeader  = errors.New("player already leader") // TODO this isn't handled yet
)

type mongoRepository struct {
	Repository

	database *mongo.Database

	partyCollection         *mongo.Collection
	partyInviteCollection   *mongo.Collection
	partySettingsCollection *mongo.Collection
}

func NewMongoRepository(ctx context.Context, cfg config.MongoDBConfig) (Repository, error) {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(cfg.URI).SetRegistry(createCodecRegistry()))
	if err != nil {
		return nil, err
	}

	database := client.Database(databaseName)
	return &mongoRepository{
		database: database,

		partyCollection:         database.Collection(partyCollectionName),
		partyInviteCollection:   database.Collection(partyInviteCollectionName),
		partySettingsCollection: database.Collection(partySettingsCollectionName),
	}, nil
}

func (m *mongoRepository) HealthCheck(ctx context.Context, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	return m.database.Client().Ping(ctx, nil)
}

func (m *mongoRepository) IsInParty(ctx context.Context, playerId uuid.UUID) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	count, err := m.partyCollection.CountDocuments(ctx, bson.M{"members.playerId": playerId})
	if err != nil {
		return false, err
	}

	return count > 0, nil
}

func (m *mongoRepository) CreateParty(ctx context.Context, party *model.Party) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if party.Id != primitive.NilObjectID {
		return ErrIdMustBeNil
	}
	party.Id = primitive.NewObjectID()

	_, err := m.partyCollection.InsertOne(ctx, party)
	if err != nil {
		return err
	}

	return nil
}

func (m *mongoRepository) DeleteParty(ctx context.Context, partyId primitive.ObjectID) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	err := m.partyCollection.FindOneAndDelete(ctx, bson.M{"_id": partyId}).Err()
	return err
}

func (m *mongoRepository) AddPartyMember(ctx context.Context, partyId primitive.ObjectID, member *model.PartyMember) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	res, err := m.partyCollection.UpdateByID(ctx, partyId, bson.M{"$addToSet": bson.M{"members": member}})
	if err != nil {
		return err
	}

	if res.MatchedCount == 0 {
		return mongo.ErrNoDocuments
	}

	if res.ModifiedCount == 0 {
		return ErrAlreadyInParty
	}

	return nil
}

func (m *mongoRepository) SetPartyLeader(ctx context.Context, partyId primitive.ObjectID, leaderId uuid.UUID) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	res, err := m.partyCollection.UpdateByID(ctx, partyId, bson.M{"$set": bson.M{"leaderId": leaderId}})
	if err != nil {
		return err
	}

	if res.MatchedCount == 0 {
		return mongo.ErrNoDocuments
	}

	if res.ModifiedCount == 0 {
		return ErrAlreadyLeader
	}

	return nil
}

func (m *mongoRepository) GetPartyById(ctx context.Context, partyId primitive.ObjectID) (*model.Party, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var party model.Party
	err := m.partyCollection.FindOne(ctx, bson.M{"_id": partyId}).Decode(&party)
	if err != nil {
		return nil, err
	}

	return &party, nil
}

func (m *mongoRepository) GetPartyByMemberId(ctx context.Context, playerId uuid.UUID) (*model.Party, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var party model.Party
	err := m.partyCollection.FindOne(ctx, bson.M{"members.playerId": playerId}).Decode(&party)
	if err != nil {
		return nil, err
	}

	return &party, nil
}

func (m *mongoRepository) GetPartyIdByMemberId(ctx context.Context, playerId uuid.UUID) (primitive.ObjectID, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// Get only the id of the party
	var result struct {
		Id primitive.ObjectID `bson:"_id"`
	}
	err := m.partyCollection.FindOne(ctx, bson.M{"members.playerId": playerId}, options.FindOne().SetProjection(bson.M{"_id": 1})).Decode(&result)
	if err != nil {
		return primitive.NilObjectID, err
	}

	return result.Id, nil
}

func (m *mongoRepository) GetPartyLeaderIdByMemberId(ctx context.Context, playerId uuid.UUID) (uuid.UUID, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// Get only the id of the party
	var result struct {
		LeaderId uuid.UUID `bson:"leaderId"`
	}
	err := m.partyCollection.FindOne(ctx, bson.M{"members.playerId": playerId}, options.FindOne().SetProjection(bson.M{"leaderId": 1})).Decode(&result)
	if err != nil {
		return uuid.Nil, err
	}

	return result.LeaderId, nil
}

func (m *mongoRepository) RemoveMemberFromParty(ctx context.Context, partyId primitive.ObjectID, playerId uuid.UUID) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	res, err := m.partyCollection.UpdateByID(ctx, partyId, bson.M{"$pull": bson.M{"members": bson.M{"playerId": playerId}}})
	if err != nil {
		return err
	}

	if res.MatchedCount == 0 {
		return mongo.ErrNoDocuments
	}

	if res.ModifiedCount == 0 {
		return ErrNotInParty
	}

	return nil
}

func (m *mongoRepository) RemoveMemberFromSelfParty(ctx context.Context, playerId uuid.UUID) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	res, err := m.partyCollection.UpdateOne(ctx, bson.M{"members.playerId": playerId}, bson.M{"$pull": bson.M{"members": bson.M{"playerId": playerId}}})
	if err != nil {
		return err
	}

	if res.MatchedCount == 0 {
		return mongo.ErrNoDocuments
	}

	return nil
}

func (m *mongoRepository) CreatePartyInvite(ctx context.Context, invite *model.PartyInvite) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if invite.Id != primitive.NilObjectID {
		return ErrIdMustBeNil
	}
	invite.Id = primitive.NewObjectID()

	_, err := m.partyInviteCollection.InsertOne(ctx, invite)
	if err != nil {
		return err
	}

	return nil
}

func (m *mongoRepository) DeletePartyInvite(ctx context.Context, partyId primitive.ObjectID, targetId uuid.UUID) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	result, err := m.partyInviteCollection.DeleteOne(ctx, bson.M{"partyId": partyId, "targetId": targetId})
	if err != nil {
		return err
	}

	if result.DeletedCount == 0 {
		return mongo.ErrNoDocuments
	}

	return nil
}

func (m *mongoRepository) DeletePartyInvitesByPartyId(ctx context.Context, partyId primitive.ObjectID) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, err := m.partyInviteCollection.DeleteMany(ctx, bson.M{"partyId": partyId})
	return err
}

func (m *mongoRepository) GetPartyInvitesByPartyId(ctx context.Context, partyId primitive.ObjectID) ([]*model.PartyInvite, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var invites []*model.PartyInvite
	cursor, err := m.partyInviteCollection.Find(ctx, bson.M{"partyId": partyId})
	if err != nil {
		return nil, err
	}

	err = cursor.All(ctx, &invites)
	if err != nil {
		return nil, err
	}

	return invites, nil
}

func (m *mongoRepository) DoesPartyInviteExist(ctx context.Context, partyId primitive.ObjectID, playerId uuid.UUID) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	count, err := m.partyInviteCollection.CountDocuments(ctx, bson.M{"partyId": partyId, "targetId": playerId})
	if err != nil {
		return false, err
	}

	return count > 0, nil
}

func (m *mongoRepository) GetPartySettings(ctx context.Context, playerId uuid.UUID) (*model.PartySettings, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var settings model.PartySettings
	err := m.partySettingsCollection.FindOne(ctx, bson.M{"_id": playerId}).Decode(&settings)
	if err != nil {
		return nil, err
	}

	return &settings, nil
}

func (m *mongoRepository) UpdatePartySettings(ctx context.Context, settings *model.PartySettings) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, err := m.partySettingsCollection.ReplaceOne(ctx, bson.M{"_id": settings.PlayerId}, settings, options.Replace().SetUpsert(true))
	if err != nil {
		return err
	}

	// NOTE: We don't care if the document was modified or not, because the settings are the same
	return nil
}

func createCodecRegistry() *bsoncodec.Registry {
	return bson.NewRegistryBuilder().
		RegisterTypeEncoder(registrytypes.UUIDType, bsoncodec.ValueEncoderFunc(registrytypes.UuidEncodeValue)).
		RegisterTypeDecoder(registrytypes.UUIDType, bsoncodec.ValueDecoderFunc(registrytypes.UuidDecodeValue)).
		Build()
}
