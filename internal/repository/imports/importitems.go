package importitems

import (
	"context"
	"fmt"
	"time"

	mg "debtster_import/internal/config/connections/mongo"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const ImportRecordItemsCollection = "import_record_items"

type Item struct {
	ImportRecordID string    `bson:"import_record_id" json:"import_record_id"`
	ModelType      string    `bson:"model_type" json:"model_type"`
	ModelID        string    `bson:"model_id" json:"model_id"`
	Payload        string    `bson:"payload" json:"payload"`
	Status         string    `bson:"status" json:"status"`
	Errors         string    `bson:"errors" json:"errors"`
	CreatedAt      time.Time `bson:"created_at" json:"created_at"`
	UpdatedAt      time.Time `bson:"updated_at" json:"updated_at"`
}

func InsertItem(ctx context.Context, m *mg.Mongo, item Item) (*mongo.InsertOneResult, error) {
	if m == nil || m.Client == nil || m.Database == nil {
		return nil, mongo.ErrClientDisconnected
	}

	now := time.Now().UTC()
	if item.CreatedAt.IsZero() {
		item.CreatedAt = now
	}
	item.UpdatedAt = now

	doc := bson.D{
		{Key: "import_record_id", Value: item.ImportRecordID},
		{Key: "model_type", Value: item.ModelType},
		{Key: "model_id", Value: item.ModelID},
		{Key: "payload", Value: item.Payload},
		{Key: "status", Value: item.Status},
		{Key: "errors", Value: item.Errors},
		{Key: "created_at", Value: item.CreatedAt},
		{Key: "updated_at", Value: item.UpdatedAt},
	}

	return m.Database.Collection(ImportRecordItemsCollection).InsertOne(ctx, doc, options.InsertOne())
}

func UpdateImportRecordStatus(ctx context.Context, m *mg.Mongo, importRecordID, status string) error {
	if m == nil || m.Database == nil {
		return mongo.ErrClientDisconnected
	}
	if importRecordID == "" {
		return fmt.Errorf("empty importRecordID")
	}
	if status == "" {
		return fmt.Errorf("empty status")
	}

	coll := m.Database.Collection("import_records")

	update := bson.M{
		"$set": bson.M{
			"status":     status,
			"updated_at": time.Now().UTC(),
		},
	}

	if oid, err := primitive.ObjectIDFromHex(importRecordID); err == nil {
		res, err := coll.UpdateOne(ctx, bson.M{"_id": oid}, update)
		if err != nil {
			return err
		}
		if res.MatchedCount > 0 {
			return nil
		}
	}

	res, err := coll.UpdateOne(ctx, bson.M{"_id": importRecordID}, update)
	if err != nil {
		return err
	}
	if res.MatchedCount == 0 {
		return fmt.Errorf("no import_record found with id %s (tried ObjectId and string)", importRecordID)
	}
	return nil
}

func UpdateImportRecordStatusDone(ctx context.Context, m *mg.Mongo, importRecordID string) error {
	return UpdateImportRecordStatus(ctx, m, importRecordID, "done")
}
