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

const ImportRecordsCollection = "import_records"

type Record struct {
	ID        any        `bson:"_id" json:"id"`
	UserID    *string    `bson:"user_id,omitempty" json:"user_id,omitempty"`
	Count     int        `bson:"count" json:"count"`
	Status    string     `bson:"status" json:"status"`
	Errors    *string    `bson:"errors,omitempty" json:"errors,omitempty"`
	Type      string     `bson:"type" json:"type"`
	Path      *string    `bson:"path,omitempty" json:"path,omitempty"`
	Bucket    *string    `bson:"bucket,omitempty" json:"bucket,omitempty"`
	Key       *string    `bson:"key,omitempty" json:"key,omitempty"`
	SizeBytes *int64     `bson:"size_bytes,omitempty" json:"size_bytes,omitempty"`
	CreatedAt time.Time  `bson:"created_at" json:"created_at"`
	UpdatedAt time.Time  `bson:"updated_at" json:"updated_at"`
	DeletedAt *time.Time `bson:"deleted_at,omitempty" json:"deleted_at,omitempty"`
}

func InsertImportRecord(ctx context.Context, m *mg.Mongo, rec Record) (*mongo.InsertOneResult, error) {
	if m == nil || m.Client == nil || m.Database == nil {
		return nil, mongo.ErrClientDisconnected
	}

	now := time.Now().UTC()
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = now
	}
	rec.UpdatedAt = now
	if rec.Status == "" {
		rec.Status = "parsed"
	}

	doc := bson.D{
		{Key: "user_id", Value: rec.UserID},
		{Key: "count", Value: rec.Count},
		{Key: "status", Value: rec.Status},
		{Key: "errors", Value: rec.Errors},
		{Key: "type", Value: rec.Type},
		{Key: "path", Value: rec.Path},
		{Key: "bucket", Value: rec.Bucket},
		{Key: "key", Value: rec.Key},
		{Key: "size_bytes", Value: rec.SizeBytes},
		{Key: "created_at", Value: rec.CreatedAt},
		{Key: "updated_at", Value: rec.UpdatedAt},
	}

	return m.Database.Collection(ImportRecordsCollection).InsertOne(ctx, doc, options.InsertOne())
}

func FindImportRecordByID(ctx context.Context, m *mg.Mongo, id string) (Record, error) {
	var out Record
	if m == nil || m.Database == nil {
		return out, mongo.ErrClientDisconnected
	}
	coll := m.Database.Collection(ImportRecordsCollection)

	if oid, err := primitive.ObjectIDFromHex(id); err == nil {
		if err := coll.FindOne(ctx, bson.M{"_id": oid}).Decode(&out); err == nil {
			out.ID = oid
			return out, nil
		}
	}

	if err := coll.FindOne(ctx, bson.M{"_id": id}).Decode(&out); err != nil {
		return out, fmt.Errorf("not found: %w", err)
	}
	out.ID = id
	return out, nil
}

func ListImportRecords(ctx context.Context, m *mg.Mongo, filter bson.M, limit, skip int64) ([]Record, int64, error) {
	if m == nil || m.Database == nil {
		return nil, 0, mongo.ErrClientDisconnected
	}
	coll := m.Database.Collection(ImportRecordsCollection)
	if filter == nil {
		filter = bson.M{}
	}

	opts := options.Find().SetSort(bson.D{{Key: "created_at", Value: -1}})
	if limit > 0 {
		opts.SetLimit(limit)
	}
	if skip > 0 {
		opts.SetSkip(skip)
	}

	cur, err := coll.Find(ctx, filter, opts)
	if err != nil {
		return nil, 0, err
	}
	defer cur.Close(ctx)

	recs := make([]Record, 0)
	for cur.Next(ctx) {
		var r Record
		if err := cur.Decode(&r); err != nil {
			continue
		}
		recs = append(recs, r)
	}
	total, err := coll.CountDocuments(ctx, filter)
	if err != nil {
		total = int64(len(recs))
	}
	return recs, total, nil
}
