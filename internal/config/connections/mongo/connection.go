package mongo

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type ConnectionInfo struct {
	Scheme     string
	User       string
	Password   string
	Host       string
	Port       string
	DB         string
	AuthSource string
}

type Mongo struct {
	Client   *mongo.Client
	Database *mongo.Database
}

func NewConnection(ctx context.Context, info ConnectionInfo) (*Mongo, error) {
	scheme := info.Scheme
	if scheme == "" {
		scheme = "mongodb"
	}

	auth := ""
	if info.User != "" {
		auth = info.User
		if info.Password != "" {
			auth += ":" + info.Password
		}
		auth += "@"
	}

	host := info.Host
	if info.Port != "" {
		host += ":" + info.Port
	}

	query := ""
	if info.AuthSource != "" {
		query = "?authSource=" + info.AuthSource
	}

	uri := fmt.Sprintf("%s://%s%s/%s%s", scheme, auth, host, info.DB, query)

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := client.Ping(pingCtx, readpref.Primary()); err != nil {
		return nil, err
	}

	db := client.Database(info.DB)
	return &Mongo{Client: client, Database: db}, nil
}

func (m *Mongo) Close(ctx context.Context) error {
	if m.Client != nil {
		return m.Client.Disconnect(ctx)
	}
	return nil
}
