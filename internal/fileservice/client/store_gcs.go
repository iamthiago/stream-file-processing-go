package client

import (
	"cloud.google.com/go/storage"
	"context"
)

type GCStore struct {
	gcs *storage.Client
}

func NewGCStore(gcs *storage.Client) *GCStore {
	return &GCStore{
		gcs: gcs,
	}
}

func (g *GCStore) ListObjects(bucket string, prefix string, ctx context.Context) *storage.ObjectIterator {
	query := &storage.Query{Prefix: prefix}
	return g.gcs.Bucket(bucket).Objects(ctx, query)
}

func (g *GCStore) Get(bucket string, name string, isCompressed bool) *storage.ObjectHandle {
	return g.gcs.Bucket(bucket).Object(name).ReadCompressed(isCompressed)
}

func (g *GCStore) Writer(bucket string, name string, ctx context.Context) *storage.Writer {
	return g.gcs.Bucket(bucket).Object(name).NewWriter(ctx)
}
