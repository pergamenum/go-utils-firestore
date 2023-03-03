package cruds

import (
	"context"
	"errors"

	"cloud.google.com/go/firestore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type FirestoreCRUDS[Document any] interface {
	Create(ctx context.Context, id string, document Document) error
	Read(ctx context.Context, id string) (Document, error)
	Update(ctx context.Context, id string, updates []firestore.Update) error
	Delete(ctx context.Context, id string) error
	Search(ctx context.Context, queries []Query) ([]Document, error)
}

type Query struct {
	Path     string
	Operator string
	Value    any
}

type CRUDS[Document any] struct {
	c     *firestore.Client
	path  string
	empty Document
}

func NewCRUDS[Document any](fc *firestore.Client, path string) *CRUDS[Document] {
	return &CRUDS[Document]{
		c:    fc,
		path: path,
	}
}

func (c *CRUDS[Document]) Create(ctx context.Context, id string, document Document) error {

	_, err := c.c.Collection(c.path).Doc(id).Create(ctx, document)
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			return ErrDocumentAlreadyExists
		}
		return err
	}

	return nil
}

func (c *CRUDS[Document]) Read(ctx context.Context, id string) (Document, error) {

	snapshot, err := c.c.Collection(c.path).Doc(id).Get(ctx)
	if err != nil {
		if !snapshot.Exists() {
			return c.empty, ErrDocumentNotFound
		}
		return c.empty, err
	}

	var d Document
	err = snapshot.DataTo(&d)
	if err != nil {
		return c.empty, err
	}

	return d, nil
}

func (c *CRUDS[Document]) Update(ctx context.Context, id string, updates []firestore.Update) error {

	_, err := c.c.Collection(c.path).Doc(id).Update(ctx, updates)
	if err != nil {
		return err
	}

	return nil
}

func (c *CRUDS[Document]) Delete(ctx context.Context, id string) error {

	_, err := c.c.Collection(c.path).Doc(id).Delete(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (c *CRUDS[Document]) Search(ctx context.Context, queries []Query) ([]Document, error) {

	collection := c.c.Collection(c.path)
	var snapshots []*firestore.DocumentSnapshot
	var err error
	if len(queries) == 0 {
		snapshots, err = collection.Documents(ctx).GetAll()
		if err != nil {
			return nil, err
		}
	} else {
		var fsq firestore.Query
		for i, q := range queries {
			if i == 0 {
				fsq = collection.Where(q.Path, q.Operator, q.Value)
			} else {
				fsq = fsq.Where(q.Path, q.Operator, q.Value)
			}
		}
		snapshots, err = fsq.Documents(ctx).GetAll()
		if err != nil {
			if status.Code(err) == codes.FailedPrecondition {
				return nil, ErrQueryNotSupported
			}
			return nil, err
		}
	}

	var ds []Document
	for _, s := range snapshots {
		var d Document
		err = s.DataTo(&d)
		if err != nil {
			err = ErrDocumentSkipped
			continue
		}
		ds = append(ds, d)
	}

	return ds, err
}

var (
	ErrDocumentNotFound      = errors.New("document not found")
	ErrDocumentAlreadyExists = errors.New("document already exists")
	ErrDocumentSkipped       = errors.New("document skipped because of error")
	ErrQueryNotSupported     = errors.New("query not supported: combining (==) with (!=, <, <=, >, >=)")
)
