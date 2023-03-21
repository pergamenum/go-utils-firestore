package dao

import (
	"context"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/firestore"
	e "github.com/pergamenum/go-consensus-standards/ehandler"
	t "github.com/pergamenum/go-consensus-standards/types"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type DAO[Document any] struct {
	c     *firestore.Client
	path  string
	log   *zap.SugaredLogger
	empty Document
}

func NewDAO[Document any](fc *firestore.Client, path string, log *zap.SugaredLogger) *DAO[Document] {

	logNamed := log.Named("firestore.DAO")

	return &DAO[Document]{
		c:    fc,
		path: path,
		log:  logNamed,
	}
}

func (c *DAO[Document]) Create(ctx context.Context, id string, document Document) error {

	now := time.Now()

	d := c.c.Collection(c.path).Doc(id)
	_, err := d.Create(ctx, document)
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			cause := fmt.Sprintf("(document '%s' already exists)", id)
			return e.Wrap(cause, e.ErrConflict)
		}
		return err
	}

	fus := []firestore.Update{
		{
			Path:  "created",
			Value: now,
		},
		{
			Path:  "updated",
			Value: now,
		},
	}

	_, err = d.Update(ctx, fus)
	if err != nil {
		return err
	}

	return nil
}

func (c *DAO[Document]) Read(ctx context.Context, id string) (Document, error) {

	snapshot, err := c.c.Collection(c.path).Doc(id).Get(ctx)
	if err != nil {
		if !snapshot.Exists() {
			cause := fmt.Sprintf("(ID: %s)", id)
			return c.empty, e.Wrap(cause, e.ErrNotFound)
		}
		return c.empty, err
	}

	var d Document
	err = snapshot.DataTo(&d)
	if err != nil {
		cause := fmt.Sprintf("(firestore serialization failed: %s)", err.Error())
		return c.empty, e.Wrap(cause, e.ErrCorrupt)
	}

	return d, nil
}

func (c *DAO[Document]) Update(ctx context.Context, id string, update t.Update) error {

	fus := c.fromUpdate(update)

	fus = append(fus, firestore.Update{
		Path:  "updated",
		Value: time.Now(),
	})

	_, err := c.c.Collection(c.path).Doc(id).Update(ctx, fus)
	if err != nil {
		return err
	}

	return nil
}

func (c *DAO[Document]) Delete(ctx context.Context, id string) error {

	_, err := c.c.Collection(c.path).Doc(id).Delete(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (c *DAO[Document]) Search(ctx context.Context, queries []t.Query) ([]Document, error) {

	log := c.log.Named("Search")

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
			op := fro(q.Operator)
			if i == 0 {
				fsq = collection.Where(q.Key, op, q.Value)
			} else {
				fsq = fsq.Where(q.Key, op, q.Value)
			}
		}
		snapshots, err = fsq.Documents(ctx).GetAll()
		if err != nil {
			if status.Code(err) == codes.FailedPrecondition {
				cause := "(query not supported: combining '==' with '!= <, <=, >, >=')"
				return nil, e.Wrap(cause, e.ErrBadRequest)
			}
			return nil, err
		}
	}

	var ds []Document
	for _, s := range snapshots {
		var d Document
		err = s.DataTo(&d)
		if err != nil {
			// Log corrupt snapshots as errors and then continue.
			cause := fmt.Sprintf("(firestore serialization failed: %s)", err.Error())
			wrapped := e.Wrap(cause, e.ErrCorrupt)
			log.With("snapshot", s).
				Error(wrapped)
			continue
		}
		ds = append(ds, d)
	}

	return ds, nil
}

func (c *DAO[Document]) fromUpdate(input t.Update) []firestore.Update {

	var fus []firestore.Update

	for key, value := range input {

		fu := firestore.Update{
			Path:  key,
			Value: value,
		}
		fus = append(fus, fu)
	}

	return fus
}

// fro = Firestore Relational Operator
func fro(input string) string {
	switch strings.ToUpper(input) {
	case "EQ":
		return "=="
	case "NE":
		return "!="
	case "LT":
		return "<"
	case "GT":
		return ">"
	case "LE":
		return "<="
	case "GE":
		return ">="
	default:
		return "UNKNOWN"
	}
}
