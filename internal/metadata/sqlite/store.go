package sqlite

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/VibolSovichea/distributed-drive/internal/metadata"
)

type Store struct {
	db    dbtx
	owned bool
}

func New(db *sql.DB) *Store {
	return &Store{db: db, owned: true}
}

type dbtx interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

var _ metadata.Store = (*Store)(nil)

func (s *Store) Ping(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, "SELECT 1"); err != nil {
		return classify("ping", err)
	}
	return nil
}

func (s *Store) Close() error {
	if !s.owned {
		return nil
	}
	db, ok := s.db.(*sql.DB)
	if !ok {
		return nil
	}
	if err := db.Close(); err != nil {
		return fmt.Errorf("sqlite: close: %w", err)
	}
	return nil
}

type beginner interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

func (s *Store) WithTx(ctx context.Context, fn func(ctx context.Context, tx metadata.Store) error) error {
	db, ok := s.db.(beginner)
	if !ok {
		return fmt.Errorf("sqlite: cannot begin a nested transaction")
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return wrap("begin transaction", err)
	}

	committed := false
	defer func() {
		if !committed {

			_ = tx.Rollback()
		}
	}()

	if err := fn(ctx, &Store{db: tx}); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return wrap("commit transaction", err)
	}
	committed = true
	return nil
}

func wrap(op string, err error) error {
	if err == nil {
		return nil
	}
	if isCancellation(err) {
		return err
	}
	return fmt.Errorf("sqlite: %s: %w", op, err)
}

func (s *Store) inTransaction(ctx context.Context, body func(ctx context.Context, tx *sql.Tx) error) error {
	if tx, ok := s.db.(*sql.Tx); ok {
		return body(ctx, tx)
	}

	db, ok := s.db.(beginner)
	if !ok {
		return fmt.Errorf("sqlite: %T cannot begin a transaction", s.db)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return wrap("begin transaction", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := body(ctx, tx); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return wrap("commit transaction", err)
	}
	return nil
}

func (s *Store) exec(ctx context.Context, op, query string, args ...any) (sql.Result, error) {
	res, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, classify(op, err)
	}
	return res, nil
}

func (s *Store) query(ctx context.Context, op, query string, args ...any) (*sql.Rows, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, classify(op, err)
	}
	return rows, nil
}
