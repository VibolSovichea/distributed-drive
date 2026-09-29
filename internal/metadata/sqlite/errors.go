package sqlite

import (
	"context"
	"errors"
	"fmt"

	"github.com/VibolSovichea/distributed-drive/internal/metadata"

	driver "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

func classify(op string, err error) error {
	if err == nil {
		return nil
	}
	if isCancellation(err) {
		return err
	}

	var serr *driver.Error
	if !errors.As(err, &serr) {
		return fmt.Errorf("sqlite: %s: %w", op, err)
	}

	switch serr.Code() {
	case sqlite3.SQLITE_CONSTRAINT_UNIQUE, sqlite3.SQLITE_CONSTRAINT_PRIMARYKEY:
		return fmt.Errorf("sqlite %s: %w", op, errors.Join(metadata.ErrConflict, serr))
	case sqlite3.SQLITE_CONSTRAINT_FOREIGNKEY,
		sqlite3.SQLITE_CONSTRAINT_NOTNULL,
		sqlite3.SQLITE_CONSTRAINT_CHECK,
		sqlite3.SQLITE_CONSTRAINT:
		return fmt.Errorf("sqlite %s: %w", op, errors.Join(metadata.ErrInvalid, serr))
	case sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED:
		return fmt.Errorf("sqlite: %s: database is busy: %w", op, err)
	default:
		return fmt.Errorf("sqlite: %s: %w", op, err)
	}
}

func isCancellation(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func notFound(entity, id string) error {
	return fmt.Errorf("%w: %s %q", metadata.ErrNotFound, entity, id)
}

func errMissingID(entity string) error {
	return fmt.Errorf("%w: %s id must be assigned before persisting", metadata.ErrInvalid, entity)
}
