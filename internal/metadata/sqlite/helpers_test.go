package sqlite

import (
	"errors"

	"github.com/VibolSovichea/distributed-drive/internal/metadata"
)

func isNotFound(err error) bool { return errors.Is(err, metadata.ErrNotFound) }
func isConflict(err error) bool { return errors.Is(err, metadata.ErrConflict) }
func isInvalid(err error) bool  { return errors.Is(err, metadata.ErrInvalid) }
