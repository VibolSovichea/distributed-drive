







package provider

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"
)








var (
	
	
	ErrNotFound = errors.New("provider: object not found")

	
	
	ErrAuth = errors.New("provider: authentication failed")

	
	
	ErrQuotaExceeded = errors.New("provider: quota exceeded")

	
	
	ErrUnavailable = errors.New("provider: node unavailable")

	
	
	ErrInvalid = errors.New("provider: invalid request")

	
	
	ErrTooLarge = errors.New("provider: object too large")
)





type ObjectMetadata struct {
	
	
	Name string

	
	
	
	Description string

	
	ContentType string

	
	
	
	
	
	Size int64
}


type RemoteObject struct {
	
	
	ID string

	
	
	Name string

	
	Size int64

	
	
	
	
	
	
	
	
	Checksum string

	
	CreatedAt  time.Time
	ModifiedAt time.Time
}


type Quota struct {
	
	
	Total int64
	
	Used int64
}




func (q Quota) Available() int64 {
	if q.Total <= 0 {
		return 0
	}
	if q.Used >= q.Total {
		return 0
	}
	return q.Total - q.Used
}


func (q Quota) Known() bool { return q.Total > 0 }


type Identity struct {
	
	
	
	AccountID string

	
	Email string

	
	DisplayName string
}



func (i Identity) Display() string {
	switch {
	case i.Email != "":
		return i.Email
	case i.DisplayName != "":
		return i.DisplayName
	case i.AccountID != "":
		return i.AccountID
	default:
		return "unknown account"
	}
}












type StorageNode interface {
	
	
	Upload(ctx context.Context, r io.Reader, meta ObjectMetadata) (RemoteObject, error)

	
	
	Download(ctx context.Context, id string) (io.ReadCloser, error)

	
	
	
	Delete(ctx context.Context, id string) error

	
	Stat(ctx context.Context, id string) (RemoteObject, error)

	
	
	
	Quota(ctx context.Context) (Quota, error)

	
	Identity(ctx context.Context) (Identity, error)
}







type Health string

const (
	
	
	
	HealthUnknown Health = ""

	
	
	HealthHealthy Health = "healthy"
	
	HealthDegraded Health = "degraded"
	
	HealthOffline Health = "offline"
	
	HealthFull Health = "full"
	
	HealthAuthError Health = "auth_error"
)












type StatusError struct {
	
	Health Health

	
	Err error
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("provider: node is %s: %v", e.Health, e.Err)
}

func (e *StatusError) Unwrap() error { return e.Err }








func HealthOf(err error) Health {
	var status *StatusError
	if errors.As(err, &status) {
		return status.Health
	}

	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return HealthUnknown
	case errors.Is(err, ErrAuth):
		return HealthAuthError
	case errors.Is(err, ErrQuotaExceeded):
		return HealthFull
	case errors.Is(err, ErrNotFound):
		
		return HealthHealthy
	case errors.Is(err, ErrInvalid):
		
		return HealthHealthy
	case errors.Is(err, ErrTooLarge):
		return HealthFull
	default:
		return HealthOffline
	}
}


func Authf(err error) error {
	return &StatusError{Health: HealthAuthError, Err: fmt.Errorf("%w: %w", ErrAuth, err)}
}


func Unavailablef(err error) error {
	return &StatusError{Health: HealthOffline, Err: fmt.Errorf("%w: %w", ErrUnavailable, err)}
}


func QuotaExceededf(err error) error {
	return &StatusError{Health: HealthFull, Err: fmt.Errorf("%w: %w", ErrQuotaExceeded, err)}
}



func Degradedf(err error) error {
	return &StatusError{Health: HealthDegraded, Err: err}
}


func NotFound(id string) error {
	return fmt.Errorf("%w: %q", ErrNotFound, id)
}
