package gdrive

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

type AuthConfig struct {
	ClientID     string
	ClientSecret string

	RedirectURI string

	endpoint oauth2.Endpoint
}

const DefaultRedirectURI = "http://127.0.0.1:8088/oauth/callback"

const authRequestTimeout = 60 * time.Second

const authorizeOffline = true

func Authorize(cfg AuthConfig, state string) (string, error) {
	if err := cfg.validate(); err != nil {
		return "", err
	}
	if strings.TrimSpace(state) == "" {
		return "", errors.New("gdrive: the authorisation state must not be empty")
	}

	endpoint := cfg.redirectURI()

	query := url.Values{
		"client_id":     {cfg.ClientID},
		"redirect_uri":  {endpoint},
		"response_type": {"code"},
		"scope":         {DriveScope},
		"state":         {state},
		"access_type":   {"offline"},

		"prompt": {"consent"},
	}

	return cfg.endpointOrDefault().AuthURL + "?" + query.Encode(), nil
}

func NewState() (string, error) {

	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("gdrive: generate authorisation state: %w", err)
	}

	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func Exchange(ctx context.Context, cfg AuthConfig, code, state string) (*oauth2.Token, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	_ = state
	if strings.TrimSpace(code) == "" {
		return nil, errors.New("gdrive: the authorisation code is empty")
	}

	ctx, cancel := context.WithTimeout(ctx, authRequestTimeout)
	defer cancel()

	token, err := cfg.oauth2Config().Exchange(ctx, code)
	if err != nil {
		return nil, fmt.Errorf("gdrive: exchange the authorisation code: %w", err)
	}

	if token.RefreshToken == "" {
		return nil, errors.New("gdrive: Google returned no refresh token; revoke the app's access " +
			"and authorise again so a durable token is issued")
	}

	return token, nil
}

func (c AuthConfig) validate() error {
	switch {
	case strings.TrimSpace(c.ClientID) == "":
		return errors.New("gdrive: the OAuth client id is required")
	case strings.TrimSpace(c.ClientSecret) == "":
		return errors.New("gdrive: the OAuth client secret is required")
	}
	return nil
}

func (c AuthConfig) oauth2Config() *oauth2.Config {
	return &oauth2.Config{
		ClientID:     c.ClientID,
		ClientSecret: c.ClientSecret,
		RedirectURL:  c.redirectURI(),
		Scopes:       []string{DriveScope},
		Endpoint:     c.endpointOrDefault(),
	}
}

func (c AuthConfig) endpointOrDefault() oauth2.Endpoint {
	if c.endpoint.TokenURL == "" {
		return google.Endpoint
	}
	return c.endpoint
}

func (c AuthConfig) redirectURI() string {
	if strings.TrimSpace(c.RedirectURI) == "" {
		return DefaultRedirectURI
	}
	return c.RedirectURI
}
