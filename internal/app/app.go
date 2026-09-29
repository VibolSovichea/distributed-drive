package app

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/VibolSovichea/distributed-drive/internal/api"
	"github.com/VibolSovichea/distributed-drive/internal/authflow"
	"github.com/VibolSovichea/distributed-drive/internal/config"
	"github.com/VibolSovichea/distributed-drive/internal/db"
	"github.com/VibolSovichea/distributed-drive/internal/db/migrate"
	"github.com/VibolSovichea/distributed-drive/internal/factory"
	"github.com/VibolSovichea/distributed-drive/internal/gdrive"
	"github.com/VibolSovichea/distributed-drive/internal/logging"
	"github.com/VibolSovichea/distributed-drive/internal/metadata"
	metastoresqlite "github.com/VibolSovichea/distributed-drive/internal/metadata/sqlite"
	"github.com/VibolSovichea/distributed-drive/internal/node"
	"github.com/VibolSovichea/distributed-drive/internal/pool"
)

const shutdownGrace = 5 * time.Second

type App struct {
	cfg    config.Config
	logger *slog.Logger

	db    *sql.DB
	store metadata.Store
	api   *api.Server
	nodes node.Service

	migrations int
}

func New(ctx context.Context, cfg config.Config, logger *slog.Logger) (*App, error) {
	if logger == nil {
		logger = slog.Default()
	}

	app := &App{cfg: cfg, logger: logger}

	handle, err := db.Open(ctx, cfg.Database)
	if err != nil {
		return nil, fmt.Errorf("app: open database: %w", err)
	}
	app.db = handle

	app.store = metastoresqlite.New(handle)

	if cfg.Database.MigrateOnOpen {
		applied, err := migrate.New(handle).Up(ctx)
		if err != nil {

			_ = app.Close()
			return nil, fmt.Errorf("app: apply migrations: %w", err)
		}
		app.migrations = applied
	}

	nodeSvc, oauthSvc, err := app.buildStorageServices()
	if err != nil {
		_ = app.Close()
		return nil, err
	}

	srv, err := api.New(api.Config{
		Pools:  pool.NewManager(app.store, nil),
		Nodes:  nodeSvc,
		OAuth:  oauthSvc,
		Store:  app.store,
		Logger: logger,
	})
	if err != nil {
		_ = app.Close()
		return nil, fmt.Errorf("app: build api: %w", err)
	}
	app.api = srv
	app.nodes = nodeSvc

	return app, nil
}

func (a *App) buildStorageServices() (node.Service, *authflow.Service, error) {
	if !a.cfg.Storage.AnyProviderEnabled() {
		a.logger.Info("no storage provider configured; node routes disabled",
			"hint", "set STORAGE_TOKEN_KEY and STORAGE_GOOGLE_CLIENT_ID to enable Google Drive")
		return nil, nil, nil
	}

	build, err := factory.New(factory.Options{
		GoogleClientID:     a.cfg.Storage.GoogleClientID,
		GoogleClientSecret: a.cfg.Storage.GoogleClientSecret,
		GoogleRedirectURI:  a.cfg.Storage.GoogleRedirectURI,
		TokenKey:           gdrive.KeyFromSecret(a.cfg.Storage.TokenEncryptionKey),
		TokenDir:           a.cfg.Storage.TokenDir,
		LocalRoot:          a.cfg.Storage.LocalNodeRoot,
		Logger:             a.logger,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("app: build the provider factory: %w", err)
	}

	auth, err := authflow.New(build, authflow.Options{})
	if err != nil {
		return nil, nil, fmt.Errorf("app: build the authorisation flow: %w", err)
	}

	svc := node.New(a.appStore(), build, node.Options{})

	a.logger.Info("storage providers enabled",
		"google", a.cfg.Storage.GoogleEnabled(),
		"local", a.cfg.Storage.LocalNodeRoot != "")

	return svc, auth, nil
}

type appStore struct{ metadata.Store }

func (a *App) appStore() appStore { return appStore{a.store} }

func (a *App) Handler() http.Handler { return a.api.Handler() }

func (a *App) Store() metadata.Store { return a.store }

func (a *App) Run(ctx context.Context) error {
	defer func() { _ = a.Close() }()

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	a.logger.Info("starting distributed-drive",
		"addr", a.cfg.HTTP.Addr,
		"config", a.cfg.String(),
		"database", a.cfg.Database.Path,
		"migrationsApplied", a.migrations,
		"logLevel", a.cfg.Logging.Level,
		"logFormat", a.cfg.Logging.Format,
	)

	errCh := make(chan error, 1)
	go func() {
		errCh <- a.api.ListenAndServe(ctx, a.cfg.HTTP.Addr, a.cfg.HTTP.ShutdownTimeout)
	}()

	select {
	case err := <-errCh:
		return err

	case <-ctx.Done():
		a.logger.Info("shutdown signal received", logging.Duration("timeout", a.cfg.HTTP.ShutdownTimeout))
	}

	select {
	case err := <-errCh:
		if err != nil {
			return err
		}
	case <-time.After(a.cfg.HTTP.ShutdownTimeout + shutdownGrace):
		return errors.New("app: shutdown timed out waiting for in-flight requests")
	}

	a.logger.Info("shutdown complete")

	return nil
}

func (a *App) Close() error {
	if a.store == nil {
		return nil
	}

	if err := a.store.Close(); err != nil {
		return fmt.Errorf("app: close store: %w", err)
	}
	a.store = nil

	return nil
}

func Run(ctx context.Context) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("app: load configuration: %w", err)
	}

	logger := logging.New(logWriter(), cfg.Logging.Level, cfg.Logging.Format)

	application, err := New(ctx, cfg, logger)
	if err != nil {
		logger.Error("startup failed", "error", err)
		return err
	}

	return application.Run(ctx)
}
