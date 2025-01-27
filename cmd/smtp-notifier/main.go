// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

// Package main contains smtp-notifier main function to start the smtp-notifier service.
package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net/url"
	"os"

	chclient "github.com/absmach/callhome/pkg/client"
	"github.com/absmach/magistrala/consumers/notifiers"
	"github.com/absmach/magistrala/consumers/notifiers/api"
	notifierpg "github.com/absmach/magistrala/consumers/notifiers/postgres"
	"github.com/absmach/magistrala/consumers/notifiers/tracing"
	"github.com/absmach/magistrala/pkg/prometheus"
	"github.com/absmach/mg-contrib/consumers/notifiers/smtp"
	email "github.com/absmach/mg-contrib/pkg/email"
	"github.com/absmach/supermq"
	"github.com/absmach/supermq/consumers"
	smqlog "github.com/absmach/supermq/logger"
	smqauthn "github.com/absmach/supermq/pkg/authn"
	authsvcAuthn "github.com/absmach/supermq/pkg/authn/authsvc"
	"github.com/absmach/supermq/pkg/grpcclient"
	jaegerclient "github.com/absmach/supermq/pkg/jaeger"
	"github.com/absmach/supermq/pkg/messaging/brokers"
	brokerstracing "github.com/absmach/supermq/pkg/messaging/brokers/tracing"
	pgclient "github.com/absmach/supermq/pkg/postgres"
	"github.com/absmach/supermq/pkg/server"
	httpserver "github.com/absmach/supermq/pkg/server/http"
	"github.com/absmach/supermq/pkg/ulid"
	"github.com/absmach/supermq/pkg/uuid"
	"github.com/caarlos0/env/v11"
	"github.com/jmoiron/sqlx"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
)

const (
	svcName        = "smtp-notifier"
	envPrefixDB    = "MG_SMTP_NOTIFIER_DB_"
	envPrefixHTTP  = "MG_SMTP_NOTIFIER_HTTP_"
	envPrefixAuth  = "MG_AUTH_GRPC_"
	defDB          = "subscriptions"
	defSvcHTTPPort = "9015"
)

type config struct {
	LogLevel      string  `env:"MG_SMTP_NOTIFIER_LOG_LEVEL"    envDefault:"info"`
	ConfigPath    string  `env:"MG_SMTP_NOTIFIER_CONFIG_PATH"  envDefault:"/config.toml"`
	From          string  `env:"MG_SMTP_NOTIFIER_FROM_ADDR"    envDefault:""`
	BrokerURL     string  `env:"MG_MESSAGE_BROKER_URL"         envDefault:"nats://localhost:4222"`
	JaegerURL     url.URL `env:"MG_JAEGER_URL"                 envDefault:"http://jaeger:14268/api/traces"`
	SendTelemetry bool    `env:"MG_SEND_TELEMETRY"             envDefault:"true"`
	InstanceID    string  `env:"MG_SMTP_NOTIFIER_INSTANCE_ID"  envDefault:""`
	TraceRatio    float64 `env:"MG_JAEGER_TRACE_RATIO"         envDefault:"1.0"`
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	g, ctx := errgroup.WithContext(ctx)

	cfg := config{}
	if err := env.Parse(&cfg); err != nil {
		log.Fatalf("failed to load %s configuration : %s", svcName, err)
	}

	logger, err := smqlog.New(os.Stdout, cfg.LogLevel)
	if err != nil {
		log.Fatalf("failed to init logger: %s", err.Error())
	}

	var exitCode int
	defer smqlog.ExitWithError(&exitCode)

	if cfg.InstanceID == "" {
		if cfg.InstanceID, err = uuid.New().ID(); err != nil {
			logger.Error(fmt.Sprintf("failed to generate instanceID: %s", err))
			exitCode = 1
			return
		}
	}

	dbConfig := pgclient.Config{Name: defDB}
	if err := env.ParseWithOptions(&dbConfig, env.Options{Prefix: envPrefixDB}); err != nil {
		logger.Error(fmt.Sprintf("failed to load %s Postgres configuration : %s", svcName, err))
		exitCode = 1
		return
	}
	db, err := pgclient.Setup(dbConfig, *notifierpg.Migration())
	if err != nil {
		logger.Error(err.Error())
		exitCode = 1
		return
	}
	defer db.Close()

	ec := email.Config{}
	if err := env.Parse(&ec); err != nil {
		logger.Error(fmt.Sprintf("failed to load email configuration : %s", err))
		exitCode = 1
		return
	}

	httpServerConfig := server.Config{Port: defSvcHTTPPort}
	if err := env.ParseWithOptions(&httpServerConfig, env.Options{Prefix: envPrefixHTTP}); err != nil {
		logger.Error(fmt.Sprintf("failed to load %s HTTP server configuration : %s", svcName, err))
		exitCode = 1
		return
	}

	tp, err := jaegerclient.NewProvider(ctx, svcName, cfg.JaegerURL, cfg.InstanceID, cfg.TraceRatio)
	if err != nil {
		logger.Error(fmt.Sprintf("failed to init Jaeger: %s", err))
		exitCode = 1
		return
	}
	defer func() {
		if err := tp.Shutdown(ctx); err != nil {
			logger.Error(fmt.Sprintf("Error shutting down tracer provider: %v", err))
		}
	}()
	tracer := tp.Tracer(svcName)

	pubSub, err := brokers.NewPubSub(ctx, cfg.BrokerURL, logger)
	if err != nil {
		logger.Error(fmt.Sprintf("failed to connect to message broker: %s", err))
		exitCode = 1
		return
	}
	defer pubSub.Close()
	pubSub = brokerstracing.NewPubSub(httpServerConfig, tracer, pubSub)

	grpcCfg := grpcclient.Config{}
	if err := env.ParseWithOptions(&grpcCfg, env.Options{Prefix: envPrefixAuth}); err != nil {
		logger.Error(fmt.Sprintf("failed to load auth gRPC client configuration : %s", err))
		exitCode = 1
		return
	}
	authn, authnClient, err := authsvcAuthn.NewAuthentication(ctx, grpcCfg)
	if err != nil {
		logger.Error(err.Error())
		exitCode = 1
		return
	}
	logger.Info("AuthN successfully connected to auth gRPC server " + authnClient.Secure())
	defer authnClient.Close()

	svc, err := newService(db, tracer, authn, cfg, ec, logger)
	if err != nil {
		logger.Error(err.Error())
		exitCode = 1
		return
	}

	if err = consumers.Start(ctx, svcName, pubSub, svc, cfg.ConfigPath, logger); err != nil {
		logger.Error(fmt.Sprintf("failed to create Postgres writer: %s", err))
		exitCode = 1
		return
	}

	hs := httpserver.NewServer(ctx, cancel, svcName, httpServerConfig, api.MakeHandler(svc, logger, cfg.InstanceID), logger)

	if cfg.SendTelemetry {
		chc := chclient.New(svcName, supermq.Version, logger, cancel)
		go chc.CallHome(ctx)
	}

	g.Go(func() error {
		return hs.Start()
	})

	g.Go(func() error {
		return server.StopSignalHandler(ctx, cancel, logger, svcName, hs)
	})

	if err := g.Wait(); err != nil {
		logger.Error(fmt.Sprintf("SMTP notifier service terminated: %s", err))
	}
}

func newService(db *sqlx.DB, tracer trace.Tracer, authClient smqauthn.Authentication, c config, ec email.Config, logger *slog.Logger) (notifiers.Service, error) {
	database := notifierpg.NewDatabase(db, tracer)
	repo := tracing.New(tracer, notifierpg.New(database))
	idp := ulid.New()

	agent, err := email.New(&ec)
	if err != nil {
		return nil, fmt.Errorf("failed to create email agent: %s", err)
	}

	notifier := smtp.New(agent)
	svc := notifiers.New(authClient, repo, idp, notifier, c.From)
	svc = api.LoggingMiddleware(svc, logger)
	counter, latency := prometheus.MakeMetrics("notifier", "smtp")
	svc = api.MetricsMiddleware(svc, counter, latency)

	return svc, nil
}
