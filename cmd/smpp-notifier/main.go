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
	"github.com/absmach/supermq"
	"github.com/absmach/supermq-contrib/consumers/notifiers"
	"github.com/absmach/supermq-contrib/consumers/notifiers/api"
	notifierpg "github.com/absmach/supermq-contrib/consumers/notifiers/postgres"
	"github.com/absmach/supermq-contrib/consumers/notifiers/smpp"
	"github.com/absmach/supermq-contrib/consumers/notifiers/tracing"
	"github.com/absmach/supermq/consumers"
	mglog "github.com/absmach/supermq/logger"
	"github.com/absmach/supermq/pkg/authn"
	"github.com/absmach/supermq/pkg/authn/authsvc"
	"github.com/absmach/supermq/pkg/grpcclient"
	jaegerclient "github.com/absmach/supermq/pkg/jaeger"
	"github.com/absmach/supermq/pkg/messaging/brokers"
	brokerstracing "github.com/absmach/supermq/pkg/messaging/brokers/tracing"
	pgclient "github.com/absmach/supermq/pkg/postgres"
	"github.com/absmach/supermq/pkg/prometheus"
	"github.com/absmach/supermq/pkg/server"
	httpserver "github.com/absmach/supermq/pkg/server/http"
	"github.com/absmach/supermq/pkg/ulid"
	"github.com/absmach/supermq/pkg/uuid"
	"github.com/caarlos0/env/v10"
	"github.com/jmoiron/sqlx"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
)

const (
	svcName           = "smtp-notifier"
	envPrefixDB       = "MG_SMTP_NOTIFIER_DB_"
	envPrefixHTTP     = "MG_SMTP_NOTIFIER_HTTP_"
	envPrefixAuth     = "SMQ_AUTH_GRPC_"
	envPrefixClients  = "SMQ_CLIENTS_AUTH_GRPC_"
	envPrefixChannels = "SMQ_CHANNELS_GRPC_"
	defDB             = "subscriptions"
	defSvcHTTPPort    = "9015"
)

type config struct {
	LogLevel      string  `env:"SMQ_SMTP_NOTIFIER_LOG_LEVEL"    envDefault:"info"`
	ConfigPath    string  `env:"SMQ_SMTP_NOTIFIER_CONFIG_PATH"  envDefault:"/config.toml"`
	From          string  `env:"SMQ_SMTP_NOTIFIER_FROM_ADDR"    envDefault:""`
	BrokerURL     string  `env:"SMQ_MESSAGE_BROKER_URL"         envDefault:"nats://localhost:4222"`
	JaegerURL     url.URL `env:"SMQ_JAEGER_URL"                 envDefault:"http://jaeger:14268/api/traces"`
	SendTelemetry bool    `env:"SMQ_SEND_TELEMETRY"             envDefault:"true"`
	InstanceID    string  `env:"SMQ_SMTP_NOTIFIER_INSTANCE_ID"  envDefault:""`
	TraceRatio    float64 `env:"SMQ_JAEGER_TRACE_RATIO"         envDefault:"1.0"`
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	g, ctx := errgroup.WithContext(ctx)

	cfg := config{}
	if err := env.Parse(&cfg); err != nil {
		log.Fatalf("failed to load %s configuration : %s", svcName, err)
	}

	logger, err := mglog.New(os.Stdout, cfg.LogLevel)
	if err != nil {
		log.Fatalf("failed to init logger: %s", err.Error())
	}

	var exitCode int
	defer mglog.ExitWithError(&exitCode)

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

	smppConfig := smpp.Config{}
	if err := env.Parse(&smppConfig); err != nil {
		logger.Error(fmt.Sprintf("failed to load SMPP configuration from environment : %s", err))
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

	clientsClientCfg := grpcclient.Config{}
	if err := env.ParseWithOptions(&clientsClientCfg, env.Options{Prefix: envPrefixClients}); err != nil {
		logger.Error(fmt.Sprintf("failed to load clients gRPC client configuration : %s", err))
		exitCode = 1
		return
	}

	authnCfg := grpcclient.Config{}
	if err := env.ParseWithOptions(&authnCfg, env.Options{Prefix: envPrefixAuth}); err != nil {
		logger.Error(fmt.Sprintf("failed to load auth gRPC client configuration : %s", err))
		exitCode = 1
		return
	}

	authn, authnHandler, err := authsvc.NewAuthentication(ctx, authnCfg)
	if err != nil {
		logger.Error(err.Error())
		exitCode = 1
		return
	}
	defer authnHandler.Close()
	logger.Info("authn successfully connected to auth gRPC server " + authnHandler.Secure())

	svc := newService(db, authn, tracer, cfg, smppConfig, logger)

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

// func newService(db *sqlx.DB, authClient authn.Authentication, tracer trace.Tracer, c config, ec email.Config, logger *slog.Logger) (notifiers.Service, error) {
// 	database := notifierpg.NewDatabase(db, tracer)
// 	repo := tracing.New(tracer, notifierpg.New(database))
// 	idp := ulid.New()

// 	agent, err := email.New(&ec)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to create email agent: %s", err)
// 	}

// 	notifier := smtp.New(agent)
// 	svc := notifiers.New(authClient, repo, idp, notifier, c.From)
// 	svc = api.LoggingMiddleware(svc, logger)
// 	counter, latency := prometheus.MakeMetrics("notifier", "smtp")
// 	svc = api.MetricsMiddleware(svc, counter, latency)

// 	return svc, nil
// }

func newService(db *sqlx.DB, authClient authn.Authentication, tracer trace.Tracer, c config, sc smpp.Config, logger *slog.Logger) notifiers.Service {
	database := notifierpg.NewDatabase(db, tracer)
	repo := tracing.New(tracer, notifierpg.New(database))
	idp := ulid.New()
	notifier := smpp.New(sc)
	svc := notifiers.New(authClient, repo, idp, notifier, c.From)
	svc = api.LoggingMiddleware(svc, logger)
	counter, latency := prometheus.MakeMetrics("notifier", "smpp")
	svc = api.MetricsMiddleware(svc, counter, latency)

	return svc
}
