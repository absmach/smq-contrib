// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

// Package main contains cassandra-reader main function to start the cassandra-reader service.
package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"

	cassandraclient "github.com/absmach/supermq-contrib/pkg/clients/cassandra"
	"github.com/absmach/supermq-contrib/readers/api"
	"github.com/absmach/supermq-contrib/readers/cassandra"

	"github.com/gocql/gocql"

	chclient "github.com/absmach/callhome/pkg/client"
	"github.com/absmach/supermq"
	httpapi "github.com/absmach/supermq-contrib/readers/api"
	smqlog "github.com/absmach/supermq/logger"
	"github.com/absmach/supermq/pkg/authn/authsvc"
	"github.com/absmach/supermq/pkg/grpcclient"
	"github.com/absmach/supermq/pkg/prometheus"
	"github.com/absmach/supermq/pkg/server"
	httpserver "github.com/absmach/supermq/pkg/server/http"
	"github.com/absmach/supermq/pkg/uuid"
	"github.com/absmach/supermq/readers"
	"github.com/caarlos0/env/v11"
	"golang.org/x/sync/errgroup"
)

const (
	svcName           = "cassandra-reader"
	envPrefixDB       = "SMQ_CASSANDRA_"
	envPrefixHTTP     = "SMQ_CASSANDRA_READER_HTTP_"
	envPrefixAuth     = "SMQ_AUTH_GRPC_"
	envPrefixClients  = "SMQ_CLIENTS_AUTH_GRPC_"
	envPrefixChannels = "SMQ_CHANNELS_GRPC_"
	defDB             = "supermq"
	defSvcHTTPPort    = "9003"
)

type config struct {
	LogLevel      string `env:"SMQ_CASSANDRA_READER_LOG_LEVEL"     envDefault:"info"`
	SendTelemetry bool   `env:"SMQ_SEND_TELEMETRY"                envDefault:"true"`
	InstanceID    string `env:"SMQ_CASSANDRA_READER_INSTANCE_ID"   envDefault:""`
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

	clientsClientCfg := grpcclient.Config{}
	if err := env.ParseWithOptions(&clientsClientCfg, env.Options{Prefix: envPrefixClients}); err != nil {
		logger.Error(fmt.Sprintf("failed to load clients gRPC client configuration : %s", err))
		exitCode = 1
		return
	}

	clientsClient, clientsHandler, err := grpcclient.SetupClientsClient(ctx, clientsClientCfg)
	if err != nil {
		logger.Error(err.Error())
		exitCode = 1
		return
	}
	defer clientsHandler.Close()
	logger.Info("Clients service gRPC client successfully connected to clients gRPC server " + clientsHandler.Secure())

	channelsClientCfg := grpcclient.Config{}
	if err := env.ParseWithOptions(&channelsClientCfg, env.Options{Prefix: envPrefixChannels}); err != nil {
		logger.Error(fmt.Sprintf("failed to load channels gRPC client configuration : %s", err))
		exitCode = 1
		return
	}

	channelsClient, channelsHandler, err := grpcclient.SetupChannelsClient(ctx, channelsClientCfg)
	if err != nil {
		logger.Error(err.Error())
		exitCode = 1
		return
	}
	defer channelsHandler.Close()
	logger.Info("Channels service gRPC client successfully connected to channels gRPC server " + channelsHandler.Secure())

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

	// Create new cassandra client
	csdSession, err := cassandraclient.Setup(envPrefixDB)
	if err != nil {
		logger.Error(err.Error())
		exitCode = 1
		return
	}
	defer csdSession.Close()

	// Create new service
	repo := newService(csdSession, logger)

	httpServerConfig := server.Config{Port: defSvcHTTPPort}
	if err := env.ParseWithOptions(&httpServerConfig, env.Options{Prefix: envPrefixHTTP}); err != nil {
		logger.Error(fmt.Sprintf("failed to load %s HTTP server configuration : %s", svcName, err))
		exitCode = 1
		return
	}
	hs := httpserver.NewServer(ctx, cancel, svcName, httpServerConfig, httpapi.MakeHandler(repo, authn, clientsClient, channelsClient, svcName, cfg.InstanceID), logger)

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
		logger.Error(fmt.Sprintf("Postgres reader service terminated: %s", err))
	}
}

func newService(csdSession *gocql.Session, logger *slog.Logger) readers.MessageRepository {
	repo := cassandra.New(csdSession)
	repo = httpapi.LoggingMiddleware(repo, logger)
	counter, latency := prometheus.MakeMetrics("cassandra", "message_reader")
	repo = api.MetricsMiddleware(repo, counter, latency)
	return repo
}
