package main

import (
	"fmt"
	"strconv"
	"time"

	cometBftHttp "github.com/cometbft/cometbft/rpc/client/http"
	"github.com/spf13/cobra"
	"github.com/streamingfast/cli/sflags"
	firecore "github.com/streamingfast/firehose-core"
	"github.com/streamingfast/firehose-core/blockpoller"
	"github.com/streamingfast/firehose-cosmos/poller"
	"github.com/streamingfast/logging"
	"go.uber.org/zap"
)

func NewFetchCmd(logger *zap.Logger, tracer logging.Tracer, cosmosChain string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "rpc <first-streamable-block>",
		Short: "fetch blocks from rpc endpoint",
		Args:  cobra.ExactArgs(1),
		RunE:  fetchRunE(logger, tracer, cosmosChain),
	}

	cmd.Flags().StringArray("endpoints", []string{}, "interval between fetch")
	cmd.Flags().String("state-dir", "/data/poller", "interval between fetch")
	cmd.Flags().Duration("interval-between-fetch", 0, "interval between fetch")
	cmd.Flags().Duration("latest-block-retry-interval", time.Second, "interval between fetch")
	cmd.Flags().Int("block-fetch-batch-size", 10, "Number of blocks to fetch in a single batch")

	return cmd
}

func fetchRunE(logger *zap.Logger, tracer logging.Tracer, cosmosChain string) firecore.CommandExecutor {
	return func(cmd *cobra.Command, args []string) (err error) {
		ctx := cmd.Context()
		rpcEndpoints := sflags.MustGetStringArray(cmd, "endpoints")

		stateDir := sflags.MustGetString(cmd, "state-dir")

		startBlock, err := strconv.ParseUint(args[0], 10, 64)
		if err != nil {
			return fmt.Errorf("unable to parse first streamable block %d: %w", startBlock, err)
		}

		fetchInterval := sflags.MustGetDuration(cmd, "interval-between-fetch")

		logger.Info(
			"launching firehose-cosmos poller",
			zap.Strings("rpc_endpoint", rpcEndpoints),
			zap.String("state_dir", stateDir),
			zap.Uint64("first_streamable_block", startBlock),
			zap.Duration("interval_between_fetch", fetchInterval),
			zap.Duration("latest_block_retry_interval", sflags.MustGetDuration(cmd, "latest-block-retry-interval")),
		)

		rpcClients := make([]*cometBftHttp.HTTP, 0, len(rpcEndpoints))
		for _, rpcEndpoint := range rpcEndpoints {
			client, err := cometBftHttp.New(rpcEndpoint, "")
			if err != nil {
				return fmt.Errorf("creating rpc client: %w", err)
			}
			rpcClients = append(rpcClients, client)
		}

		latestBlockRetryInterval := sflags.MustGetDuration(cmd, "latest-block-retry-interval")

		var rpcFetcher blockpoller.BlockFetcher

		if cosmosChain == "injective" {
			rpcFetcher = poller.NewRPCFetcher(rpcClients, fetchInterval, latestBlockRetryInterval, logger)
		}

		poller := blockpoller.New(
			rpcFetcher,
			blockpoller.NewFireBlockHandler("type.googleapis.com/sf.cosmos.type.v2.Block"),
			blockpoller.WithStoringState(stateDir),
			blockpoller.WithLogger(logger),
		)

		err = poller.Run(ctx, startBlock, sflags.MustGetInt(cmd, "block-fetch-batch-size"))
		if err != nil {
			return fmt.Errorf("running poller: %w", err)
		}

		return nil
	}
}
