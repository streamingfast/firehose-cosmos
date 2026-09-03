// Package v1 polls blocks from a CometBFT v1 RPC endpoint. The directory is
// 101 for CometBFT v1.0.1, matching the 03811 (v0.38.11) sibling, while the
// package is v1 because a Go identifier cannot start with a digit.
package v1

import (
	"fmt"
	"strconv"
	"time"

	cometBftHttp "github.com/cometbft/cometbft/rpc/client/http"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/streamingfast/cli/sflags"
	firecore "github.com/streamingfast/firehose-core"
	"github.com/streamingfast/firehose-core/blockpoller"
	firecoreRPC "github.com/streamingfast/firehose-core/rpc"
	"github.com/streamingfast/logging"
	"go.uber.org/zap"
)

func NewFetchCmd(logger *zap.Logger, tracer logging.Tracer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "rpc <first-streamable-block>",
		Short: "fetch blocks from rpc endpoint",
		Args:  cobra.ExactArgs(1),
		RunE:  fetchRunE(logger, tracer),
	}

	cmd.Flags().StringArray("endpoints", []string{"https://sentry.tm.injective.network:443"}, "rpc endpoints to fetch blocks from")
	cmd.Flags().String("state-dir", "/data/fetcher", "directory holding the poller state")
	cmd.Flags().Duration("latest-block-retry-interval", time.Second, "interval between fetch")
	cmd.Flags().Int("block-fetch-batch-size", 10, "Number of blocks to fetch in a single batch")
	cmd.Flags().Duration("max-block-fetch-duration", 60*time.Second, "maximum delay before considering a block fetch as failed")

	return cmd
}

func fetchRunE(logger *zap.Logger, _ logging.Tracer) firecore.CommandExecutor {
	return func(cmd *cobra.Command, args []string) (err error) {
		rpcEndpoints := sflags.MustGetStringArray(cmd, "endpoints")

		stateDir := sflags.MustGetString(cmd, "state-dir")

		startBlock, err := strconv.ParseUint(args[0], 10, 64)
		if err != nil {
			return fmt.Errorf("unable to parse first streamable block %q: %w", args[0], err)
		}

		logger.Info("Command Flags and Values:")
		cmd.Flags().VisitAll(func(flag *pflag.Flag) {
			logger.Info("Flag", zap.String("name", flag.Name), zap.String("value", flag.Value.String()))
		})

		blockFetchMaxDuration := sflags.MustGetDuration(cmd, "max-block-fetch-duration")
		wrappedCometHttpClients := firecoreRPC.NewClients[*CometHttpClientWrap](blockFetchMaxDuration, firecoreRPC.NewStickyRollingStrategy[*CometHttpClientWrap](), logger)
		for _, rpcEndpoint := range rpcEndpoints {
			client, err := cometBftHttp.New(rpcEndpoint)
			if err != nil {
				return fmt.Errorf("creating rpc client: %w", err)
			}
			wrappedCometHttpClients.Add(NewCometHttpClientWrap(rpcEndpoint, client))
		}

		latestBlockRetryInterval := sflags.MustGetDuration(cmd, "latest-block-retry-interval")

		rpcFetcher := NewRPCFetcher(latestBlockRetryInterval, logger)
		poller := blockpoller.New[*CometHttpClientWrap](
			rpcFetcher,
			blockpoller.NewFireBlockHandler("type.googleapis.com/sf.cosmos.type.v2.Block"),
			wrappedCometHttpClients,
			blockpoller.WithStoringState[*CometHttpClientWrap](stateDir),
			blockpoller.WithLogger[*CometHttpClientWrap](logger),
		)

		err = poller.Run(startBlock, nil, sflags.MustGetInt(cmd, "block-fetch-batch-size"))
		if err != nil {
			return fmt.Errorf("running fetcher: %w", err)
		}

		return nil
	}
}
