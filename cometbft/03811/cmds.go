package v03811

import (
	"fmt"
	"strconv"
	"time"

	cometBftHttp "github.com/cometbft/cometbft/rpc/client/http"
	"github.com/spf13/cobra"
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

	cmd.Flags().StringArray("endpoints", []string{"https://sentry.tm.injective.network:443"}, "interval between fetch")
	cmd.Flags().String("state-dir", "/data/fetcher", "interval between fetch")
	cmd.Flags().Duration("latest-block-retry-interval", time.Second, "interval between fetch")
	cmd.Flags().Int("block-fetch-batch-size", 10, "Number of blocks to fetch in a single batch")

	return cmd
}

func fetchRunE(logger *zap.Logger, _ logging.Tracer) firecore.CommandExecutor {
	return func(cmd *cobra.Command, args []string) (err error) {
		rpcEndpoints := sflags.MustGetStringArray(cmd, "endpoints")

		stateDir := sflags.MustGetString(cmd, "state-dir")

		startBlock, err := strconv.ParseUint(args[0], 10, 64)
		if err != nil {
			return fmt.Errorf("unable to parse first streamable block %d: %w", startBlock, err)
		}

		logger.Info(
			"launching firehose-cosmos fetcher",
			zap.Strings("rpc_endpoint", rpcEndpoints),
			zap.String("state_dir", stateDir),
			zap.Uint64("first_streamable_block", startBlock),
			zap.Duration("latest_block_retry_interval", sflags.MustGetDuration(cmd, "latest-block-retry-interval")),
		)

		rpcClients := firecoreRPC.NewClients[*cometBftHttp.HTTP](10*time.Second, firecoreRPC.NewStickyRollingStrategy[*cometBftHttp.HTTP](), logger)
		for _, rpcEndpoint := range rpcEndpoints {
			client, err := cometBftHttp.New(rpcEndpoint, "")
			if err != nil {
				return fmt.Errorf("creating rpc client: %w", err)
			}
			rpcClients.Add(client)
		}

		latestBlockRetryInterval := sflags.MustGetDuration(cmd, "latest-block-retry-interval")

		rpcFetcher := NewRPCFetcher(latestBlockRetryInterval, logger)
		poller := blockpoller.New[*cometBftHttp.HTTP](
			rpcFetcher,
			blockpoller.NewFireBlockHandler("type.googleapis.com/sf.cosmos.type.v2.Block"),
			rpcClients,
			blockpoller.WithStoringState[*cometBftHttp.HTTP](stateDir),
			blockpoller.WithLogger[*cometBftHttp.HTTP](logger),
		)

		err = poller.Run(startBlock, nil, sflags.MustGetInt(cmd, "block-fetch-batch-size"))
		if err != nil {
			return fmt.Errorf("running fetcher: %w", err)
		}

		return nil
	}
}
