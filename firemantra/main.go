package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	v03807 "github.com/streamingfast/firehose-cosmos/cometbft/03807"
	"github.com/streamingfast/logging"
	"go.uber.org/zap"
)

var logger, tracer = logging.PackageLogger("firecosmos", "github.com/streamingfast/firehose-cosmos")
var rootCmd = &cobra.Command{
	Use:   "firemantra",
	Short: "Firehose fetcher for Mantra block fetching and tooling",
	Args:  cobra.ExactArgs(1),
}

func init() {
	logging.InstantiateLoggers(logging.WithDefaultLevel(zap.InfoLevel))

	rootCmd.AddCommand(newFetchCmd(logger, tracer))
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Whoops. There was an error while executing your CLI '%s'", err)
		os.Exit(1)
	}
}

func newFetchCmd(logger *zap.Logger, tracer logging.Tracer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "fetch",
		Short: "fetch blocks from different sources",
		Args:  cobra.ExactArgs(2),
	}
	cmd.AddCommand(v03807.NewFetchCmd(logger, tracer))
	return cmd
}
