package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/streamingfast/firehose-cosmos/cosmos/cmd"
	"github.com/streamingfast/logging"
	"go.uber.org/zap"
)

var logger, tracer = logging.PackageLogger("firecosmos", "github.com/streamingfast/firehose-cosmos")
var rootCmd = &cobra.Command{
	Use:   "fireinjective",
	Short: "injective block fetching and tooling",
	Args:  cobra.ExactArgs(1),
}

func init() {
	logging.InstantiateLoggers(logging.WithDefaultLevel(zap.InfoLevel))

	fetchCmd := &cobra.Command{
		Use:   "fetch",
		Short: "fetch blocks from different sources",
		Args:  cobra.ExactArgs(2),
	}
	fetchCmd.AddCommand(cmd.NewFetchRPCCmd(logger, tracer, "mantra"))

	rootCmd.AddCommand(fetchCmd)
	rootCmd.AddCommand(cmd.NewToolsFixUnknownTypeBlocks(logger, tracer))
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Whoops. There was an error while executing your CLI '%s'", err)
		os.Exit(1)
	}
}
