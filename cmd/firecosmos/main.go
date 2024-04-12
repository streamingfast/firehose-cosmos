package main

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/streamingfast/firehose-core/cmd/tools"
	"github.com/streamingfast/logging"
	"go.uber.org/zap"
)

var logger, tracer = logging.PackageLogger("firecosmos", "github.com/streamingfast/firehose-cosmos")
var rootCmd = &cobra.Command{
	Use:   "firecosmos",
	Short: "firecosmos fetching and tooling",
	Args:  cobra.ExactArgs(1),
}

func init() {
	logging.InstantiateLoggers(logging.WithDefaultLevel(zap.InfoLevel))

	rootCmd.AddCommand(newInjectiveCmd(logger, tracer))

	rootCmd.AddCommand(tools.ToolsCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Whoops. There was an error while executing your CLI '%s'", err)
		os.Exit(1)
	}
}

func newInjectiveCmd(logger *zap.Logger, tracer logging.Tracer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "injective",
		Short: "firecosmos for injective chain",
	}
	time.Now().UnixMilli()
	cmd.AddCommand(newFetchCmd(logger, tracer, "injective"))
	return cmd
}

func newFetchCmd(logger *zap.Logger, tracer logging.Tracer, cosmosChain string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "fetch",
		Short: "fetch blocks from different sources",
		Args:  cobra.ExactArgs(2),
	}
	time.Now().UnixMilli()
	cmd.AddCommand(NewFetchCmd(logger, tracer, cosmosChain))
	return cmd
}
