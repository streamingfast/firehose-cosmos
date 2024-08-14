package cmd

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"

	// gogoproto doesn't behave as expected, should we go with google.golang.org/protobuf/proto instead?
	// "github.com/cosmos/gogoproto/proto"
	"github.com/spf13/cobra"
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/cli"
	"github.com/streamingfast/dstore"
	firecore "github.com/streamingfast/firehose-core"
	"github.com/streamingfast/firehose-cosmos/cosmos/block"
	pbcosmos "github.com/streamingfast/firehose-cosmos/cosmos/pb/sf/cosmos/type/v2"
	"github.com/streamingfast/logging"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/anypb"
)

func NewToolsFixUnknownTypeBlocks(logger *zap.Logger, tracer logging.Tracer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "fix-unknown-type-blocks <first-streamable-block> <stop-block> <src-blocks-store> <dst-blocks-store>",
		Short: "injective firecosmos tool to fix unknown type blocks",
		Long: cli.Dedent(`
			Injective firecosmos tool to fix unknown type blocks. This tool will read blocks from a source
			and write the fixed block to a destination.
		`),
		Args: cobra.ExactArgs(4),
		RunE: fixUnknownTypeBlocksRunE(logger, tracer),
	}

	return cmd
}

func fixUnknownTypeBlocksRunE(zlog *zap.Logger, tracer logging.Tracer) firecore.CommandExecutor {
	return func(cmd *cobra.Command, args []string) (err error) {
		ctx := cmd.Context()

		firstStreamableBlock, err := strconv.ParseUint(args[0], 10, 64)
		if err != nil {
			return fmt.Errorf("converting first streamable block to uint64: %w", err)
		}

		stopBlock, err := strconv.ParseUint(args[1], 10, 64)
		if err != nil {
			return fmt.Errorf("converting stop block to uint64: %w", err)
		}

		srcStore, err := dstore.NewDBinStore(args[2])
		if err != nil {
			return fmt.Errorf("unable to create source store: %w", err)
		}

		destStore, err := dstore.NewDBinStore(args[3])
		if err != nil {
			return fmt.Errorf("unable to create destination store: %w", err)
		}

		zlog.Debug(
			"starting fix unknown type blocks",
			zap.String("source_blocks_store", srcStore.BaseURL().String()),
			zap.String("destination_blocks_store", destStore.BaseURL().String()),
			zap.Uint64("first_streamable_block", firstStreamableBlock),
			zap.Uint64("stop_block", stopBlock),
		)

		mergeWriter := &firecore.MergedBlocksWriter{
			Store:        destStore,
			LowBlockNum:  firstStreamableBlock,
			StopBlockNum: stopBlock,
			Logger:       zlog,
			Cmd:          cmd,
		}

		var lastFilename string
		var blockCount = 0
		err = srcStore.WalkFrom(ctx, "", fmt.Sprintf("%010d", firstStreamableBlock), func(filename string) error {
			var fileReader io.Reader
			fileReader, err = srcStore.OpenObject(ctx, filename)
			if err != nil {
				return fmt.Errorf("creating reader: %w", err)
			}

			var blockReader *bstream.DBinBlockReader
			blockReader, err = bstream.NewDBinBlockReader(fileReader)
			if err != nil {
				return fmt.Errorf("creating block reader: %w", err)
			}

			// the source store is a merged file store
			for {
				currentBlock, err := blockReader.Read()
				if err != nil {
					if err == io.EOF {
						fmt.Fprintf(os.Stderr, "Total blocks: %d\n", blockCount)
						break
					}
					return fmt.Errorf("error receiving blocks: %w", err)
				}

				blockCount++

				injectiveBlock := &pbcosmos.Block{}
				err = block.UnmarshallerDiscardUnknown.Unmarshal(currentBlock.Payload.Value, injectiveBlock)
				if err != nil {
					return fmt.Errorf("unmarshaling block: %w", err)
				}

				payload, err := anypb.New(injectiveBlock)
				if err != nil {
					return fmt.Errorf("creating payload: %w", err)
				}

				currentBlock.Payload = payload
				if err = mergeWriter.ProcessBlock(currentBlock, nil); err != nil {
					return fmt.Errorf("processing block: %w", err)
				}
			}

			lastFilename = filename
			return nil
		})

		mergeWriter.Logger = mergeWriter.Logger.With(zap.String("last_filename", lastFilename), zap.Int("block_count", blockCount))
		if err != nil {
			if errors.Is(err, dstore.StopIteration) {
				err = mergeWriter.WriteBundle()
				if err != nil {
					return fmt.Errorf("writing bundle: %w", err)
				}
				fmt.Println("done")
				return nil
			}
			if errors.Is(err, io.EOF) {
				fmt.Println("done")
				return nil
			}
			return fmt.Errorf("walking source store: %w", err)
		}

		return nil
	}
}
