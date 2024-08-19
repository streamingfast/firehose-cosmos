package main

import (
	"encoding/hex"
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
	"github.com/streamingfast/logging"
	"go.uber.org/zap"
)

func NewToolsFixBlockHashes(logger *zap.Logger, tracer logging.Tracer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "fix-unknown-type-blocks <first-streamable-block> <stop-block> <src-blocks-store> <dst-blocks-store>",
		Short: "firecosmos tool to fix unknown type blocks",
		Long: cli.Dedent(`
			Firecosmos tool to fix unknown type blocks. This tool will read blocks from a source
			and write the fixed block to a destination.
		`),
		Args: cobra.ExactArgs(4),
		RunE: fixBlockHashesRunE(logger, tracer),
	}

	return cmd
}

func fixBlockHashesRunE(zlog *zap.Logger, tracer logging.Tracer) firecore.CommandExecutor {
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

		zlog.Info(
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

				idBytes, err := hex.DecodeString(currentBlock.Id)
				if err != nil {
					return fmt.Errorf("decoding block id: %w", err)
				}
				parentIdBytes, err := hex.DecodeString(currentBlock.ParentId)
				if err != nil {
					return fmt.Errorf("decoding parent id: %w", err)
				}

				id := hex.EncodeToString(idBytes)
				parentId := hex.EncodeToString(parentIdBytes)
				currentBlock.Id = id
				currentBlock.ParentId = parentId

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
