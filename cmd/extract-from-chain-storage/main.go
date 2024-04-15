package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/streamingfast/firehose-cosmos/block/injective"

	dbm "github.com/cometbft/cometbft-db"
	"github.com/cometbft/cometbft/state"
	txindexkv "github.com/cometbft/cometbft/state/txindex/kv"
	"github.com/cometbft/cometbft/store"
	"github.com/streamingfast/dstore"
	"github.com/streamingfast/logging"
	"go.uber.org/zap"
)

var logger, tracer = logging.PackageLogger("firecosmos", "github.com/streamingfast/firehose-cosmos")

func main() {
	if err := Main(); err != nil {
		logger.Error("failed", zap.Error(err))
	}
}

func Main() error {
	logging.InstantiateLoggers(logging.WithDefaultLevel(zap.InfoLevel))

	//homeDir := "/Users/cbillett/t/injective/home/data/"
	//destStore, err := dstore.NewDBinStore("file:///Users/cbillett/t/injective/merged_blocks")

	homeDir := os.Args[1]
	destStore, err := dstore.NewDBinStore(os.Args[2])
	if err != nil {
		return fmt.Errorf("unable to create destination store: %w", err)
	}

	startBlock, err := strconv.Atoi(os.Args[3])
	if err != nil {
		return fmt.Errorf("unable to parse start block: %w", err)
	}
	endBlock, err := strconv.Atoi(os.Args[4])
	if err != nil {
		return fmt.Errorf("unable to parse end block: %w", err)

	}

	dbType := dbm.BackendType("goleveldb")
	blockDB, err := dbm.NewDB("blockstore", dbType, homeDir)
	if err != nil {
		return err
	}

	blockStore := store.NewBlockStore(blockDB)

	stateDB, err := dbm.NewDB("state", dbType, homeDir)
	if err != nil {
		return err
	}
	stateStore := state.NewStore(stateDB, state.StoreOptions{
		DiscardABCIResponses: false,
	})

	txIndexDB, err := dbm.NewDB("tx_index", dbType, homeDir)
	if err != nil {
		return err
	}
	txIndexStore := txindexkv.NewTxIndex(txIndexDB)

	merger := injective.NewSimpleMerger(blockStore, stateStore, txIndexStore, logger)

	err = merger.GenerateMergeBlock(int64(startBlock), int64(endBlock), destStore)
	if err != nil {
		return fmt.Errorf("error generating merge blocks files: %w", err)
	}

	return nil
}
