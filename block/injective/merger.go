package injective

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"

	abci "github.com/cometbft/cometbft/abci/types"
	"github.com/cometbft/cometbft/state"
	txIndex "github.com/cometbft/cometbft/state/txindex/kv"
	"github.com/cometbft/cometbft/store"
	"github.com/streamingfast/bstream"
	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"
	"github.com/streamingfast/dstore"
	pbinj "github.com/streamingfast/firehose-cosmos/pb/sf/injective/type/v1"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type SimpleMerge struct {
	blockStore   *store.BlockStore
	stateStore   state.Store
	txIndexStore *txIndex.TxIndex
	logger       *zap.Logger
}

func NewSimpleMerger(blockStore *store.BlockStore, stateStore state.Store, txIndexStore *txIndex.TxIndex, logger *zap.Logger) *SimpleMerge {
	return &SimpleMerge{
		blockStore:   blockStore,
		stateStore:   stateStore,
		txIndexStore: txIndexStore,
		logger:       logger,
	}
}

func (l *SimpleMerge) GenerateMergeBlock(startBlock int64, endBlock int64, destStore dstore.Store) error {
	first := l.blockStore.Base()
	last := l.blockStore.Height()

	l.logger.Info("generation merge blocks", zap.Int64("start_block", startBlock), zap.Int64("end_block", endBlock), zap.Int64("first", first), zap.Int64("last", last))

	if startBlock < first {
		return fmt.Errorf("start block %d is before the first block %d of current snapshot", startBlock, first)
	}
	if startBlock%100 != 0 {
		return fmt.Errorf("start block %d is not a boundary", startBlock)
	}

	if endBlock > last {
		return fmt.Errorf("end block %d is after the last block %d of current snapshot", endBlock, last)
	}

	if endBlock%100 != 0 {
		return fmt.Errorf("end block %d is not a boundary", endBlock)
	}

	height := startBlock
	for {

		if height >= endBlock {
			break
		}

		file := filename(height)

		err := l.ToMergeBlockFile(height, file, destStore)
		if err != nil {
			return fmt.Errorf("generating merge blocks file: %w", err)
		}

		height += 100
	}

	return nil
}

func (l *SimpleMerge) GenerateOneBlock(startBlock int64, endBlock int64, destStore dstore.Store) error {
	first := l.blockStore.Base()
	last := l.blockStore.Height()

	l.logger.Info("generating one blocks", zap.Int64("start_block", startBlock), zap.Int64("end_block", endBlock), zap.Int64("first", first), zap.Int64("last", last))

	if startBlock < first {
		return fmt.Errorf("start block %d is before the first block %d of current snapshot", startBlock, first)
	}

	if endBlock > last {
		return fmt.Errorf("end block %d is after the last block %d of current snapshot", endBlock, last)
	}

	height := startBlock
	for {

		if height > endBlock {
			l.logger.Info("end block reached", zap.Int64("height", height), zap.Int64("end_block", endBlock))
			break
		}

		file := filename(height)

		err := l.ToOneBlockFile(height, file, destStore)
		if err != nil {
			return fmt.Errorf("generating one blocks file: %w", err)
		}

		height++
	}

	return nil
}

func (l *SimpleMerge) ToMergeBlockFile(startBlock int64, filename string, destStore dstore.Store) error {
	l.logger.Info("generating merge blocks file", zap.Int64("start_block", startBlock), zap.String("filename", filename))

	pr, pw := io.Pipe()
	blockWriter, err := bstream.NewDBinBlockWriter(pw)
	if err != nil {
		return fmt.Errorf("creating block writer: %w", err)
	}

	go func() {
		var processErr error
		defer func() {
			err := pw.CloseWithError(processErr)
			if err != nil {
				panic(err)
			}
			l.logger.Info("closing pipe", zap.Error(processErr))
		}()

		for height := startBlock; height <= startBlock+99; height++ {
			l.logger.Debug("loading block", zap.Int64("height", height))
			block, err := l.loadBlock(height)
			if err != nil {
				processErr = fmt.Errorf("loading block %d: %w", height, err)
				return
			}

			payload, err := anypb.New(block)
			if err != nil {
				processErr = fmt.Errorf("creating payload for block %d: %w", height, err)
				return
			}
			bstreamBlock := &pbbstream.Block{
				Number:    uint64(block.Height),
				Id:        hex.EncodeToString(block.Hash),
				ParentId:  hex.EncodeToString(block.Header.LastBlockId.Hash),
				Timestamp: block.Time,
				LibNum:    uint64(block.Height - 1),
				ParentNum: uint64(block.Height - 1),
				Payload:   payload,
			}

			err = blockWriter.Write(bstreamBlock)
			if err != nil {
				processErr = fmt.Errorf("writing block %d: %w", height, err)
				return
			}
		}
		l.logger.Info("finished writing blocks to pipe")
	}()

	l.logger.Info("writing merged file to store", zap.String("filename", filename))
	err = destStore.WriteObject(context.Background(), filename, pr)
	if err != nil {
		return fmt.Errorf("writing to store: %w", err)
	}

	l.logger.Info("finished writing merged file to store", zap.String("filename", filename))
	return nil
}

func (l *SimpleMerge) ToOneBlockFile(blockNum int64, filename string, destStore dstore.Store) error {
	l.logger.Info("generating one blocks file", zap.Int64("block_num", blockNum), zap.String("filename", filename))

	pr, pw := io.Pipe()
	blockWriter, err := bstream.NewDBinBlockWriter(pw)
	if err != nil {
		return fmt.Errorf("creating block writer: %w", err)
	}

	go func() {
		var processErr error
		defer func() {
			err := pw.CloseWithError(processErr)
			if err != nil {
				panic(err)
			}
			l.logger.Info("closing pipe", zap.Error(processErr))
		}()

		l.logger.Debug("loading block", zap.Int64("block_num", blockNum))
		block, err := l.loadBlock(blockNum)
		if err != nil {
			processErr = fmt.Errorf("loading block %d: %w", blockNum, err)
			return
		}

		payload, err := anypb.New(block)
		if err != nil {
			processErr = fmt.Errorf("creating payload for block %d: %w", blockNum, err)
			return
		}
		bstreamBlock := &pbbstream.Block{
			Number:    uint64(block.Height),
			Id:        hex.EncodeToString(block.Hash),
			ParentId:  hex.EncodeToString(block.Header.LastBlockId.Hash),
			Timestamp: block.Time,
			LibNum:    uint64(block.Height - 1),
			ParentNum: uint64(block.Height - 1),
			Payload:   payload,
		}

		err = blockWriter.Write(bstreamBlock)
		if err != nil {
			processErr = fmt.Errorf("writing block %d: %w", blockNum, err)
			return
		}

		l.logger.Info("finished writing blocks to pipe")
	}()

	l.logger.Info("writing merged file to store", zap.String("filename", filename))
	err = destStore.WriteObject(context.Background(), filename, pr)
	if err != nil {
		return fmt.Errorf("writing to store: %w", err)
	}

	l.logger.Info("finished writing merged file to store", zap.String("filename", filename))
	return nil
}

func (l *SimpleMerge) loadBlock(height int64) (*pbinj.Block, error) {

	block := l.blockStore.LoadBlock(height)
	if block == nil {
		return nil, fmt.Errorf("block %d not found", height)
	}
	blockMeta := l.blockStore.LoadBlockMeta(height)
	if blockMeta == nil {
		return nil, fmt.Errorf("block meta %d not found", height)
	}

	blockResponse, err := l.stateStore.LoadFinalizeBlockResponse(height)
	if err != nil {
		return nil, fmt.Errorf("loading finalize block response: %w", err)
	}

	var txResults []*abci.ExecTxResult
	for _, tx := range block.Txs {
		hash := tx.Hash()

		res, err := l.txIndexStore.Get(hash)
		if err != nil {
			return nil, fmt.Errorf("loading tx index for hash: %w", err)
		}
		if res == nil {
			return nil, fmt.Errorf("tx index not found for hash %s", hash)
		}
		txResults = append(txResults, &res.Result)
	}
	blockResponse.TxResults = txResults

	header := &pbinj.Header{}
	err = protoFlip(blockMeta.Header.ToProto(), header)
	if err != nil {
		return nil, fmt.Errorf("converting block meta header: %w", err)
	}

	consensusParamUpdates := &pbinj.ConsensusParams{}
	err = protoFlip(blockResponse.ConsensusParamUpdates, consensusParamUpdates)
	if err != nil {
		return nil, fmt.Errorf("converting block meta header: %w", err)
	}

	events := make([]*pbinj.Event, len(blockResponse.Events))
	err = arrayProtoFlip(arrayToPointerArray(blockResponse.Events), events)
	if err != nil {
		return nil, fmt.Errorf("converting events: %w", err)
	}

	trxResults := make([]*pbinj.TxResults, len(blockResponse.TxResults))
	err = arrayProtoFlip(blockResponse.TxResults, trxResults)
	if err != nil {
		return nil, fmt.Errorf("converting tx results: %w", err)
	}

	validators := make([]*pbinj.ValidatorUpdate, len(blockResponse.ValidatorUpdates))
	err = arrayProtoFlip(arrayToPointerArray(blockResponse.ValidatorUpdates), validators)
	if err != nil {
		return nil, fmt.Errorf("converting validators: %w", err)
	}

	misbehaviors, err := MisbehaviorsFromEvidences(block.Evidence.Evidence)
	if err != nil {
		return nil, fmt.Errorf("converting misbehaviors from evidences: %w", err)
	}

	pbBlock := &pbinj.Block{
		Hash:                  block.Hash(),
		Height:                block.Height,
		Time:                  timestamppb.New(block.Time),
		Header:                header,
		Misbehavior:           misbehaviors,
		Events:                events,
		Txs:                   block.Txs.ToSliceOfBytes(),
		TxResults:             trxResults,
		ValidatorUpdates:      validators,
		ConsensusParamUpdates: consensusParamUpdates,
	}

	return pbBlock, nil
}

func filename(num int64) string {
	return fmt.Sprintf("%010d", num)
}
