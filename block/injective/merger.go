package injective

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"reflect"

	abci "github.com/cometbft/cometbft/abci/types"
	"github.com/cometbft/cometbft/state"
	txIndex "github.com/cometbft/cometbft/state/txindex/kv"
	"github.com/cometbft/cometbft/store"
	cometType "github.com/cometbft/cometbft/types"
	cosmoProto "github.com/cosmos/gogoproto/proto"
	"github.com/streamingfast/bstream"
	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"
	"github.com/streamingfast/dstore"
	pbinj "github.com/streamingfast/firehose-cosmos/pb/sf/injective/type/v1"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
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
	abciResponses, err := l.stateStore.LoadABCIResponses(height)
	if err != nil {
		return nil, fmt.Errorf("loading finalize block response: %w", err)
	}

	//var txResults []*abci.ExecTxResult
	//for _, tx := range block.Txs {
	//	hash := tx.Hash()
	//
	//	res, err := l.txIndexStore.Get(hash)
	//	if err != nil {
	//		return nil, fmt.Errorf("loading tx index for hash: %w", err)
	//	}
	//	if res == nil {
	//		return nil, fmt.Errorf("tx index not found for hash %s", hash)
	//	}
	//	txResults = append(txResults, &res.Result)
	//}
	//abciResponses.TxResults = txResults

	header := &pbinj.Header{}
	err = protoFlip(blockMeta.Header.ToProto(), header)
	if err != nil {
		return nil, fmt.Errorf("converting block meta header: %w", err)
	}

	consensusParamUpdates := &pbinj.ConsensusParams{}
	err = protoFlip(abciResponses.EndBlock.ConsensusParamUpdates, consensusParamUpdates)
	if err != nil {
		return nil, fmt.Errorf("converting block meta header: %w", err)
	}

	abciEvents := abciResponses.BeginBlock.Events
	abciEvents = append(abciEvents, abciResponses.EndBlock.Events...)
	events := make([]*pbinj.Event, len(abciEvents))
	for i, _ := range events {
		events[i] = &pbinj.Event{}
	}
	err = arrayProtoFlip(arrayToPointerArray(abciEvents), events)
	if err != nil {
		return nil, fmt.Errorf("converting events: %w", err)
	}

	trxResults := make([]*pbinj.TxResults, len(abciResponses.DeliverTxs))
	for i, _ := range trxResults {
		trxResults[i] = &pbinj.TxResults{}
	}
	err = arrayProtoFlip(abciResponses.DeliverTxs, trxResults)
	if err != nil {
		return nil, fmt.Errorf("converting tx results: %w", err)
	}

	validators := make([]*pbinj.ValidatorUpdate, len(abciResponses.EndBlock.ValidatorUpdates))
	for i, _ := range validators {
		validators[i] = &pbinj.ValidatorUpdate{}
	}
	err = arrayProtoFlip(arrayToPointerArray(abciResponses.EndBlock.ValidatorUpdates), validators)
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

func arrayToPointerArray[T any](ts []T) []*T {
	res := make([]*T, len(ts))
	for i, t := range ts {
		res[i] = &t
	}
	return res
}

func arrayProtoFlip[U cosmoProto.Message, V proto.Message](origins []U, targets []V) error {
	if len(origins) != len(targets) {
		return fmt.Errorf("origin and target arrays have different lengths: %d != %d", len(origins), len(targets))
	}
	if len(origins) == 0 {
		return nil
	}

	for i := range origins {
		err := protoFlip(origins[i], targets[i])
		if err != nil {
			return fmt.Errorf("converting element %d: %w", i, err)
		}
	}

	return nil
}

func protoFlip(origin cosmoProto.Message, target proto.Message) error {
	if origin == nil || reflect.ValueOf(origin).IsNil() {
		return nil
	}
	//marshall origin the unmarshall to target
	data, err := cosmoProto.Marshal(origin)
	if err != nil {
		return fmt.Errorf("mashalling origin object %T: %w", data, err)
	}

	err = proto.Unmarshal(data, target)
	if err != nil {
		if e, ok := origin.(*abci.Event); ok {

			fmt.Println("event data:", data)
			fmt.Println("type bytes:", []byte(e.Type))
			fmt.Printf("origin event type %q\n", e.Type)
			for _, attr := range e.Attributes {
				fmt.Printf("key: %q, value: %q %t	\n", attr.Key, attr.Value, attr.Index)
			}

			fmt.Println("data:", string(data))
		}
		return fmt.Errorf("unmashalling target object %T: %w", data, err)
	}

	return nil
}

func MisbehaviorsFromEvidences(evidences cometType.EvidenceList) ([]*pbinj.Misbehavior, error) {
	var misbehaviors []*pbinj.Misbehavior
	for _, e := range evidences {

		abciMisbehavior := e.ABCI()

		var partial []*pbinj.Misbehavior
		err := arrayProtoFlip(arrayToPointerArray(abciMisbehavior), partial)
		if err != nil {
			return nil, fmt.Errorf("converting abci misbehavior: %w", err)
		}

		misbehaviors = append(misbehaviors, partial...)
	}
	return misbehaviors, nil
}

func filename(num int64) string {
	return fmt.Sprintf("%010d", num)
}
