package poller

import (
	"fmt"

	"github.com/cometbft/cometbft/state"
	txIndex "github.com/cometbft/cometbft/state/txindex/kv"
	"github.com/cometbft/cometbft/store"
	pbcosmos "github.com/streamingfast/firehose-cosmos/cosmos/pb/sf/cosmos/type/v2"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type BlockLoader struct {
	blockStore   *store.BlockStore
	stateStore   state.Store
	txIndexStore *txIndex.TxIndex
	logger       *zap.Logger
}

func NewLoader(blockStore *store.BlockStore, stateStore state.Store, txIndexStore *txIndex.TxIndex, logger *zap.Logger) *BlockLoader {
	return &BlockLoader{
		blockStore:   blockStore,
		stateStore:   stateStore,
		txIndexStore: txIndexStore,
		logger:       logger,
	}
}

func (l *BlockLoader) BlockRange() (int64, int64) {
	first := l.blockStore.Base()
	last := l.blockStore.Height()

	return first, last
}

func (l *BlockLoader) loadBlock(height int64) (*pbcosmos.Block, error) {
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

	header := &pbcosmos.Header{}
	err = protoFlip(blockMeta.Header.ToProto(), header)
	if err != nil {
		return nil, fmt.Errorf("converting block meta header: %w", err)
	}

	consensusParamUpdates := &pbcosmos.ConsensusParams{}
	err = protoFlip(abciResponses.EndBlock.ConsensusParamUpdates, consensusParamUpdates)
	if err != nil {
		return nil, fmt.Errorf("converting block meta header: %w", err)
	}

	abciEvents := abciResponses.BeginBlock.Events
	abciEvents = append(abciEvents, abciResponses.EndBlock.Events...)
	events := make([]*pbcosmos.Event, len(abciEvents))
	for i := range events {
		events[i] = &pbcosmos.Event{}
	}
	err = arrayProtoFlip(arrayToPointerArray(abciEvents), events)
	if err != nil {
		return nil, fmt.Errorf("converting events: %w", err)
	}

	//trxResults := make([]*pbcosmos.TxResults, len(abciResponses.DeliverTxs))
	//for i, _ := range trxResults {
	//	trxResults[i] = &pbcosmos.TxResults{}
	//}
	//err = arrayProtoFlip(abciResponses.DeliverTxs, trxResults)
	//if err != nil {
	//	return nil, fmt.Errorf("converting tx results: %w", err)
	//}

	trxResults, err := convertDeliverTxs(abciResponses.DeliverTxs)
	if err != nil {
		return nil, fmt.Errorf("converting tx results: %w", err)
	}

	validators := make([]*pbcosmos.ValidatorUpdate, len(abciResponses.EndBlock.ValidatorUpdates))
	for i := range validators {
		validators[i] = &pbcosmos.ValidatorUpdate{}
	}
	err = arrayProtoFlip(arrayToPointerArray(abciResponses.EndBlock.ValidatorUpdates), validators)
	if err != nil {
		return nil, fmt.Errorf("converting validators: %w", err)
	}

	misbehaviors, err := MisbehaviorsFromEvidences(block.Evidence.Evidence)
	if err != nil {
		return nil, fmt.Errorf("converting misbehaviors from evidences: %w", err)
	}

	pbBlock := &pbcosmos.Block{
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
