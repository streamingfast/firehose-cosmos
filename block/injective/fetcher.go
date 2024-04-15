package injective

import (
	"context"
	"fmt"
	"math"
	"time"

	abci "github.com/cometbft/cometbft/abci/types"
	"github.com/cometbft/cometbft/proto/tendermint/types"
	cometBftHttp "github.com/cometbft/cometbft/rpc/client/http"
	ctypes "github.com/cometbft/cometbft/rpc/core/types"
	cometType "github.com/cometbft/cometbft/types"
	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"
	"github.com/streamingfast/derr"
	pbinj "github.com/streamingfast/firehose-cosmos/pb/sf/injective/type/v1"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type RPCFetcher struct {
	rpcClient                *cometBftHttp.HTTP
	fetchInterval            time.Duration
	latestBlockRetryInterval time.Duration
	latestBlockNum           uint64
	logger                   *zap.Logger
}

func NewRPCFetcher(rpcClient *cometBftHttp.HTTP, fetchInterval time.Duration, latestBlockRetryInterval time.Duration, logger *zap.Logger) *RPCFetcher {
	return &RPCFetcher{
		rpcClient:                rpcClient,
		fetchInterval:            fetchInterval,
		latestBlockRetryInterval: latestBlockRetryInterval,
		logger:                   logger,
	}
}

func (f *RPCFetcher) IsBlockAvailable(requestedSlot uint64) bool {
	return true
}

func (f *RPCFetcher) Fetch(ctx context.Context, requestBlockNum uint64) (b *pbbstream.Block, skipped bool, err error) {
	f.logger.Info("fetching block", zap.Uint64("block_num", requestBlockNum))

	sleepDuration := time.Duration(0)
	for f.latestBlockNum < requestBlockNum {
		time.Sleep(sleepDuration)
		resultChainInfo, err := f.rpcClient.BlockchainInfo(ctx, 0, 0)
		if err != nil {
			return nil, false, fmt.Errorf("fetching last block num: %w", err)
		}

		f.latestBlockNum = uint64(resultChainInfo.LastHeight)

		f.logger.Info("got latest block num", zap.Uint64("latest_block_num", f.latestBlockNum), zap.Uint64("requested_block_num", requestBlockNum))

		if f.latestBlockNum >= requestBlockNum {
			break
		}
		sleepDuration = f.latestBlockRetryInterval
	}

	rpcBlockResponse, rpcBlockResults, err := f.fetch(requestBlockNum)
	if err != nil {
		return nil, false, fmt.Errorf("fetching block %d: %w", requestBlockNum, err)
	}

	bstreamBlock, err := convertBlockFromResponse(rpcBlockResponse, rpcBlockResults)
	if err != nil {
		return nil, false, fmt.Errorf("converting block %d from rpc response: %w", requestBlockNum, err)
	}

	return bstreamBlock, false, nil
}

func (f *RPCFetcher) fetch(requestBlockNum uint64) (*ctypes.ResultBlock, *ctypes.ResultBlockResults, error) {
	requestBlockNumAsInt := int64(requestBlockNum)
	var rpcBlockResponse *ctypes.ResultBlock
	var rpcBlockResults *ctypes.ResultBlockResults
	err := derr.Retry(math.MaxUint64, func(ctx context.Context) error {
		var err error
		rpcBlockResponse, err = f.rpcClient.Block(ctx, &requestBlockNumAsInt)
		if err != nil {
			return fmt.Errorf("fetching block %d from rpc endpoint: %w", requestBlockNumAsInt, err)
		}

		rpcBlockResults, err = f.rpcClient.BlockResults(ctx, &requestBlockNumAsInt)
		if err != nil {
			return fmt.Errorf("fetching block results %d from rpc endpoint: %w", requestBlockNumAsInt, err)
		}

		return nil
	})
	if err != nil {
		return nil, nil, fmt.Errorf("after retrying fetch block %d: %w", requestBlockNum, err)
	}

	return rpcBlockResponse, rpcBlockResults, nil
}

func convertBlockFromResponse(rpcBlock *ctypes.ResultBlock, rpcBlockResults *ctypes.ResultBlockResults) (*pbbstream.Block, error) {
	blockTimestamp := timestamppb.New(rpcBlock.Block.Time)
	blockHeight := rpcBlock.Block.Height
	blockHash := rpcBlock.Block.Hash()
	prevBlockHash := rpcBlock.Block.LastBlockID.Hash.String()
	misbehaviors, err := MisbehaviorsFromEvidences(rpcBlock.Block.Evidence.Evidence)
	if err != nil {
		return nil, fmt.Errorf("converting misbehaviors: %w", err)
	}

	header, err := convertHeaderFromResponse(&rpcBlock.Block.Header)
	if err != nil {
		return nil, fmt.Errorf("converting header from response: %w", err)
	}

	txResults, err := convertTxResultsFromResponse(rpcBlockResults.TxsResults)
	if err != nil {
		return nil, fmt.Errorf("converting tx results: %w", err)
	}

	validatorUpdates, err := convertValidatorUpdatesFromResponse(rpcBlockResults.ValidatorUpdates)
	if err != nil {
		return nil, fmt.Errorf("converting validator updates: %w", err)
	}

	consensusParamUpdates, err := convertConsensusParamUpdatesFromResponse(rpcBlockResults.ConsensusParamUpdates)
	if err != nil {
		return nil, fmt.Errorf("converting consensus param updates: %w", err)
	}

	finalEvents := rpcBlockResults.BeginBlockEvents
	finalEvents = append(finalEvents, rpcBlockResults.EndBlockEvents...)
	events, err := convertEventsFromResponse(finalEvents)
	if err != nil {
		return nil, fmt.Errorf("converting events: %w", err)
	}

	injectiveBlock := &pbinj.Block{
		Hash:                  blockHash,
		Height:                blockHeight,
		Time:                  blockTimestamp,
		Header:                header,
		Misbehavior:           misbehaviors,
		Events:                events,
		Txs:                   convertTxsFromResponse(rpcBlock.Block.Txs),
		TxResults:             txResults,
		ValidatorUpdates:      validatorUpdates,
		ConsensusParamUpdates: consensusParamUpdates,
	}

	payload, err := anypb.New(injectiveBlock)
	if err != nil {
		return nil, fmt.Errorf("creating payload: %w", err)
	}

	bstreamBlock := &pbbstream.Block{
		Number:    uint64(blockHeight),
		Id:        blockHash.String(),
		ParentId:  prevBlockHash,
		Timestamp: blockTimestamp,
		LibNum:    uint64(blockHeight - 1),
		ParentNum: uint64(blockHeight - 1),
		Payload:   payload,
	}

	return bstreamBlock, nil
}

func convertEventsFromResponse(responseEvents []abci.Event) ([]*pbinj.Event, error) {
	events := make([]*pbinj.Event, len(responseEvents))
	for i := range events {
		events[i] = &pbinj.Event{}
	}

	err := arrayProtoFlip(arrayToPointerArray(responseEvents), events)
	if err != nil {
		return nil, fmt.Errorf("converting events: %w", err)
	}
	return events, nil
}

func convertTxsFromResponse(transactions cometType.Txs) (txs [][]byte) {
	return transactions.ToSliceOfBytes()
}

func convertTxResultsFromResponse(transactionResults cometType.ABCIResults) ([]*pbinj.TxResults, error) {
	trxResults := make([]*pbinj.TxResults, len(transactionResults))
	for i := range trxResults {
		trxResults[i] = &pbinj.TxResults{}
	}

	err := arrayProtoFlip(transactionResults, trxResults)
	if err != nil {
		return nil, fmt.Errorf("converting tx results: %w", err)
	}
	return trxResults, nil
}

func convertHeaderFromResponse(responseHeader *cometType.Header) (*pbinj.Header, error) {
	header := &pbinj.Header{}

	err := protoFlip(responseHeader.ToProto(), header)
	if err != nil {
		return nil, fmt.Errorf("converting block meta header: %w", err)
	}

	return header, nil
}

func convertValidatorUpdatesFromResponse(validatorUpdates []abci.ValidatorUpdate) ([]*pbinj.ValidatorUpdate, error) {
	validators := make([]*pbinj.ValidatorUpdate, len(validatorUpdates))
	for i := range validators {
		validators[i] = &pbinj.ValidatorUpdate{}
	}

	err := arrayProtoFlip(arrayToPointerArray(validatorUpdates), validators)
	if err != nil {
		return nil, fmt.Errorf("converting validators: %w", err)
	}
	return validators, nil
}

func convertConsensusParamUpdatesFromResponse(consensusParamUpdates *types.ConsensusParams) (*pbinj.ConsensusParams, error) {
	out := &pbinj.ConsensusParams{}
	err := protoFlip(consensusParamUpdates, out)
	if err != nil {
		return nil, fmt.Errorf("converting consensus param updates: %w", err)
	}
	return out, nil
}
