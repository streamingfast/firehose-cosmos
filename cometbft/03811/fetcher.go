package v03811

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	abci "github.com/cometbft/cometbft/abci/types"
	"github.com/cometbft/cometbft/proto/tendermint/types"
	cometBftHttp "github.com/cometbft/cometbft/rpc/client/http"
	ctypes "github.com/cometbft/cometbft/rpc/core/types"
	cometType "github.com/cometbft/cometbft/types"
	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"
	pbcosmos "github.com/streamingfast/firehose-cosmos/cosmos/pb/sf/cosmos/type/v2"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type CometHttpClientWrap struct {
	endpoint        string
	cometHttpClient *cometBftHttp.HTTP
}

func NewCometHttpClientWrap(endpoint string, cometHttpClient *cometBftHttp.HTTP) *CometHttpClientWrap {
	return &CometHttpClientWrap{
		endpoint:        endpoint,
		cometHttpClient: cometHttpClient,
	}
}

type RPCBlockFetcher struct {
	latestBlockRetryInterval time.Duration
	latestBlockNum           uint64
	logger                   *zap.Logger
}

func NewRPCFetcher(latestBlockRetryInterval time.Duration, logger *zap.Logger) *RPCBlockFetcher {
	return &RPCBlockFetcher{
		latestBlockRetryInterval: latestBlockRetryInterval,
		logger:                   logger,
	}
}

func (f *RPCBlockFetcher) IsBlockAvailable(requestedSlot uint64) bool {
	return true
}

func (f *RPCBlockFetcher) fetchLatestBlockNum(ctx context.Context, client *CometHttpClientWrap) (uint64, error) {
	resultChainInfo, err := client.cometHttpClient.BlockchainInfo(ctx, 0, 0)
	if err != nil {
		return 0, err
	}
	return uint64(resultChainInfo.LastHeight), nil

}

func (f *RPCBlockFetcher) Fetch(ctx context.Context, wrappedClient *CometHttpClientWrap, requestBlockNum uint64) (b *pbbstream.Block, skipped bool, err error) {
	f.logger.Info("fetching block", zap.Uint64("block_num", requestBlockNum))

	sleepDuration := time.Duration(0)
	for f.latestBlockNum < requestBlockNum {
		time.Sleep(sleepDuration)

		f.latestBlockNum, err = f.fetchLatestBlockNum(ctx, wrappedClient)
		if err != nil {
			return nil, false, fmt.Errorf("fetching latest block num: %w", err)
		}

		f.logger.Info("got latest block num", zap.Uint64("latest_block_num", f.latestBlockNum), zap.Uint64("requested_block_num", requestBlockNum))

		if f.latestBlockNum >= requestBlockNum {
			break
		}
		sleepDuration = f.latestBlockRetryInterval
	}

	f.logger.Info("fetching block", zap.Uint64("block_num", requestBlockNum))
	rpcBlockResponse, rpcBlockResults, err := f.fetch(ctx, wrappedClient, requestBlockNum)
	if err != nil {
		return nil, false, fmt.Errorf("fetching block %d: %w", requestBlockNum, err)
	}

	f.logger.Info("converting block", zap.Uint64("block_num", requestBlockNum))
	bstreamBlock, err := convertBlockFromResponse(rpcBlockResponse, rpcBlockResults)
	if err != nil {
		return nil, false, fmt.Errorf("converting block %d from rpc response: %w", requestBlockNum, err)
	}

	return bstreamBlock, false, nil
}

func (f *RPCBlockFetcher) fetch(ctx context.Context, wrappedClient *CometHttpClientWrap, requestBlockNum uint64) (*ctypes.ResultBlock, *ctypes.ResultBlockResults, error) {
	requestBlockNumAsInt := int64(requestBlockNum)
	var block *ctypes.ResultBlock
	var rpcBlockResults *ctypes.ResultBlockResults

	f.logger.Info("fetching block from rpc", zap.Int64("block_num", requestBlockNumAsInt))

	block, err := wrappedClient.cometHttpClient.Block(ctx, &requestBlockNumAsInt)
	if err != nil {
		f.logger.Warn("failed to fetch block from rpc", zap.Int64("block_num", requestBlockNumAsInt), zap.Error(err), zap.String("rpc_endpoint", wrappedClient.endpoint))
		return nil, nil, fmt.Errorf("fetching block %d from rpc endpoint: %w", requestBlockNumAsInt, err)
	}

	f.logger.Info("fetching block results from rpc", zap.Int64("block_num", requestBlockNumAsInt))
	rpcBlockResults, err = wrappedClient.cometHttpClient.BlockResults(ctx, &requestBlockNumAsInt)
	if err != nil {
		f.logger.Warn("failed to fetch block results from rpc", zap.Int64("block_num", requestBlockNumAsInt), zap.Error(err))
		return nil, nil, fmt.Errorf("fetching block results %d from rpc endpoint: %w", requestBlockNumAsInt, err)
	}

	return block, rpcBlockResults, nil
}

func convertBlockFromResponse(rpcBlock *ctypes.ResultBlock, rpcBlockResults *ctypes.ResultBlockResults) (*pbbstream.Block, error) {
	blockTimestamp := timestamppb.New(rpcBlock.Block.Time)
	blockHeight := rpcBlock.Block.Height
	blockHash := rpcBlock.Block.Hash()

	id := hex.EncodeToString(rpcBlock.Block.Hash())
	parentId := hex.EncodeToString(rpcBlock.Block.LastBlockID.Hash)

	misbehaviors, err := MisbehaviorsFromEvidences(rpcBlock.Block.Evidence.Evidence)
	if err != nil {
		return nil, fmt.Errorf("converting misbehaviors: %w", err)
	}

	header, err := convertHeaderFromResponse(&rpcBlock.Block.Header)
	if err != nil {
		return nil, fmt.Errorf("converting header from response: %w", err)
	}

	txResults, err := convertDeliverTxs(rpcBlockResults.TxsResults)

	validatorUpdates, err := convertValidatorUpdatesFromResponse(rpcBlockResults.ValidatorUpdates)
	if err != nil {
		return nil, fmt.Errorf("converting validator updates: %w", err)
	}

	consensusParamUpdates, err := convertConsensusParamUpdatesFromResponse(rpcBlockResults.ConsensusParamUpdates)
	if err != nil {
		return nil, fmt.Errorf("converting consensus param updates: %w", err)
	}

	finalEvents := rpcBlockResults.FinalizeBlockEvents
	events, err := convertEventsFromResponse(finalEvents)
	if err != nil {
		return nil, fmt.Errorf("converting events: %w", err)
	}

	injectiveBlock := &pbcosmos.Block{
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
		Id:        id,
		ParentId:  parentId,
		Timestamp: blockTimestamp,
		LibNum:    uint64(blockHeight - 1),
		ParentNum: uint64(blockHeight - 1),
		Payload:   payload,
	}

	return bstreamBlock, nil
}

func convertEventsFromResponse(responseEvents []abci.Event) ([]*pbcosmos.Event, error) {
	events := make([]*pbcosmos.Event, len(responseEvents))
	for i := range events {
		events[i] = &pbcosmos.Event{}
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

func convertHeaderFromResponse(responseHeader *cometType.Header) (*pbcosmos.Header, error) {
	header := &pbcosmos.Header{}

	err := protoFlip(responseHeader.ToProto(), header)
	if err != nil {
		return nil, fmt.Errorf("converting block meta header: %w", err)
	}

	return header, nil
}

func convertValidatorUpdatesFromResponse(validatorUpdates []abci.ValidatorUpdate) ([]*pbcosmos.ValidatorUpdate, error) {
	validators := make([]*pbcosmos.ValidatorUpdate, len(validatorUpdates))
	for i := range validators {
		validators[i] = &pbcosmos.ValidatorUpdate{}
	}

	err := arrayProtoFlip(arrayToPointerArray(validatorUpdates), validators)
	if err != nil {
		return nil, fmt.Errorf("converting validators: %w", err)
	}
	return validators, nil
}

func convertConsensusParamUpdatesFromResponse(consensusParamUpdates *types.ConsensusParams) (*pbcosmos.ConsensusParams, error) {
	out := &pbcosmos.ConsensusParams{}
	err := protoFlip(consensusParamUpdates, out)
	if err != nil {
		return nil, fmt.Errorf("converting consensus param updates: %w", err)
	}
	return out, nil
}

func convertResponseDeliverTx(tx *abci.ExecTxResult) (*pbcosmos.TxResults, error) {
	events := make([]*pbcosmos.Event, len(tx.Events))
	for i, _ := range events {
		events[i] = &pbcosmos.Event{}
	}
	err := arrayProtoFlip(arrayToPointerArray(tx.Events), events)
	if err != nil {
		return nil, fmt.Errorf("converting events: %w", err)
	}

	fixedLog := strings.Map(fixUtf, tx.Log)

	txResults := &pbcosmos.TxResults{
		Code:      tx.Code,
		Data:      tx.Data,
		Log:       fixedLog,
		Info:      tx.Info,
		GasWanted: tx.GasWanted,
		GasUsed:   tx.GasUsed,
		Events:    events,
		Codespace: tx.Codespace,
	}

	return txResults, nil
}

func convertDeliverTxs(txs []*abci.ExecTxResult) ([]*pbcosmos.TxResults, error) {
	txResults := make([]*pbcosmos.TxResults, len(txs))
	for i, tx := range txs {
		txResults[i], _ = convertResponseDeliverTx(tx)
	}
	return txResults, nil
}

func MisbehaviorsFromEvidences(evidences cometType.EvidenceList) ([]*pbcosmos.Misbehavior, error) {
	var misbehaviors []*pbcosmos.Misbehavior
	for _, e := range evidences {

		abciMisbehavior := e.ABCI()

		partials := make([]*pbcosmos.Misbehavior, len(abciMisbehavior))
		for i, _ := range partials {
			partials[i] = &pbcosmos.Misbehavior{}
		}

		err := arrayProtoFlip(arrayToPointerArray(abciMisbehavior), partials)
		if err != nil {
			return nil, fmt.Errorf("converting abci misbehavior: %w", err)
		}

		misbehaviors = append(misbehaviors, partials...)
	}
	return misbehaviors, nil
}

func fixUtf(r rune) rune {
	if r == utf8.RuneError {
		fmt.Println("found rune error")
		return '�'
	}
	return r
}
