package v1

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	abci "github.com/cometbft/cometbft/abci/types"
	cometBftHttp "github.com/cometbft/cometbft/rpc/client/http"
	ctypes "github.com/cometbft/cometbft/rpc/core/types"
	cometType "github.com/cometbft/cometbft/types"
	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"
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

	f.logger.Info("fetching block from rpc", zap.Int64("block_num", requestBlockNumAsInt))
	block, err := wrappedClient.cometHttpClient.Block(ctx, &requestBlockNumAsInt)
	if err != nil {
		f.logger.Warn("failed to fetch block from rpc", zap.Int64("block_num", requestBlockNumAsInt), zap.Error(err), zap.String("rpc_endpoint", wrappedClient.endpoint))
		return nil, nil, fmt.Errorf("fetching block %d from rpc endpoint: %w", requestBlockNumAsInt, err)
	}

	f.logger.Info("fetching block results from rpc", zap.Int64("block_num", requestBlockNumAsInt))
	rpcBlockResults, err := wrappedClient.cometHttpClient.BlockResults(ctx, &requestBlockNumAsInt)
	if err != nil {
		f.logger.Warn("failed to fetch block results from rpc", zap.Int64("block_num", requestBlockNumAsInt), zap.Error(err))
		return nil, nil, fmt.Errorf("fetching block results %d from rpc endpoint: %w", requestBlockNumAsInt, err)
	}

	return block, rpcBlockResults, nil
}

func convertBlockFromResponse(rpcBlock *ctypes.ResultBlock, rpcBlockResults *ctypes.ResultBlockResults) (*pbbstream.Block, error) {
	if rpcBlock == nil || rpcBlock.Block == nil {
		return nil, fmt.Errorf("rpc block is nil")
	}
	if rpcBlockResults == nil {
		return nil, fmt.Errorf("rpc block results is nil")
	}

	block := rpcBlock.Block
	req := abci.FinalizeBlockRequest{
		Txs:                   block.Txs.ToSliceOfBytes(),
		Misbehavior:           misbehaviorsFromEvidence(block.Evidence.Evidence),
		Hash:                  block.Hash(),
		Height:                block.Height,
		Time:                  block.Time,
		NextValidatorsHash:    block.NextValidatorsHash,
		ProposerAddress:       block.ProposerAddress,
		LastBlockHash:         block.LastBlockID.Hash,
		LastBlockPartSetTotal: int64(block.LastBlockID.PartSetHeader.Total),
		LastBlockPartSetHash:  block.LastBlockID.PartSetHeader.Hash,
		LastCommitHash:        block.LastCommitHash,
		DataHash:              block.DataHash,
		ValidatorsHash:        block.ValidatorsHash,
		ConsensusHash:         block.ConsensusHash,
		AppHash:               block.AppHash,
		LastResultsHash:       block.LastResultsHash,
		EvidenceHash:          block.EvidenceHash,
	}
	res := abci.FinalizeBlockResponse{
		Events:                rpcBlockResults.FinalizeBlockEvents,
		TxResults:             rpcBlockResults.TxResults,
		ValidatorUpdates:      rpcBlockResults.ValidatorUpdates,
		ConsensusParamUpdates: rpcBlockResults.ConsensusParamUpdates,
		AppHash:               rpcBlockResults.AppHash,
	}

	cosmosBlock, err := ConvertBlock(block.ChainID, block.Version.App, req, res)
	if err != nil {
		return nil, fmt.Errorf("converting finalize block: %w", err)
	}

	payload, err := anypb.New(cosmosBlock)
	if err != nil {
		return nil, fmt.Errorf("creating payload: %w", err)
	}

	blockTimestamp := timestamppb.New(block.Time)
	blockHash := block.Hash()
	return &pbbstream.Block{
		Number:    uint64(block.Height),
		Id:        hex.EncodeToString(blockHash),
		ParentId:  hex.EncodeToString(block.LastBlockID.Hash),
		Timestamp: blockTimestamp,
		LibNum:    uint64(block.Height - 1),
		ParentNum: uint64(block.Height - 1),
		Payload:   payload,
	}, nil
}

func misbehaviorsFromEvidence(evidences cometType.EvidenceList) []abci.Misbehavior {
	var out []abci.Misbehavior
	for _, e := range evidences {
		out = append(out, e.ABCI()...)
	}
	return out
}
