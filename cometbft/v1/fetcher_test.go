package v1

import (
	"testing"
	"time"

	abci "github.com/cometbft/cometbft/abci/types"
	cmtversion "github.com/cometbft/cometbft/api/cometbft/version/v1"
	"github.com/cometbft/cometbft/crypto/ed25519"
	ctypes "github.com/cometbft/cometbft/rpc/core/types"
	"github.com/cometbft/cometbft/types"
	"github.com/stretchr/testify/require"

	pbcosmos "buf.build/gen/go/streamingfast/firehose-cosmos/protocolbuffers/go/sf/cosmos/type/v2"
)

func TestConvertBlockFromResponse(t *testing.T) {
	blockTime := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	rpcBlock := &ctypes.ResultBlock{
		Block: &types.Block{
			Header: types.Header{
				Version: cmtversion.Consensus{Block: 11, App: 6},
				ChainID: "injective-1",
				Height:  100,
				Time:    blockTime,
				LastBlockID: types.BlockID{
					Hash:          []byte("last-block"),
					PartSetHeader: types.PartSetHeader{Total: 3, Hash: []byte("part-set")},
				},
				LastCommitHash:     []byte("last-commit"),
				DataHash:           []byte("data"),
				ValidatorsHash:     []byte("vals"),
				NextValidatorsHash: []byte("next-vals"),
				ConsensusHash:      []byte("cons"),
				AppHash:            []byte("app-hash"),
				LastResultsHash:    []byte("last-results"),
				EvidenceHash:       []byte("evidence"),
				ProposerAddress:    []byte("proposer"),
			},
			Data: types.Data{Txs: types.ToTxs([][]byte{[]byte("tx-0"), []byte("tx-1")})},
		},
	}
	rpcResults := &ctypes.ResultBlockResults{
		TxResults: []*abci.ExecTxResult{{
			Code:      0,
			Log:       "ok",
			GasUsed:   8,
			GasWanted: 10,
		}},
		FinalizeBlockEvents: []abci.Event{{
			Type: "coin_received",
			Attributes: []abci.EventAttribute{{
				Key:   "receiver",
				Value: "inj1abc",
			}},
		}},
		ValidatorUpdates: []abci.ValidatorUpdate{{
			Power:       100,
			PubKeyBytes: []byte("ed25519-pubkey-bytes________32"),
			PubKeyType:  ed25519.KeyType,
		}},
	}

	got, err := convertBlockFromResponse(rpcBlock, rpcResults)
	require.NoError(t, err)
	require.Equal(t, uint64(100), got.Number)

	block := &pbcosmos.Block{}
	require.NoError(t, got.Payload.UnmarshalTo(block))
	require.Equal(t, "injective-1", block.Header.ChainId)
	require.Equal(t, []byte("last-block"), block.Header.LastBlockId.Hash)
	require.Equal(t, uint32(3), block.Header.LastBlockId.PartSetHeader.Total)
	require.Equal(t, []byte("app-hash"), block.Header.AppHash)
	require.Equal(t, uint64(6), block.Header.Version.App)
	require.Equal(t, rpcBlock.Block.Txs.ToSliceOfBytes(), block.Txs)
	require.Equal(t, "coin_received", block.Events[0].Type)
	require.Equal(t, []byte("ed25519-pubkey-bytes________32"), block.ValidatorUpdates[0].PubKey.GetEd25519())
}
