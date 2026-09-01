package v1

import (
	"testing"
	"time"

	abci "github.com/cometbft/cometbft/abci/types"
	cmtproto "github.com/cometbft/cometbft/api/cometbft/types/v1"
	"github.com/cometbft/cometbft/crypto/ed25519"
	"github.com/cometbft/cometbft/version"
	gogotypes "github.com/cosmos/gogoproto/types"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	pbcosmos "buf.build/gen/go/streamingfast/firehose-cosmos/protocolbuffers/go/sf/cosmos/type/v2"
)

func TestConvertBlockMatchesPollerLayout(t *testing.T) {
	blockTime := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	req := abci.FinalizeBlockRequest{
		Hash:                  []byte("block-hash-32-bytes___________"),
		Height:                100,
		Time:                  blockTime,
		Txs:                   [][]byte{[]byte("tx-0"), []byte("tx-1")},
		NextValidatorsHash:    []byte("next-vals"),
		ProposerAddress:       []byte("proposer"),
		LastBlockHash:         []byte("last-block"),
		LastBlockPartSetTotal: 3,
		LastBlockPartSetHash:  []byte("part-set"),
		AppHash:               []byte("app-hash"),
		ValidatorsHash:        []byte("vals"),
		ConsensusHash:         []byte("cons"),
		DataHash:              []byte("data"),
		EvidenceHash:          []byte("evidence"),
		LastCommitHash:        []byte("last-commit"),
		LastResultsHash:       []byte("last-results"),
		Misbehavior: []abci.Misbehavior{{
			Type: abci.MISBEHAVIOR_TYPE_DUPLICATE_VOTE,
			Validator: abci.Validator{
				Address: []byte("offender"),
				Power:   50,
			},
			Height:           99,
			Time:             blockTime.Add(-time.Second),
			TotalVotingPower: 1000,
		}},
	}
	res := abci.FinalizeBlockResponse{
		Events: []abci.Event{{
			Type: "coin_received",
			Attributes: []abci.EventAttribute{{
				Key:   "receiver",
				Value: "inj1abc",
				Index: true,
			}},
		}},
		TxResults: []*abci.ExecTxResult{{
			Code:      0,
			Data:      []byte("data"),
			Log:       "ok\xff",
			Info:      "info",
			GasWanted: 10,
			GasUsed:   8,
			Codespace: "sdk",
			Events: []abci.Event{{
				Type: "message",
				Attributes: []abci.EventAttribute{{
					Key:   "action",
					Value: "/cosmos.bank.v1beta1.MsgSend",
					Index: true,
				}},
			}},
		}},
		ValidatorUpdates: []abci.ValidatorUpdate{{
			Power:       100,
			PubKeyBytes: []byte("ed25519-pubkey-bytes________32"),
			PubKeyType:  ed25519.KeyType,
		}},
		ConsensusParamUpdates: &cmtproto.ConsensusParams{
			Block: &cmtproto.BlockParams{MaxBytes: 1000, MaxGas: 2000},
			Evidence: &cmtproto.EvidenceParams{
				MaxAgeNumBlocks: 10,
				MaxAgeDuration:  time.Hour,
				MaxBytes:        50,
			},
			Validator: &cmtproto.ValidatorParams{PubKeyTypes: []string{ed25519.KeyType}},
			Version:   &cmtproto.VersionParams{App: 7},
			Abci:      &cmtproto.ABCIParams{VoteExtensionsEnableHeight: 50},
			Synchrony: &cmtproto.SynchronyParams{
				Precision:    durationPtr(time.Millisecond),
				MessageDelay: durationPtr(2 * time.Second),
			},
			Feature: &cmtproto.FeatureParams{
				VoteExtensionsEnableHeight: &gogotypes.Int64Value{Value: 100},
				PbtsEnableHeight:           &gogotypes.Int64Value{Value: 200},
			},
		},
	}

	got, err := ConvertBlock("injective-1", 6, req, res)
	require.NoError(t, err)

	require.Equal(t, req.Hash, got.Hash)
	require.Equal(t, int64(100), got.Height)
	require.True(t, got.Time.AsTime().Equal(blockTime))

	require.Equal(t, version.BlockProtocol, got.Header.Version.Block)
	require.Equal(t, uint64(6), got.Header.Version.App)
	require.Equal(t, "injective-1", got.Header.ChainId)
	require.Equal(t, req.LastBlockHash, got.Header.LastBlockId.Hash)
	require.Equal(t, uint32(3), got.Header.LastBlockId.PartSetHeader.Total)
	require.Equal(t, req.LastBlockPartSetHash, got.Header.LastBlockId.PartSetHeader.Hash)
	require.Equal(t, req.LastCommitHash, got.Header.LastCommitHash)
	require.Equal(t, req.DataHash, got.Header.DataHash)
	require.Equal(t, req.ValidatorsHash, got.Header.ValidatorsHash)
	require.Equal(t, req.NextValidatorsHash, got.Header.NextValidatorsHash)
	require.Equal(t, req.ConsensusHash, got.Header.ConsensusHash)
	require.Equal(t, req.AppHash, got.Header.AppHash)
	require.Equal(t, req.LastResultsHash, got.Header.LastResultsHash)
	require.Equal(t, req.EvidenceHash, got.Header.EvidenceHash)
	require.Equal(t, req.ProposerAddress, got.Header.ProposerAddress)

	require.Equal(t, req.Txs, got.Txs)
	require.Len(t, got.Misbehavior, 1)
	require.Equal(t, pbcosmos.MisbehaviorType_DUPLICATE_VOTE, got.Misbehavior[0].Type)
	require.Equal(t, []byte("offender"), got.Misbehavior[0].Validator.Address)
	require.Equal(t, int64(50), got.Misbehavior[0].Validator.Power)

	require.Len(t, got.Events, 1)
	require.Equal(t, "coin_received", got.Events[0].Type)
	require.Equal(t, "receiver", got.Events[0].Attributes[0].Key)
	require.Equal(t, "inj1abc", got.Events[0].Attributes[0].Value)

	require.Len(t, got.TxResults, 1)
	require.Equal(t, "ok�", got.TxResults[0].Log)
	require.Equal(t, uint32(0), got.TxResults[0].Code)
	require.Equal(t, int64(8), got.TxResults[0].GasUsed)
	require.Equal(t, "message", got.TxResults[0].Events[0].Type)

	require.Len(t, got.ValidatorUpdates, 1)
	require.Equal(t, int64(100), got.ValidatorUpdates[0].Power)
	require.Equal(t, []byte("ed25519-pubkey-bytes________32"), got.ValidatorUpdates[0].PubKey.GetEd25519())

	require.Equal(t, int64(1000), got.ConsensusParamUpdates.Block.MaxBytes)
	require.Equal(t, int64(2000), got.ConsensusParamUpdates.Block.MaxGas)
	require.Equal(t, uint64(7), got.ConsensusParamUpdates.Version.App)
	require.Equal(t, int64(50), got.ConsensusParamUpdates.Abci.VoteExtensionsEnableHeight)
	require.Equal(t, time.Millisecond, got.ConsensusParamUpdates.Synchrony.Precision.AsDuration())
	require.Equal(t, 2*time.Second, got.ConsensusParamUpdates.Synchrony.MessageDelay.AsDuration())
	require.Equal(t, int64(100), got.ConsensusParamUpdates.Feature.VoteExtensionsEnableHeight.GetValue())
	require.Equal(t, int64(200), got.ConsensusParamUpdates.Feature.PbtsEnableHeight.GetValue())
}

func durationPtr(d time.Duration) *time.Duration {
	return &d
}

func TestConvertEventsDropsIndexFlag(t *testing.T) {
	events, err := convertEvents([]abci.Event{{
		Type: "transfer",
		Attributes: []abci.EventAttribute{{
			Key:   "amount",
			Value: "1inj",
			Index: true,
		}},
	}})
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, "amount", events[0].Attributes[0].Key)

	marshalled, err := proto.Marshal(events[0])
	require.NoError(t, err)
	require.NotContains(t, string(marshalled), "\x08\x01")
	_ = marshalled
}

func TestConvertPublicKeyAcceptsBLS(t *testing.T) {
	pk, err := convertPublicKey("bls12_381", []byte("bls-pubkey-bytes"))
	require.NoError(t, err)
	require.Equal(t, []byte("bls-pubkey-bytes"), pk.GetBls12381())

	pk, err = convertPublicKey("cometbft/PubKeyBls12_381", []byte("bls-pubkey-bytes"))
	require.NoError(t, err)
	require.Equal(t, []byte("bls-pubkey-bytes"), pk.GetBls12381())
}

func TestConvertPublicKeyRejectsUnknown(t *testing.T) {
	_, err := convertPublicKey("mldsa65", []byte{1})
	require.Error(t, err)
}

func TestConvertEventsSanitizesInvalidUTF8(t *testing.T) {
	events, err := convertEvents([]abci.Event{{
		Type: "wasm",
		Attributes: []abci.EventAttribute{{
			Key:   "msg\xff",
			Value: "ok\xffnope",
		}},
	}})
	require.NoError(t, err)
	require.Equal(t, "msg�", events[0].Attributes[0].Key)
	require.Equal(t, "ok�nope", events[0].Attributes[0].Value)

	_, err = proto.Marshal(events[0])
	require.NoError(t, err)
}
