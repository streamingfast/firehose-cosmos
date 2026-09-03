package convert

import (
	"fmt"
	"strings"
	"unicode/utf8"

	abci "github.com/cometbft/cometbft/abci/types"
	cmtproto "github.com/cometbft/cometbft/api/cometbft/types/v1"
	"github.com/cometbft/cometbft/crypto/ed25519"
	"github.com/cometbft/cometbft/crypto/secp256k1"
	"github.com/cometbft/cometbft/version"
	"google.golang.org/protobuf/types/known/timestamppb"

	pbcosmos "buf.build/gen/go/streamingfast/firehose-cosmos/protocolbuffers/go/sf/cosmos/type/v2"
)

func ConvertBlock(chainID string, versionApp uint64, req abci.FinalizeBlockRequest, res abci.FinalizeBlockResponse) (*pbcosmos.Block, error) {
	misbehaviors, err := convertMisbehaviors(req.Misbehavior)
	if err != nil {
		return nil, fmt.Errorf("converting misbehaviors: %w", err)
	}

	events, err := convertEvents(res.Events)
	if err != nil {
		return nil, fmt.Errorf("converting events: %w", err)
	}

	txResults, err := convertTxResults(res.TxResults)
	if err != nil {
		return nil, fmt.Errorf("converting tx results: %w", err)
	}

	validatorUpdates, err := convertValidatorUpdates(res.ValidatorUpdates)
	if err != nil {
		return nil, fmt.Errorf("converting validator updates: %w", err)
	}

	consensusParamUpdates, err := convertConsensusParams(res.ConsensusParamUpdates)
	if err != nil {
		return nil, fmt.Errorf("converting consensus param updates: %w", err)
	}

	return &pbcosmos.Block{
		Hash:                  req.Hash,
		Height:                req.Height,
		Time:                  timestamppb.New(req.Time.UTC()),
		Header:                convertHeader(chainID, versionApp, req),
		Misbehavior:           misbehaviors,
		Events:                events,
		Txs:                   req.Txs,
		TxResults:             txResults,
		ValidatorUpdates:      validatorUpdates,
		ConsensusParamUpdates: consensusParamUpdates,
	}, nil
}

func convertHeader(chainID string, versionApp uint64, req abci.FinalizeBlockRequest) *pbcosmos.Header {
	return &pbcosmos.Header{
		Version: &pbcosmos.Consensus{
			Block: version.BlockProtocol,
			App:   versionApp,
		},
		ChainId: sanitizeUTF8(chainID),
		Height:  req.Height,
		Time:    timestamppb.New(req.Time.UTC()),
		LastBlockId: &pbcosmos.BlockID{
			Hash: req.LastBlockHash,
			PartSetHeader: &pbcosmos.PartSetHeader{
				Total: uint32(req.LastBlockPartSetTotal),
				Hash:  req.LastBlockPartSetHash,
			},
		},
		LastCommitHash:     req.LastCommitHash,
		DataHash:           req.DataHash,
		ValidatorsHash:     req.ValidatorsHash,
		NextValidatorsHash: req.NextValidatorsHash,
		ConsensusHash:      req.ConsensusHash,
		AppHash:            req.AppHash,
		LastResultsHash:    req.LastResultsHash,
		EvidenceHash:       req.EvidenceHash,
		ProposerAddress:    req.ProposerAddress,
	}
}

func convertEvents(src []abci.Event) ([]*pbcosmos.Event, error) {
	if len(src) == 0 {
		return nil, nil
	}
	events := make([]*pbcosmos.Event, len(src))
	for i, e := range src {
		attrs := make([]*pbcosmos.EventAttribute, len(e.Attributes))
		for j, a := range e.Attributes {
			attrs[j] = &pbcosmos.EventAttribute{
				Key:   sanitizeUTF8(a.Key),
				Value: sanitizeUTF8(a.Value),
			}
		}
		events[i] = &pbcosmos.Event{
			Type:       sanitizeUTF8(e.Type),
			Attributes: attrs,
		}
	}
	return events, nil
}

func convertMisbehaviors(src []abci.Misbehavior) ([]*pbcosmos.Misbehavior, error) {
	if len(src) == 0 {
		return nil, nil
	}
	out := make([]*pbcosmos.Misbehavior, len(src))
	for i := range out {
		out[i] = &pbcosmos.Misbehavior{}
	}
	if err := arrayProtoFlip(arrayToPointerArray(src), out); err != nil {
		return nil, err
	}
	return out, nil
}

func convertTxResults(txs []*abci.ExecTxResult) ([]*pbcosmos.TxResults, error) {
	if len(txs) == 0 {
		return nil, nil
	}
	out := make([]*pbcosmos.TxResults, len(txs))
	for i, tx := range txs {
		converted, err := convertTxResult(tx)
		if err != nil {
			return nil, fmt.Errorf("tx result %d: %w", i, err)
		}
		out[i] = converted
	}
	return out, nil
}

func convertTxResult(tx *abci.ExecTxResult) (*pbcosmos.TxResults, error) {
	if tx == nil {
		return &pbcosmos.TxResults{}, nil
	}
	events, err := convertEvents(tx.Events)
	if err != nil {
		return nil, fmt.Errorf("converting events: %w", err)
	}
	return &pbcosmos.TxResults{
		Code:      tx.Code,
		Data:      tx.Data,
		Log:       sanitizeUTF8(tx.Log),
		Info:      sanitizeUTF8(tx.Info),
		GasWanted: tx.GasWanted,
		GasUsed:   tx.GasUsed,
		Events:    events,
		Codespace: sanitizeUTF8(tx.Codespace),
	}, nil
}

func convertValidatorUpdates(src []abci.ValidatorUpdate) ([]*pbcosmos.ValidatorUpdate, error) {
	if len(src) == 0 {
		return nil, nil
	}
	out := make([]*pbcosmos.ValidatorUpdate, len(src))
	for i, u := range src {
		pk, err := convertPublicKey(u.PubKeyType, u.PubKeyBytes)
		if err != nil {
			return nil, fmt.Errorf("validator update %d: %w", i, err)
		}
		out[i] = &pbcosmos.ValidatorUpdate{
			PubKey: pk,
			Power:  u.Power,
		}
	}
	return out, nil
}

func convertPublicKey(keyType string, keyBytes []byte) (*pbcosmos.PublicKey, error) {
	if keyType == "" && len(keyBytes) == 0 {
		return nil, nil
	}
	switch keyType {
	case ed25519.KeyType, "tendermint/PubKeyEd25519":
		return &pbcosmos.PublicKey{Sum: &pbcosmos.PublicKey_Ed25519{Ed25519: keyBytes}}, nil
	case secp256k1.KeyType, "tendermint/PubKeySecp256k1":
		return &pbcosmos.PublicKey{Sum: &pbcosmos.PublicKey_Secp256K1{Secp256K1: keyBytes}}, nil
	case "bls12_381", "cometbft/PubKeyBls12_381", "tendermint/PubKeyBls12_381":
		return &pbcosmos.PublicKey{Sum: &pbcosmos.PublicKey_Bls12381{Bls12381: keyBytes}}, nil
	default:
		return nil, fmt.Errorf("unsupported validator public key type %q", keyType)
	}
}

func convertConsensusParams(src *cmtproto.ConsensusParams) (*pbcosmos.ConsensusParams, error) {
	if src == nil {
		return nil, nil
	}
	out := &pbcosmos.ConsensusParams{}
	if err := protoFlip(src, out); err != nil {
		return nil, err
	}
	return out, nil
}

func sanitizeUTF8(s string) string {
	return strings.Map(fixUtf, s)
}

func fixUtf(r rune) rune {
	if r == utf8.RuneError {
		return '�'
	}
	return r
}
