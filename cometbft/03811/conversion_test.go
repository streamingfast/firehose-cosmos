package v03811

import (
	"testing"

	"github.com/cometbft/cometbft/proto/tendermint/types"
	pbcosmos "github.com/streamingfast/firehose-cosmos/cosmos/pb/sf/cosmos/type/v2"
	"github.com/stretchr/testify/require"
)

func TestProtoFlipKeepsABCIParams(t *testing.T) {
	in := &types.ConsensusParams{
		Version: &types.VersionParams{App: 7},
		Abci:    &types.ABCIParams{VoteExtensionsEnableHeight: 12345},
	}
	out := &pbcosmos.ConsensusParams{}
	require.NoError(t, protoFlip(in, out))
	require.Equal(t, uint64(7), out.GetVersion().GetApp())
	require.Equal(t, int64(12345), out.GetAbci().GetVoteExtensionsEnableHeight())
	require.Nil(t, out.GetSynchrony())
	require.Nil(t, out.GetFeature())
}
