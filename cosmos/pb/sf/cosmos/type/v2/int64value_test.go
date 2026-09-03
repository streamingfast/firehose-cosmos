package pbcomos

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestInt64ValueWireCompatibleWithGoogle(t *testing.T) {
	src := &wrapperspb.Int64Value{Value: 100}
	data, err := proto.Marshal(src)
	require.NoError(t, err)

	dst := &Int64Value{}
	require.NoError(t, proto.Unmarshal(data, dst))
	require.Equal(t, int64(100), dst.Value)

	back, err := proto.Marshal(dst)
	require.NoError(t, err)
	got := &wrapperspb.Int64Value{}
	require.NoError(t, proto.Unmarshal(back, got))
	require.Equal(t, int64(100), got.Value)
}

func TestFeatureParamsUnsetDistinctFromZero(t *testing.T) {
	unset, err := proto.Marshal(&FeatureParams{})
	require.NoError(t, err)
	require.Empty(t, unset)

	zero, err := proto.Marshal(&FeatureParams{VoteExtensionsEnableHeight: &Int64Value{Value: 0}})
	require.NoError(t, err)
	require.NotEmpty(t, zero)
}
