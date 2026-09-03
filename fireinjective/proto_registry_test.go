package main

import (
	"testing"

	"google.golang.org/protobuf/reflect/protoregistry"
)

// A binary may hold only one generated package per proto file. This one links
// two for sf/cosmos/type/v2/block.proto: the in-repo cosmos/pb, used by the
// tools commands, and the buf.build gen reached through cometbft/101. The
// protobuf runtime panics in init() over the collision, so every fireinjective
// invocation dies before reaching a command — and this test binary panics
// before running the test below. Fixing it means picking one of the two
// packages for the whole binary.
func TestBlockProtoIsRegisteredOnce(t *testing.T) {
	if _, err := protoregistry.GlobalFiles.FindFileByPath("sf/cosmos/type/v2/block.proto"); err != nil {
		t.Fatalf("sf/cosmos/type/v2/block.proto is not registered: %v", err)
	}
}
