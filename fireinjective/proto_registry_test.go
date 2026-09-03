package main

import (
	"testing"

	"google.golang.org/protobuf/reflect/protoregistry"
)

// fireinjective must register sf/cosmos/type/v2/block.proto exactly once.
// Linking both cosmos/pb and the buf.build gen panics in init().
func TestBlockProtoIsRegisteredOnce(t *testing.T) {
	if _, err := protoregistry.GlobalFiles.FindFileByPath("sf/cosmos/type/v2/block.proto"); err != nil {
		t.Fatalf("sf/cosmos/type/v2/block.proto is not registered: %v", err)
	}
}
