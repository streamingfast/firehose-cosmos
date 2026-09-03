module github.com/streamingfast/firehose-cosmos/cometbft/101/convert

go 1.26.2

// FinalizeBlockRequest header hashes (LastBlockHash, AppHash, …) exist only on
// the firehose-patched fork. Stock v1.0.1 does not carry them.
replace github.com/cometbft/cometbft => github.com/streamingfast/cometbft v1.0.1-inj.v1.19.0-rollback-firehose

replace github.com/cometbft/cometbft/api => github.com/streamingfast/cometbft/api v1.0.1-inj.v1.19.0-rollback-firehose

require (
	buf.build/gen/go/streamingfast/firehose-cosmos/protocolbuffers/go v1.36.12-20260901132337-3c05174eb2a8.1
	github.com/cometbft/cometbft v1.0.1
	github.com/cometbft/cometbft/api v1.0.0
	github.com/cosmos/gogoproto v1.7.2
	github.com/stretchr/testify v1.12.1
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.4.0 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/google/go-cmp v0.7.0 // indirect
	github.com/oasisprotocol/curve25519-voi v0.0.0-20220708102147-0a8a51822cae // indirect
	github.com/petermattis/goid v0.0.0-20240813172612-4fcff4a6cae7 // indirect
	github.com/sasha-s/go-deadlock v0.3.5 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/crypto v0.41.0 // indirect
	golang.org/x/net v0.43.0 // indirect
	golang.org/x/sys v0.35.0 // indirect
	golang.org/x/text v0.28.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250825161204-c5933d9347a5 // indirect
	google.golang.org/grpc v1.75.0 // indirect
)
