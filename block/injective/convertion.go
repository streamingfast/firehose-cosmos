package injective

import (
	"fmt"
	"reflect"

	abci "github.com/cometbft/cometbft/abci/types"
	cometType "github.com/cometbft/cometbft/types"
	cosmoProto "github.com/cosmos/gogoproto/proto"
	pbinj "github.com/streamingfast/firehose-cosmos/pb/sf/injective/type/v1"
	"google.golang.org/protobuf/proto"
)

func arrayToPointerArray[T any](ts []T) []*T {
	res := make([]*T, len(ts))
	for i, t := range ts {
		res[i] = &t
	}
	return res
}

func arrayProtoFlip[U cosmoProto.Message, V proto.Message](origins []U, targets []V) error {
	if len(origins) != len(targets) {
		return fmt.Errorf("origin and target arrays have different lengths: %d != %d", len(origins), len(targets))
	}
	if len(origins) == 0 {
		return nil
	}

	for i := range origins {
		err := protoFlip(origins[i], targets[i])
		if err != nil {
			return fmt.Errorf("converting element %d: %w", i, err)
		}
	}

	return nil
}

func protoFlip(origin cosmoProto.Message, target proto.Message) error {
	if origin == nil || reflect.ValueOf(origin).IsNil() {
		return nil
	}
	//marshall origin the unmarshall to target
	data, err := cosmoProto.Marshal(origin)
	if err != nil {
		return fmt.Errorf("mashalling origin object %T: %w", data, err)
	}

	err = proto.Unmarshal(data, target)
	if err != nil {
		if e, ok := origin.(*abci.Event); ok {

			fmt.Println("event data:", data)
			fmt.Println("type bytes:", []byte(e.Type))
			fmt.Printf("origin event type %q\n", e.Type)
			for _, attr := range e.Attributes {
				fmt.Printf("key: %q, value: %q %t	\n", attr.Key, attr.Value, attr.Index)
			}

			fmt.Println("data:", string(data))
		}
		return fmt.Errorf("unmashalling target object %T: %w", data, err)
	}

	return nil
}

func MisbehaviorsFromEvidences(evidences cometType.EvidenceList) ([]*pbinj.Misbehavior, error) {
	var misbehaviors []*pbinj.Misbehavior
	for _, e := range evidences {

		abciMisbehavior := e.ABCI()

		partials := make([]*pbinj.Misbehavior, len(abciMisbehavior))
		for i, _ := range partials {
			partials[i] = &pbinj.Misbehavior{}
		}

		err := arrayProtoFlip(arrayToPointerArray(abciMisbehavior), partials)
		if err != nil {
			return nil, fmt.Errorf("converting abci misbehavior: %w", err)
		}

		misbehaviors = append(misbehaviors, partials...)
	}
	return misbehaviors, nil
}
