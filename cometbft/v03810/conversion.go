package v03810

import (
	"fmt"
	"reflect"

	cosmoProto "github.com/cosmos/gogoproto/proto"
	"google.golang.org/protobuf/proto"
)

var UnmarshallerDiscardUnknown = &proto.UnmarshalOptions{
	DiscardUnknown: true,
}

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

	err = UnmarshallerDiscardUnknown.Unmarshal(data, target)
	if err != nil {
		return fmt.Errorf("unmashalling target object %T: %w", data, err)
	}

	return nil
}
