package utils

import (
	"fmt"
	"reflect"
	"unicode/utf8"

	"google.golang.org/protobuf/proto"
)

var UnmarshallerDiscardUnknown = &proto.UnmarshalOptions{
	DiscardUnknown: true,
}

func ArrayToPointerArray[T any](ts []T) []*T {
	res := make([]*T, len(ts))
	for i, t := range ts {
		res[i] = &t
	}
	return res
}

func ArrayProtoFlip[U cosmoProto.Message, V proto.Message](origins []U, targets []V) error {
	if len(origins) != len(targets) {
		return fmt.Errorf("origin and target arrays have different lengths: %d != %d", len(origins), len(targets))
	}
	if len(origins) == 0 {
		return nil
	}

	for i := range origins {
		err := ProtoFlip(origins[i], targets[i])
		if err != nil {
			return fmt.Errorf("converting element %d: %w", i, err)
		}
	}

	return nil
}

func ProtoFlip(origin cosmoProto.Message, target proto.Message) error {
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

func FixUtf(r rune) rune {
	if r == utf8.RuneError {
		fmt.Println("found rune error")
		return '�'
	}
	return r
}
