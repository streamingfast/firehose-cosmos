package injective

import (
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"
	"unicode/utf8"

	abci "github.com/cometbft/cometbft/abci/types"
	cometType "github.com/cometbft/cometbft/types"
	cosmoProto "github.com/cosmos/gogoproto/proto"
	pbcosmos "github.com/streamingfast/firehose-cosmos/cosmos/pb/sf/cosmos/type/v2"
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
		if e, ok := origin.(*abci.ResponseDeliverTx); ok {
			fmt.Println("event data:", hex.EncodeToString(data))
			fmt.Println("log:", e.Log)
			fmt.Println("info:", e.Info)
		}
		return fmt.Errorf("unmashalling target object %T: %w", data, err)
	}

	return nil
}

func convertResponseDeliverTx(tx *abci.ResponseDeliverTx) (*pbcosmos.TxResults, error) {
	events := make([]*pbcosmos.Event, len(tx.Events))
	for i, _ := range events {
		events[i] = &pbcosmos.Event{}
	}
	err := arrayProtoFlip(arrayToPointerArray(tx.Events), events)
	if err != nil {
		return nil, fmt.Errorf("converting events: %w", err)
	}

	fixedLog := strings.Map(fixUtf, tx.Log)

	txResults := &pbcosmos.TxResults{
		Code:      tx.Code,
		Data:      tx.Data,
		Log:       fixedLog,
		Info:      tx.Info,
		GasWanted: tx.GasWanted,
		GasUsed:   tx.GasUsed,
		Events:    events,
		Codespace: tx.Codespace,
	}

	return txResults, nil
}

func convertDeliverTxs(txs []*abci.ResponseDeliverTx) ([]*pbcosmos.TxResults, error) {
	txResults := make([]*pbcosmos.TxResults, len(txs))
	for i, tx := range txs {
		txResults[i], _ = convertResponseDeliverTx(tx)
	}
	return txResults, nil
}

func MisbehaviorsFromEvidences(evidences cometType.EvidenceList) ([]*pbcosmos.Misbehavior, error) {
	var misbehaviors []*pbcosmos.Misbehavior
	for _, e := range evidences {

		abciMisbehavior := e.ABCI()

		partials := make([]*pbcosmos.Misbehavior, len(abciMisbehavior))
		for i, _ := range partials {
			partials[i] = &pbcosmos.Misbehavior{}
		}

		err := arrayProtoFlip(arrayToPointerArray(abciMisbehavior), partials)
		if err != nil {
			return nil, fmt.Errorf("converting abci misbehavior: %w", err)
		}

		misbehaviors = append(misbehaviors, partials...)
	}
	return misbehaviors, nil
}

func fixUtf(r rune) rune {
	if r == utf8.RuneError {
		fmt.Println("found rune error")
		return '�'
	}
	return r
}
