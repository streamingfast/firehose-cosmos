package main

import (
	"fmt"
	"log"
	"os"

	dbm "github.com/cometbft/cometbft-db"
	abci "github.com/cometbft/cometbft/abci/types"
	"github.com/cometbft/cometbft/libs/json"
	"github.com/cometbft/cometbft/state"
	txindexkv "github.com/cometbft/cometbft/state/txindex/kv"
	"github.com/cometbft/cometbft/store"
	"github.com/cosmos/cosmos-sdk/store/streaming/firehose"
)

func main() {
	if err := Main(); err != nil {
		log.Fatalln("err", err)
	}
}

func Main() error {
	homeDir := "/Users/abourget/.injectived/data/"

	dbType := dbm.BackendType("goleveldb")
	blockDB, err := dbm.NewDB("blockstore", dbType, homeDir)
	if err != nil {
		return err
	}
	blockStore := store.NewBlockStore(blockDB)

	stateDB, err := dbm.NewDB("state", dbType, homeDir)
	if err != nil {
		return err
	}
	stateStore := state.NewStore(stateDB, state.StoreOptions{
		DiscardABCIResponses: false,
	})

	txIndexDB, err := dbm.NewDB("tx_index", dbType, homeDir)
	if err != nil {
		return err
	}
	txIndexStore := txindexkv.NewTxIndex(txIndexDB)

	first := blockStore.Base()
	last := blockStore.Height()
	fmt.Println(first, last)

	for height := first; height <= last-1; height++ {
		block := blockStore.LoadBlock(height)
		fmt.Println("Writing", height, block.Header.Height)

		writeProto(block, fmt.Sprintf("%d_block.json", height))
		blockMeta := blockStore.LoadBlockMeta(height)
		writeProto(blockMeta, fmt.Sprintf("%d_blockmeta.json", height))

		blockResponse, err := stateStore.LoadFinalizeBlockResponse(height)
		if err != nil {
			return err
		}
		var txResults []*abci.ExecTxResult
		for _, tx := range block.Txs {
			hash := tx.Hash()
			res, err := txIndexStore.Get(hash)
			if err != nil {
				return err
			}
			txResults = append(txResults, &res.Result)
		}
		blockResponse.TxResults = txResults

		asBytes, err := blockResponse.Marshal()
		if err != nil {
			return fmt.Errorf("marshal block response: %w", err)
		}
		newFormatResponseBlock := &firehose.ResponseFinalizeBlock{}
		err = newFormatResponseBlock.Unmarshal(asBytes)
		if err != nil {
			return fmt.Errorf("marshal block response: %w", err)
		}

		//writeProto(blockResponse, fmt.Sprintf("%d_finalized_block.json", height))

		bstreamBlock := &firehose.Block{
			Header: blockMeta.Header.ToProto(),
			Req:    &firehose.RequestFinalizeBlock{},
			Res:    newFormatResponseBlock,
		}

		writeProto(bstreamBlock, fmt.Sprintf("%b_stream.json", height))
	}

	return nil

	// Other database

	// evidenceDB, err := dbm.NewDB("evidence", dbType, homeDir)
	// if err != nil {
	// 	panic(err)
	// }
	// evidencePool, err := evidence.NewPool(evidenceDB, stateDB, blockStore)
	// if err != nil {
	// 	panic(err)
	// }

	//stateBlock, err := evidencePool.State().
	//if err != nil {
	//	panic(err)
	//}
	//j, err = json.MarshalIndent(stateBlock, "", "  ")
	//if err != nil {
	//	panic(err)
	//}
	//
	//err = write(j, "block_state.json")
	//if err != nil {
	//	panic(err)
	//}
}

func writeProto(msg any, filename string) {
	j, err := json.MarshalIndent(msg, "", "  ")
	if err != nil {
		panic(err)
	}

	err = write(j, filename)
	if err != nil {
		panic(err)
	}
}

func write(data []byte, filename string) error {
	//wire json to file
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	_, err = file.Write(data)
	if err != nil {
		return err
	}
	file.Close()
	return nil
}
