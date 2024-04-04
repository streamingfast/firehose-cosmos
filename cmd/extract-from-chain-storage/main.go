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
	pb "github.com/streamingfast/firehose-cosmos/pb/github.com/streamingfast/firehose-cosmos/pb"
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

	for height := first; height <= last-1 && height <= first+5; height++ {
		var nilCount int
		block := blockStore.LoadBlock(height)

		//writeProto(block, fmt.Sprintf("%d_block.json", height))
		blockMeta := blockStore.LoadBlockMeta(height)
		//writeProto(blockMeta, fmt.Sprintf("%d_blockmeta.json", height))

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
			if res == nil {
				nilCount += 1
				txResults = append(txResults, nil)
				/// TODO: we should fail here, it's because the snapshot doesn't contain the transaction results
				// of the block at which it is supposed to have the state.
			} else {
				txResults = append(txResults, &res.Result)
			}
		}
		blockResponse.TxResults = txResults

		/*
			NEW DATA MODEL:
				req handles:
				   type RequestFinalizeBlock struct {
				   	Txs               [][]byte      `protobuf:"bytes,1,rep,name=txs,proto3" json:"txs,omitempty"`
				   	DecidedLastCommit CommitInfo    `protobuf:"bytes,2,opt,name=decided_last_commit,json=decidedLastCommit,proto3" json:"decided_last_commit"`
				   	Misbehavior       []Misbehavior `protobuf:"bytes,3,rep,name=misbehavior,proto3" json:"misbehavior"`
				   	// hash is the merkle root hash of the fields of the decided block.
				   	Hash               []byte    `protobuf:"bytes,4,opt,name=hash,proto3" json:"hash,omitempty"`
				   	Height             int64     `protobuf:"varint,5,opt,name=height,proto3" json:"height,omitempty"`
				   	Time               time.Time `protobuf:"bytes,6,opt,name=time,proto3,stdtime" json:"time"`
				   	NextValidatorsHash []byte    `protobuf:"bytes,7,opt,name=next_validators_hash,json=nextValidatorsHash,proto3" json:"next_validators_hash,omitempty"`
				   	// proposer_address is the address of the public key of the original proposer of the block.
				   	ProposerAddress []byte `protobuf:"bytes,8,opt,name=proposer_address,json=proposerAddress,proto3" json:"proposer_address,omitempty"`
				   }

				    type CommitInfo struct {
						Round int32      `protobuf:"varint,1,opt,name=round,proto3" json:"round,omitempty"`
						Votes []VoteInfo `protobuf:"bytes,2,rep,name=votes,proto3" json:"votes"`
					}
					type VoteInfo struct {
						Validator   Validator          `protobuf:"bytes,1,opt,name=validator,proto3" json:"validator"`
						BlockIdFlag types1.BlockIDFlag `protobuf:"varint,3,opt,name=block_id_flag,json=blockIdFlag,proto3,enum=tendermint.types.BlockIDFlag" json:"block_id_flag,omitempty"`
					}
					type Validator struct {
						Address []byte `protobuf:"bytes,1,opt,name=address,proto3" json:"address,omitempty"`
						// PubKey pub_key = 2 [(gogoproto.nullable)=false];
						Power int64 `protobuf:"varint,3,opt,name=power,proto3" json:"power,omitempty"`
					}

			// ON DISK and OLDER DATA MODEL:
				BlockMeta holds:
				   	BlockID   BlockID `json:"block_id"`
				   	BlockSize int     `json:"block_size"`
				   	Header    Header  `json:"header"`
				   	NumTxs    int     `json:"num_txs"`
				Block holds:
				   	Header     `json:"header"`
					Data       `json:"data"`
					    which contains:
				   			Txs Txs `json:"txs"`
					Evidence   EvidenceData `json:"evidence"`
					LastCommit *Commit      `json:"last_commit"`
				Commit holds:
					Height     int64       `json:"height"`
					Round      int32       `json:"round"`
					BlockID    BlockID     `json:"block_id"`
					Signatures []CommitSig `json:"signatures"`
				message CommitSig {
					BlockIDFlag block_id_flag = 1;
					bytes validator_address = 2;
					Timestamp timestamp = 3 [(gogoproto.nullable) = false, (gogoproto.stdtime) = true];
					bytes signature = 4;
				}
				EvidenceData contains, deep inside a bunch of `abci.Misbehavior`

		*/

		req := &abci.RequestFinalizeBlock{
			//DecidedLastCommit:  blockMeta.LastCommit,
			Misbehavior:        block.Evidence.Evidence.ToABCI(),
			Hash:               blockMeta.BlockID.Hash,
			Height:             block.Header.Height,
			Time:               block.Header.Time,
			NextValidatorsHash: block.Header.NextValidatorsHash,
			ProposerAddress:    block.Header.ProposerAddress,
		}
		for _, tx := range block.Data.Txs {
			req.Txs = append(req.Txs, tx)
		}

		// Patrycia used it
		//block.LastCommit

		bstreamBlock := &pb.Block{
			// Hash:   blockMeta.BlockID.Hash,
			// Txs:    block.Data.Txs,
			Header: blockMeta.Header.ToProto(),
			// TODO: HEY ADD Req!!!
			Req: req,
			Res: blockResponse,
		}

		fmt.Println("Writing", height, nilCount)

		writeProto(bstreamBlock, fmt.Sprintf("%d_stream.json", height))
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
