package v03807

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"

	"github.com/streamingfast/bstream"
	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"
	"github.com/streamingfast/dstore"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/anypb"
)

type SimpleMerge struct {
	logger      *zap.Logger
	blockLoader *BlockLoader
}

func NewSimpleMerger(blockLoader *BlockLoader, logger *zap.Logger) *SimpleMerge {
	return &SimpleMerge{
		blockLoader: blockLoader,
		logger:      logger,
	}
}

func (m *SimpleMerge) GenerateMergeBlock(startBlock int64, endBlock int64, destStore dstore.Store) error {
	first, last := m.blockLoader.BlockRange()

	m.logger.Info("generation merge blocks", zap.Int64("start_block", startBlock), zap.Int64("end_block", endBlock), zap.Int64("first", first), zap.Int64("last", last))

	if startBlock < first {
		return fmt.Errorf("start block %d is before the first block %d of current snapshot", startBlock, first)
	}

	if endBlock > last {
		return fmt.Errorf("end block %d is after the last block %d of current snapshot", endBlock, last)
	}

	if endBlock%100 != 0 {
		return fmt.Errorf("end block %d is not a boundary", endBlock)
	}

	height := startBlock
	for {

		if height >= endBlock {
			break
		}

		file := filename(height)

		err := m.ToMergeBlockFile(height, file, destStore)
		if err != nil {
			return fmt.Errorf("generating merge blocks file: %w", err)
		}

		height += 100
	}

	return nil
}

func (m *SimpleMerge) GenerateOneBlock(startBlock int64, endBlock int64, destStore dstore.Store) error {
	first, last := m.blockLoader.BlockRange()
	m.logger.Info("generating one blocks", zap.Int64("start_block", startBlock), zap.Int64("end_block", endBlock), zap.Int64("first", first), zap.Int64("last", last))

	if startBlock < first {
		return fmt.Errorf("start block %d is before the first block %d of current snapshot", startBlock, first)
	}

	if endBlock > last {
		return fmt.Errorf("end block %d is after the last block %d of current snapshot", endBlock, last)
	}

	height := startBlock
	for {
		if height > endBlock {
			m.logger.Info("end block reached", zap.Int64("height", height), zap.Int64("end_block", endBlock))
			break
		}

		file := filename(height)

		err := m.ToOneBlockFile(height, file, destStore)
		if err != nil {
			return fmt.Errorf("generating one blocks file: %w", err)
		}

		height++
	}

	return nil
}

func (m *SimpleMerge) ToMergeBlockFile(startBlock int64, filename string, destStore dstore.Store) error {
	m.logger.Info("generating merge blocks file", zap.Int64("start_block", startBlock), zap.String("filename", filename))

	pr, pw := io.Pipe()
	blockWriter, err := bstream.NewDBinBlockWriter(pw)
	if err != nil {
		return fmt.Errorf("creating block writer: %w", err)
	}

	go func() {
		var processErr error
		defer func() {
			err := pw.CloseWithError(processErr)
			if err != nil {
				panic(err)
			}
			m.logger.Info("closing pipe", zap.Error(processErr))
		}()

		//round down to the nearest 100
		endBlock := (startBlock - (startBlock % 100)) + 99

		for height := startBlock; height <= endBlock; height++ {
			m.logger.Debug("loading block", zap.Int64("height", height))
			block, err := m.blockLoader.loadBlock(height)
			if err != nil {
				processErr = fmt.Errorf("loading block %d: %w", height, err)
				return
			}

			payload, err := anypb.New(block)
			if err != nil {
				processErr = fmt.Errorf("creating payload for block %d: %w", height, err)
				return
			}
			bstreamBlock := &pbbstream.Block{
				Number:    uint64(block.Height),
				Id:        hex.EncodeToString(block.Hash),
				ParentId:  hex.EncodeToString(block.Header.LastBlockId.Hash),
				Timestamp: block.Time,
				LibNum:    uint64(block.Height - 1),
				ParentNum: uint64(block.Height - 1),
				Payload:   payload,
			}

			err = blockWriter.Write(bstreamBlock)
			if err != nil {
				processErr = fmt.Errorf("writing block %d: %w", height, err)
				return
			}
		}
		m.logger.Info("finished writing blocks to pipe")
	}()

	m.logger.Info("writing merged file to store", zap.String("filename", filename))
	err = destStore.WriteObject(context.Background(), filename, pr)
	if err != nil {
		return fmt.Errorf("writing to store: %w", err)
	}

	m.logger.Info("finished writing merged file to store", zap.String("filename", filename))
	return nil
}

func (m *SimpleMerge) ToOneBlockFile(blockNum int64, filename string, destStore dstore.Store) error {
	m.logger.Info("generating one blocks file", zap.Int64("block_num", blockNum), zap.String("filename", filename))

	pr, pw := io.Pipe()
	blockWriter, err := bstream.NewDBinBlockWriter(pw)
	if err != nil {
		return fmt.Errorf("creating block writer: %w", err)
	}

	go func() {
		var processErr error
		defer func() {
			err := pw.CloseWithError(processErr)
			if err != nil {
				panic(err)
			}
			m.logger.Info("closing pipe", zap.Error(processErr))
		}()

		m.logger.Debug("loading block", zap.Int64("block_num", blockNum))
		block, err := m.blockLoader.loadBlock(blockNum)
		if err != nil {
			processErr = fmt.Errorf("loading block %d: %w", blockNum, err)
			return
		}

		payload, err := anypb.New(block)
		if err != nil {
			processErr = fmt.Errorf("creating payload for block %d: %w", blockNum, err)
			return
		}
		bstreamBlock := &pbbstream.Block{
			Number:    uint64(block.Height),
			Id:        hex.EncodeToString(block.Hash),
			ParentId:  hex.EncodeToString(block.Header.LastBlockId.Hash),
			Timestamp: block.Time,
			LibNum:    uint64(block.Height - 1),
			ParentNum: uint64(block.Height - 1),
			Payload:   payload,
		}

		err = blockWriter.Write(bstreamBlock)
		if err != nil {
			processErr = fmt.Errorf("writing block %d: %w", blockNum, err)
			return
		}

		m.logger.Info("finished writing blocks to pipe")
	}()

	m.logger.Info("writing merged file to store", zap.String("filename", filename))
	err = destStore.WriteObject(context.Background(), filename, pr)
	if err != nil {
		return fmt.Errorf("writing to store: %w", err)
	}

	m.logger.Info("finished writing merged file to store", zap.String("filename", filename))
	return nil
}

func filename(num int64) string {
	return fmt.Sprintf("%010d", num)
}
