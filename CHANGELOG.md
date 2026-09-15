# Changelog

## Unreleased

### Fixed

- Place EndBlock `block_bloom` immediately before `EventSetBalances` so
  CometBFT v1 RPC blocks match production firehose-cosmos event order.
