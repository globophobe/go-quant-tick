# What?

Go Quant Tick aggregates high frequency tick data from WebSockets.

# How?

Sequences of trades that have equal symbol, timestamp, and tick rule are aggregated. Aggregating trades in this way can increase information, as they are either orders of size or stop loss cascades.

As well, the number of messages can be reduced by 30-50%

By filtering aggregated messages, for example only emitting a message when an aggregated trade is greater than or equal to a `SIGNIFICANT_TRADE_FILTER`, the number of messages can be reduced more.

Example
-------
The following are two sequential aggregated trades by timestamp, nanoseconds, and tick rule.

As it was aggregated from 4 raw trades, the second trade has ticks 4.

```json
[
    {
        "timestamp": 1620000915.31424,
        "price": "57064.01",
        "volume": "566.6479018604",
        "notional": "0.00993004",
        "tickRule": -1,
        "ticks": 1
    },
    {
        "timestamp": 1620000915.885381,
        "price": "57071.2",
        "volume": "9376.6869202914",
        "notional": "0.16429813",
        "tickRule": 1,
        "ticks": 4
    }
]
```

An example filtered message, emitted because `SIGNIFICANT_TRADE_FILTER` is `1000`.

Information related to the first trade is aggregated with the second.

```json
[
    {
        "timestamp": 1620000915.885381,
        "price": "57071.2",
        "volume": "9376.6869202914",
        "notional": "0.16429813",
        "tickRule": 1,
        "ticks": 4,
        "high": "57071.2",
        "low": "57064.01",
        "totalBuyVolume": "9376.6869202914",
        "totalVolume": "9943.3348221518",
        "totalBuyNotional": "0.16429813",
        "totalNotional": "0.17422817",
        "totalBuyTicks": 4,
        "totalTicks": 5
    }
]
```

If no significant trade occurs in a minute window, a context tick is emitted with `volume`, `notional`, `tickRule`, and `ticks` omitted.

Settings
--------

For local JSON-lines output:

```shell
go run ./cmd/quanttick -publisher=stdout
```

Environment variables:

```shell
BINANCE_SYMBOLS=BTCUSDT=10000
BINANCE_FUTURES_SYMBOLS=BTCUSDT
BYBIT_SPOT_SYMBOLS=BTCUSDT
BYBIT_LINEAR_SYMBOLS=BTCUSDT
BYBIT_INVERSE_SYMBOLS=BTCUSD
BITFINEX_SYMBOLS=tBTCF0:USTF0
BITMEX_SYMBOLS=XBTUSD
COINBASE_SYMBOLS=BTC-USD
DERIBIT_SYMBOLS=BTC-PERPETUAL
HYPERLIQUID_SYMBOLS=BTC
WEBSOCKET_DATA_STREAMS=significant-trades
SIGNIFICANT_TRADE_FILTER=1000
```

Symbol lists are comma-separated. A symbol can include an optional significant trade threshold as `SYMBOL=THRESHOLD`; symbols without an override use `SIGNIFICANT_TRADE_FILTER`.

`WEBSOCKET_DATA_STREAMS` accepts:

```text
raw-trades,aggregated-trades,significant-trades
```


Example scripts
---------------

Each example prints significant-trade JSON lines until interrupted:

```shell
go run ./examples/binance
go run ./examples/binance-futures
go run ./examples/bybit
go run ./examples/coinbase
go run ./examples/deribit
go run ./examples/bitfinex
go run ./examples/bitmex
go run ./examples/hyperliquid
```

Example with a `BTCUSDT` threshold of `10000`:

```shell
BINANCE_SYMBOLS=BTCUSDT=10000 go run ./examples/binance
```

Supported exchanges
-------------------

✅ Binance

✅ Binance Futures

✅ Bitfinex

✅ BitMEX

✅ Bybit

✅ Coinbase

✅ Deribit

✅ Hyperliquid

Tests
-----

Run tests with:

```shell
go test ./...
go test -race ./...
```
