# What?

Asyncio Quant Tick aggregates high frequency tick data from WebSockets.

# How?

Sequences of trades that have equal symbol, timestamp, and tick rule are aggregated. Aggregating trades in this way can increase information, as they are either orders of size or stop loss cascades.

As well, the number of messages can be reduced by 30-50%

By filtering aggregated messages, for example only emitting a message when an aggregated trade is greater than or equal to a `significant_trade_filter`, the number of messages can be reduced more.

Messages can optionally be published to GCP Pub/Sub.

Example
-------
The following are two sequential aggregated trades by timestamp, nanoseconds, and tick rule.

As it was aggregated from 4 raw trades, the second trade has ticks 4.

```python
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

An example filtered message, emitted because the second aggregated trade exceeds `significant_trade_filter >= 1000`

Information related to the first trade is aggregated with the second.

```python
[
    {
        "timestamp": 1620000915.885381,
        "price": "57071.2",
        "volume": "9376.6869202914",
        "notional": "0.16429813",
        "tickRule": 1,
        "ticks": 4,
        "high": '57071.2',
        "low": '57064.01',
        "totalBuyVolume": "9376.6869202914",
        "totalVolume": "9943.3348221518",
        "totalBuyNotional": "0.16429813",
        "totalNotional": "0.17422817",
        "totalBuyTicks": 4,
        "totalTicks": 5
    }
]
```

For settings, see the [examples](https://github.com/globophobe/asyncio-quant-tick/blob/main/examples/)

Supported exchanges
-------------------

:white_check_mark: Hyperliquid

:white_check_mark: Binance

:white_check_mark: Bitfinex

:white_check_mark: BitMEX

:white_check_mark: Coinbase Pro

Contributing
------------

Install dependencies with `uv sync`. The docker example is built with [invoke tasks](https://github.com/globophobe/asyncio-quant-tick/blob/master/tasks.py). For example, `invoke build-container`
