#!/usr/bin/env python3
import asyncio
import os
from collections.abc import Awaitable, Callable, Iterable

from dotenv import load_dotenv

from quant_tick.events import TradeEvent
from quant_tick.exchanges import Binance, Bitfinex, Bitmex, Coinbase
from quant_tick.pipeline import run_clients
from quant_tick.pubsub import PubSubPublisher
from quant_tick.trades import (
    SignificantTradeCallback,
    TradeCallback,
)

RAW_TRADES = "raw-trades"
AGGREGATED_TRADES = "aggregated-trades"
SIGNIFICANT_TRADES = "significant-trades"
TRADE_STREAMS = {RAW_TRADES, AGGREGATED_TRADES, SIGNIFICANT_TRADES}


def get_csv_env(name: str, default: Iterable[str]) -> list[str]:
    """Parse comma-separated environment values."""
    value = os.environ.get(name)
    if not value:
        return list(default)
    return [item.strip() for item in value.split(",") if item.strip()]


def get_publish_streams() -> set[str]:
    """Read the enabled output stream names."""
    streams = set(get_csv_env("PUBLISH_STREAMS", [SIGNIFICANT_TRADES]))
    unknown = streams - TRADE_STREAMS
    if unknown:
        raise ValueError(f"unknown publish streams: {', '.join(sorted(unknown))}")
    return streams


def get_publishers(project_id: str) -> dict[str, PubSubPublisher]:
    """Create one Pub/Sub publisher for each enabled stream."""
    topics = {
        RAW_TRADES: os.environ.get("RAW_TRADES_TOPIC", RAW_TRADES),
        AGGREGATED_TRADES: os.environ.get("AGGREGATED_TRADES_TOPIC", AGGREGATED_TRADES),
        SIGNIFICANT_TRADES: os.environ.get("SIGNIFICANT_TRADES_TOPIC", SIGNIFICANT_TRADES),
    }
    return {stream: PubSubPublisher(project_id, topics[stream]) for stream in get_publish_streams()}


def get_trade_handler(
    publishers: dict[str, PubSubPublisher],
    significant_trade_filter: int = 1_000,
) -> Callable[[TradeEvent], Awaitable[None]]:
    """Build the raw, aggregated, and significant trade pipeline."""
    significant_callback = None
    if SIGNIFICANT_TRADES in publishers:
        significant_callback = SignificantTradeCallback(
            publishers[SIGNIFICANT_TRADES],
            significant_trade_filter=significant_trade_filter,
            window_seconds=60,
        )

    async def handle_aggregated_trade(trade: dict, timestamp: float) -> None:
        if AGGREGATED_TRADES in publishers:
            await publishers[AGGREGATED_TRADES](trade, timestamp)
        if significant_callback is not None:
            await significant_callback(trade, timestamp)

    aggregate_callback = TradeCallback(handle_aggregated_trade)

    async def handle_trade(trade: TradeEvent) -> None:
        payload = trade.to_dict()
        timestamp = trade.received_at.timestamp()
        if RAW_TRADES in publishers:
            await publishers[RAW_TRADES](payload, timestamp)
        if AGGREGATED_TRADES in publishers or SIGNIFICANT_TRADES in publishers:
            await aggregate_callback(payload, timestamp)

    return handle_trade


async def run() -> None:
    """Connect configured exchanges and publish selected trade streams."""
    publishers = get_publishers(os.environ["PROJECT_ID"])
    significant_trade_filter = int(os.environ.get("SIGNIFICANT_TRADE_FILTER", 1_000))
    handler = get_trade_handler(publishers, significant_trade_filter)
    await run_clients(
        [
            (Binance(get_csv_env("BINANCE_SYMBOLS", ["BTCUSDT"])), handler),
            (Coinbase(get_csv_env("COINBASE_SYMBOLS", ["BTC-USD"])), handler),
            (Bitfinex(get_csv_env("BITFINEX_SYMBOLS", ["tBTCUSD"])), handler),
            (Bitmex(get_csv_env("BITMEX_SYMBOLS", ["XBTUSD"])), handler),
        ]
    )


if __name__ == "__main__":
    load_dotenv()
    sentry_dsn = os.environ.get("SENTRY_DSN")
    if sentry_dsn:
        import sentry_sdk

        sentry_sdk.init(sentry_dsn, traces_sample_rate=1.0)

    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass
