from collections.abc import Awaitable, Callable

from .events import TradeEvent
from .trades import SignificantTradeCallback, TradeCallback


async def print_payload(payload: dict, timestamp: float) -> None:
    """Print callback payloads."""
    print(payload)


def get_significant_trade_handler(
    significant_trade_filter: int = 1_000,
) -> Callable[[TradeEvent], Awaitable[None]]:
    """Print significant trades with one-minute context."""
    callback = TradeCallback(
        SignificantTradeCallback(
            print_payload,
            significant_trade_filter=significant_trade_filter,
            window_seconds=60,
        )
    )

    async def handle_trade(trade: TradeEvent) -> None:
        await callback(trade.to_dict(), trade.received_at.timestamp())

    return handle_trade
