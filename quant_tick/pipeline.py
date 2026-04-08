import asyncio
from collections.abc import Awaitable, Callable, Iterable

from .events import TradeEvent
from .client import ExchangeClient

TradeHandler = Callable[[TradeEvent], Awaitable[None]]


async def run_client(client: ExchangeClient, handler: TradeHandler) -> None:
    """Run one exchange client."""
    async for trade in client.trades():
        await handler(trade)


async def run_clients(clients: Iterable[tuple[ExchangeClient, TradeHandler]]) -> None:
    """Run exchange clients concurrently."""
    await asyncio.gather(*(run_client(client, handler) for client, handler in clients))
