from collections.abc import AsyncIterator
from datetime import UTC, datetime
from unittest import IsolatedAsyncioTestCase

from quant_tick.exchanges import Coinbase


class CoinbaseFixture(Coinbase):
    """Coinbase client with fixture messages."""

    async def messages(self) -> AsyncIterator[tuple[dict, datetime]]:
        """Yield fixture websocket messages."""
        received_at = datetime(2026, 4, 8, tzinfo=UTC)
        for trade_id in (100, 101, 103):
            yield {
                "type": "match",
                "trade_id": trade_id,
                "product_id": "BTC-USD",
                "time": "2026-04-08T00:00:00Z",
                "price": "100",
                "size": "1",
                "side": "buy",
            }, received_at


class CoinbaseTests(IsolatedAsyncioTestCase):
    async def test_coinbase_tracks_sequential_trade_ids(self) -> None:
        client = CoinbaseFixture(["BTC-USD"])
        trades = []
        async for trade in client.trades():
            trades.append(trade)

        self.assertEqual([trade.uid for trade in trades], ["100", "101", "103"])
        self.assertEqual([trade.is_sequential for trade in trades], [True, True, False])
