from collections.abc import AsyncIterator
from datetime import UTC, datetime
from unittest import IsolatedAsyncioTestCase

from quant_tick.exchanges import Bitfinex


class BitfinexFixture(Bitfinex):
    """Bitfinex client with fixture messages."""

    async def messages(self) -> AsyncIterator[tuple[dict | list, datetime]]:
        """Yield fixture websocket messages."""
        received_at = datetime(2026, 4, 8, tzinfo=UTC)
        yield {"event": "subscribed", "chanId": 1, "symbol": "tBTCUSD"}, received_at
        for trade_id in (100, 102, 101):
            yield [1, "te", [trade_id, 1775557140000, 1, 100]], received_at


class BitfinexTests(IsolatedAsyncioTestCase):
    async def test_bitfinex_tracks_monotonic_trade_ids(self) -> None:
        client = BitfinexFixture(["tBTCUSD"])
        trades = []
        async for trade in client.trades():
            trades.append(trade)

        self.assertEqual([trade.uid for trade in trades], ["100", "102", "101"])
        self.assertEqual([trade.is_sequential for trade in trades], [True, True, False])
