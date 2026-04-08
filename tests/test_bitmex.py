from collections.abc import AsyncIterator
from datetime import UTC, datetime
from decimal import Decimal
from unittest import IsolatedAsyncioTestCase

from quant_tick.exchanges import Bitmex


class BitmexFixture(Bitmex):
    """BitMEX client with fixture messages."""

    async def messages(self) -> AsyncIterator[tuple[dict, datetime]]:
        """Yield fixture websocket messages."""
        received_at = datetime(2026, 4, 8, tzinfo=UTC)
        yield {
            "table": "trade",
            "action": "insert",
            "data": [
                {
                    "trdMatchID": "a",
                    "symbol": "XBTUSD",
                    "timestamp": "2026-04-08T00:00:00.000Z",
                    "side": "Buy",
                    "price": 100.0,
                    "homeNotional": "1.5",
                },
                {
                    "trdMatchID": "b",
                    "symbol": "XBTUSD",
                    "timestamp": "2026-04-08T00:00:01.000Z",
                    "side": "Sell",
                    "price": 200.0,
                    "foreignNotional": "400.0",
                },
            ],
        }, received_at


class BitmexTests(IsolatedAsyncioTestCase):
    async def test_bitmex_normalizes_trade_messages(self) -> None:
        client = BitmexFixture(["XBTUSD"])
        trades = []
        async for trade in client.trades():
            trades.append(trade)

        self.assertEqual(len(trades), 2)
        self.assertEqual([trade.uid for trade in trades], ["a", "b"])
        self.assertEqual([trade.exchange for trade in trades], ["bitmex"] * 2)
        self.assertEqual([trade.symbol for trade in trades], ["XBTUSD"] * 2)
        self.assertEqual([trade.tick_rule for trade in trades], [1, -1])
        self.assertEqual([trade.price for trade in trades], [Decimal("100.0"), Decimal("200.0")])
        self.assertEqual([trade.notional for trade in trades], [Decimal("1.5"), Decimal("2")])
        self.assertEqual([trade.volume for trade in trades], [Decimal("150.00"), Decimal("400.0")])
        self.assertEqual(
            [trade.timestamp for trade in trades],
            [
                datetime(2026, 4, 8, 0, 0, 0, tzinfo=UTC),
                datetime(2026, 4, 8, 0, 0, 1, tzinfo=UTC),
            ],
        )
