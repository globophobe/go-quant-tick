from collections.abc import AsyncIterator
from datetime import UTC, datetime
from decimal import Decimal
from unittest import IsolatedAsyncioTestCase

from quant_tick.exchanges import Hyperliquid


class HyperliquidFixture(Hyperliquid):
    """Hyperliquid client with fixture messages."""

    async def messages(self) -> AsyncIterator[tuple[dict, datetime]]:
        """Yield fixture websocket messages."""
        received_at = datetime(2026, 4, 8, tzinfo=UTC)
        yield {
            "channel": "subscriptionResponse",
            "data": {
                "method": "subscribe",
                "subscription": {"type": "trades", "coin": "BTC"},
            },
        }, received_at
        yield {
            "channel": "trades",
            "data": [
                {
                    "coin": "BTC",
                    "side": "B",
                    "px": "100",
                    "sz": "1.5",
                    "hash": "0xabc",
                    "time": 1775606400000,
                    "tid": 10,
                    "users": ["0x1", "0x2"],
                },
                {
                    "coin": "BTC",
                    "side": "A",
                    "px": "101",
                    "sz": "2.5",
                    "hash": "0xdef",
                    "time": 1775606401000,
                    "tid": 11,
                    "users": ["0x3", "0x4"],
                },
            ],
        }, received_at


class HyperliquidTests(IsolatedAsyncioTestCase):
    async def test_hyperliquid_normalizes_trade_messages(self) -> None:
        client = HyperliquidFixture(["BTC"])
        trades = []
        async for trade in client.trades():
            trades.append(trade)

        self.assertEqual(len(trades), 2)
        self.assertEqual([trade.uid for trade in trades], ["1775606400000:BTC:10", "1775606401000:BTC:11"])
        self.assertEqual([trade.exchange for trade in trades], ["hyperliquid"] * 2)
        self.assertEqual([trade.symbol for trade in trades], ["BTC"] * 2)
        self.assertEqual([trade.tick_rule for trade in trades], [1, -1])
        self.assertEqual([trade.price for trade in trades], [Decimal("100"), Decimal("101")])
        self.assertEqual([trade.notional for trade in trades], [Decimal("1.5"), Decimal("2.5")])
        self.assertEqual([trade.volume for trade in trades], [Decimal("150.0"), Decimal("252.5")])
        self.assertEqual([trade.is_sequential for trade in trades], [False, False])
        self.assertEqual(
            [trade.timestamp for trade in trades],
            [
                datetime.fromtimestamp(1775606400, UTC),
                datetime.fromtimestamp(1775606401, UTC),
            ],
        )
