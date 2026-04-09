from collections.abc import AsyncIterator
from datetime import UTC, datetime
from decimal import Decimal
from unittest import IsolatedAsyncioTestCase

from quant_tick.exchanges import Binance


class BinanceFixture(Binance):
    """Binance client with fixture messages."""

    async def messages(self) -> AsyncIterator[tuple[dict, datetime]]:
        """Yield fixture websocket messages."""
        received_at = datetime(2026, 4, 8, tzinfo=UTC)
        messages = [
            {
                "e": "trade",
                "s": "BTCUSDT",
                "t": 100,
                "T": 1775606400000,
                "p": "100",
                "q": "1",
                "m": False,
            },
            {
                "e": "trade",
                "s": "BTCUSDT",
                "t": 101,
                "T": 1775606401000,
                "p": "101",
                "q": "2",
                "m": True,
            },
            {
                "e": "trade",
                "s": "BTCUSDT",
                "t": 103,
                "T": 1775606402000,
                "p": "102",
                "q": "3",
                "m": False,
            },
        ]
        for message in messages:
            yield message, received_at


class BinanceTests(IsolatedAsyncioTestCase):
    async def test_binance_normalizes_trade_messages(self) -> None:
        client = BinanceFixture(["BTCUSDT"])
        trades = []
        async for trade in client.trades():
            trades.append(trade)

        self.assertEqual(len(trades), 3)
        self.assertEqual([trade.uid for trade in trades], ["100", "101", "103"])
        self.assertEqual([trade.ticks for trade in trades], [1, 1, 1])
        self.assertEqual([trade.is_sequential for trade in trades], [True, True, False])
        self.assertEqual([trade.exchange for trade in trades], ["binance"] * 3)
        self.assertEqual([trade.symbol for trade in trades], ["BTCUSDT"] * 3)
        self.assertEqual([trade.tick_rule for trade in trades], [1, -1, 1])
        self.assertEqual([trade.price for trade in trades], [Decimal("100"), Decimal("101"), Decimal("102")])
        self.assertEqual([trade.notional for trade in trades], [Decimal("1"), Decimal("2"), Decimal("3")])
        self.assertEqual(
            [trade.volume for trade in trades],
            [Decimal("100"), Decimal("202"), Decimal("306")],
        )
        self.assertEqual(
            [trade.timestamp for trade in trades],
            [
                datetime.fromtimestamp(1775606400, UTC),
                datetime.fromtimestamp(1775606401, UTC),
                datetime.fromtimestamp(1775606402, UTC),
            ],
        )
