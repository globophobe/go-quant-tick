from collections.abc import AsyncIterator
from datetime import UTC, datetime
from decimal import Decimal
from unittest import IsolatedAsyncioTestCase

from quant_tick.exchanges import Bitfinex


class BitfinexFixture(Bitfinex):
    """Bitfinex client with fixture messages."""

    async def messages(self) -> AsyncIterator[tuple[dict | list, datetime]]:
        """Yield fixture websocket messages."""
        received_at = datetime(2026, 4, 8, tzinfo=UTC)
        yield {"event": "subscribed", "chanId": 1, "symbol": "tBTCUSD"}, received_at
        yield [1, "hb"], received_at
        yield [1, "tu", [999, 1775557139000, 1, 99]], received_at
        yield [1, "te", [100, 1775557140000, 1, 100]], received_at
        yield [1, "te", [102, 1775557141000, -2, 101]], received_at
        yield [1, "te", [101, 1775557142000, 3, 102]], received_at


class BitfinexTests(IsolatedAsyncioTestCase):
    async def test_bitfinex_normalizes_trade_messages(self) -> None:
        client = BitfinexFixture(["tBTCUSD"])
        trades = []
        async for trade in client.trades():
            trades.append(trade)

        self.assertEqual(len(trades), 3)
        self.assertEqual([trade.uid for trade in trades], ["100", "102", "101"])
        self.assertEqual([trade.is_sequential for trade in trades], [True, True, False])
        self.assertEqual([trade.exchange for trade in trades], ["bitfinex"] * 3)
        self.assertEqual([trade.symbol for trade in trades], ["tBTCUSD"] * 3)
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
                datetime.fromtimestamp(1775557140, UTC),
                datetime.fromtimestamp(1775557141, UTC),
                datetime.fromtimestamp(1775557142, UTC),
            ],
        )
