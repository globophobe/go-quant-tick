from datetime import UTC, datetime
from decimal import Decimal
from unittest import IsolatedAsyncioTestCase

import main
from quant_tick.events import TradeEvent


class Handler:
    def __init__(self):
        self.payloads = []

    async def __call__(self, payload, timestamp):
        self.payloads.append((payload, timestamp))


def get_trade(timestamp, price="100", notional="1"):
    price = Decimal(price)
    notional = Decimal(notional)
    return TradeEvent(
        exchange="test",
        symbol="BTCUSD",
        uid=str(timestamp.timestamp()),
        timestamp=timestamp,
        received_at=timestamp,
        price=price,
        notional=notional,
        volume=price * notional,
        tick_rule=1,
        is_sequential=True,
    )


class MainTests(IsolatedAsyncioTestCase):
    async def test_significant_trade_handler_uses_one_minute_window(self):
        handler = Handler()
        trade_handler = main.get_trade_handler(
            {main.SIGNIFICANT_TRADES: handler},
            significant_trade_filter=1_000,
        )
        await trade_handler(get_trade(datetime(2026, 4, 8, 0, 0, 0, tzinfo=UTC)))
        await trade_handler(get_trade(datetime(2026, 4, 8, 0, 1, 0, tzinfo=UTC), price="1", notional="1"))
        await trade_handler(get_trade(datetime(2026, 4, 8, 0, 2, 0, tzinfo=UTC), price="1", notional="1"))

        self.assertEqual(len(handler.payloads), 1)
        payload, _ = handler.payloads[0]
        self.assertNotIn("receivedAt", payload)
        self.assertEqual(payload["uid"], str(datetime(2026, 4, 8, 0, 0, 0, tzinfo=UTC).timestamp()))
        self.assertEqual(payload["nanoseconds"], 0)
        self.assertIsNone(payload["volume"])
        self.assertIsNone(payload["notional"])
        self.assertIsNone(payload["tickRule"])
        self.assertIsNone(payload["ticks"])
        self.assertEqual(payload["totalVolume"], Decimal("100"))
        self.assertEqual(payload["timestamp"], datetime(2026, 4, 8, 0, 0, 0, tzinfo=UTC))
