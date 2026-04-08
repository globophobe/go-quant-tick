from datetime import UTC, datetime
from decimal import Decimal
from unittest import TestCase

from quant_tick.trades import TradeCallback


def get_trade(
    uid: str,
    *,
    timestamp: datetime,
    nanoseconds: int = 0,
    tick_rule: int = 1,
    price: str = "100",
    notional: str = "1",
) -> dict:
    price_decimal = Decimal(price)
    notional_decimal = Decimal(notional)
    return {
        "exchange": "test",
        "symbol": "BTCUSD",
        "uid": uid,
        "timestamp": timestamp,
        "nanoseconds": nanoseconds,
        "price": price_decimal,
        "volume": price_decimal * notional_decimal,
        "notional": notional_decimal,
        "tickRule": tick_rule,
        "ticks": 1,
        "isSequential": True,
    }


class TradeCallbackTests(TestCase):
    def test_aggregate_trades_groups_by_nanoseconds(self) -> None:
        callback = TradeCallback(lambda *_: None)
        timestamp = datetime(2026, 4, 8, tzinfo=UTC)

        self.assertIsNone(callback.main(get_trade("1", timestamp=timestamp, nanoseconds=1)))
        flushed = callback.main(get_trade("2", timestamp=timestamp, nanoseconds=2))

        self.assertIsNotNone(flushed)
        self.assertEqual(flushed["uid"], "1")
        self.assertEqual(flushed["nanoseconds"], 1)
        self.assertEqual(flushed["ticks"], 1)

    def test_aggregate_trades_keeps_first_uid_for_sample(self) -> None:
        callback = TradeCallback(lambda *_: None)
        timestamp = datetime(2026, 4, 8, tzinfo=UTC)

        self.assertIsNone(callback.main(get_trade("1", timestamp=timestamp, nanoseconds=1)))
        self.assertIsNone(
            callback.main(get_trade("2", timestamp=timestamp, nanoseconds=1, notional="2"))
        )
        flushed = callback.main(get_trade("3", timestamp=timestamp, nanoseconds=1, tick_rule=-1))

        self.assertIsNotNone(flushed)
        self.assertEqual(flushed["uid"], "1")
        self.assertEqual(flushed["ticks"], 2)
        self.assertEqual(flushed["notional"], Decimal("3"))
        self.assertEqual(flushed["volume"], Decimal("300"))
