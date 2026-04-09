from datetime import UTC, datetime
from decimal import Decimal
from unittest import TestCase

from quant_tick.trades import SignificantTradeCallback, TradeCallback


def get_trade(
    uid: str,
    *,
    timestamp: datetime,
    nanoseconds: int = 0,
    tick_rule: int = 1,
    price: str = "100",
    notional: str = "1",
    is_sequential: bool = True,
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
        "isSequential": is_sequential,
    }


class TradeCallbackTests(TestCase):
    def test_trade_callback_aggregates_trades(self) -> None:
        callback = TradeCallback(lambda *_: None)
        timestamp = datetime(2026, 4, 8, tzinfo=UTC)

        self.assertIsNone(callback.main(get_trade("1", timestamp=timestamp, nanoseconds=1)))
        self.assertIsNone(
            callback.main(get_trade("2", timestamp=timestamp, nanoseconds=1, notional="2"))
        )
        flushed = callback.main(get_trade("3", timestamp=timestamp, nanoseconds=2))

        self.assertIsNotNone(flushed)
        self.assertEqual(flushed["uid"], "1")
        self.assertEqual(flushed["timestamp"], timestamp)
        self.assertEqual(flushed["nanoseconds"], 1)
        self.assertEqual(flushed["tickRule"], 1)
        self.assertEqual(flushed["ticks"], 2)
        self.assertEqual(flushed["notional"], Decimal("3"))
        self.assertEqual(flushed["volume"], Decimal("300"))


class SignificantTradeCallbackTests(TestCase):
    def test_significant_trade_callback_emits_context_tick(self) -> None:
        callback = SignificantTradeCallback(
            lambda *_: None,
            significant_trade_filter=1_000,
            window_seconds=60,
        )
        timestamp = datetime(2026, 4, 8, 0, 0, 0, tzinfo=UTC)

        self.assertIsNone(callback.main(get_trade("1", timestamp=timestamp, price="100", notional="1")))
        self.assertIsNone(
            callback.main(
                get_trade(
                    "2",
                    timestamp=timestamp.replace(second=1),
                    price="101",
                    notional="2",
                    tick_rule=-1,
                    is_sequential=False,
                )
            )
        )
        result = callback.main(get_trade("3", timestamp=timestamp.replace(minute=1), price="102", notional="1"))

        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        payload = result[0]
        self.assertEqual(payload["uid"], "2")
        self.assertEqual(payload["timestamp"], timestamp.replace(second=1))
        self.assertEqual(payload["price"], Decimal("101"))
        self.assertIsNone(payload["volume"])
        self.assertIsNone(payload["notional"])
        self.assertIsNone(payload["tickRule"])
        self.assertIsNone(payload["ticks"])
        self.assertEqual(payload["high"], Decimal("101"))
        self.assertEqual(payload["low"], Decimal("100"))
        self.assertEqual(payload["totalBuyVolume"], Decimal("100"))
        self.assertEqual(payload["totalVolume"], Decimal("302"))
        self.assertEqual(payload["totalBuyNotional"], Decimal("1"))
        self.assertEqual(payload["totalNotional"], Decimal("3"))
        self.assertEqual(payload["totalBuyTicks"], 1)
        self.assertEqual(payload["totalTicks"], 2)
        self.assertFalse(payload["isSequential"])
