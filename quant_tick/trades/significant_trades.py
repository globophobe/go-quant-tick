from collections.abc import Callable
from decimal import Decimal
from typing import List, Optional, Tuple

from .constants import NOTIONAL, TICKS, VOLUME
from .window import WindowMixin


class SignificantTradeCallback(WindowMixin):
    """Emit trades that exceed the volume filter with window context."""

    def __init__(
        self,
        handler: Callable,
        significant_trade_filter: int = 1000,
        window_seconds: Optional[int] = None,
    ) -> None:
        self.handler = handler
        self.significant_trade_filter = Decimal(significant_trade_filter)
        self.trades = {}
        self.window_seconds = window_seconds
        self.window = {}

    async def __call__(self, trade: dict, timestamp: float) -> Tuple[dict, float]:
        result = self.main(trade)
        if isinstance(result, list):
            for t in result:
                await self.handler(t, timestamp)
        elif isinstance(result, dict):
            await self.handler(result, timestamp)

    def main(self, trade: dict) -> Optional[dict]:
        symbol = trade["symbol"]
        timestamp = trade["timestamp"]
        self.trades.setdefault(symbol, [])
        window = self.get_window(symbol, timestamp)
        if window is not None:
            # Was message received late?
            if window["start"] is not None and timestamp < window["start"]:
                # FUBAR
                return self.aggregate([trade], is_late=True)
            # Is window exceeded?
            elif window["stop"] is not None and timestamp >= window["stop"]:
                ticks = []
                # Get tick
                tick = self.get_tick(symbol)
                if tick is not None:
                    ticks.append(tick)
                # Append trade
                self.trades[symbol].append(trade)
                # Maybe another tick
                if trade[VOLUME] >= self.significant_trade_filter:
                    tick = self.get_tick(symbol)
                    ticks.append(tick)
                # Set window
                self.set_window(symbol, timestamp)
                # Finally, return ticks
                return ticks
            else:
                return self.get_significant_trade_or_tick(symbol, trade)
        else:
            return self.get_significant_trade_or_tick(symbol, trade)

    def get_significant_trade_or_tick(self, symbol: str, trade: dict) -> Optional[dict]:
        if trade[VOLUME] < self.significant_trade_filter:
            self.trades[symbol].append(trade)
        else:
            self.trades[symbol].append(trade)
            return self.get_tick(symbol)

    def aggregate(self, trades: List[dict], is_late: bool = False) -> Optional[dict]:
        buy_trades = [t for t in trades if t["tickRule"] == 1]
        stats = {
            "high": max(t["price"] for t in trades),
            "low": min(t["price"] for t in trades),
            "totalBuyVolume": sum([t[VOLUME] for t in buy_trades]),
            "totalVolume": sum([t[VOLUME] for t in trades]),
            "totalBuyNotional": sum([t[NOTIONAL] for t in buy_trades]),
            "totalNotional": sum([t[NOTIONAL] for t in trades]),
            "totalBuyTicks": sum([t[TICKS] for t in buy_trades]),
            "totalTicks": sum([t[TICKS] for t in trades]),
        }
        greater_than_significant_trade_filter = [
            t for t in trades if t[VOLUME] >= self.significant_trade_filter
        ]
        # Are there 1 or 0 trades, which exceed min_volume?
        assert len(greater_than_significant_trade_filter) <= 1
        if greater_than_significant_trade_filter:
            data = greater_than_significant_trade_filter[0]
            data.update(stats)
        else:
            last_trade = trades[-1]
            data = {
                "exchange": last_trade["exchange"],
                "symbol": last_trade["symbol"],
                "timestamp": last_trade["timestamp"],
                "price": last_trade["price"],
                "isSequential": all([t["isSequential"] for t in trades]),
            }
            data.update(stats)
        if is_late:
            data["isLate"] = is_late
        return data
