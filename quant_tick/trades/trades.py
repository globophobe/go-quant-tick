from collections.abc import Callable
from typing import Optional, Tuple


class TradeCallback:
    """
    Aggregate sequences of trades that have equal symbol, timestamp, nanoseconds, and
    tick rule.
    """

    def __init__(self, handler: Callable) -> None:
        self.handler = handler
        self.trades = {}

    async def __call__(self, trade: dict, timestamp: float) -> Tuple[dict, float]:
        t = self.main(trade)
        if t is not None:
            await self.handler(t, timestamp)

    def main(self, trade: dict) -> dict:
        """Subclasses override this method"""
        t = self.prepare_trade(trade)
        return self.aggregate(t)

    def prepare_trade(self, trade: dict) -> dict:
        if "ticks" not in trade:
            trade["ticks"] = 1  # b/c Binance
        if "isSequential" not in trade:
            trade["isSequential"] = False
        return trade

    def aggregate(self, trade: dict) -> Optional[dict]:
        symbol = trade["symbol"]
        trades = self.trades.setdefault(symbol, [])
        if not len(trades):
            self.trades[symbol].append(trade)
        else:
            last_trade = trades[-1]
            if last_trade["timestamp"] == trade["timestamp"]:
                if last_trade["tickRule"] == trade["tickRule"]:
                    self.trades[symbol].append(trade)
                    return
            aggregated = self.get_aggregated_trade(symbol)
            self.trades[symbol] = [trade]  # Next
            return aggregated

    def get_aggregated_trade(self, symbol: str) -> dict:
        trades = self.trades[symbol]
        from copy import copy

        last_trade = copy(trades[-1])
        # Is there more than 1 trade?
        if len(trades) > 1:
            # Assert
            keys = ["timestamp", "tickRule"]
            if last_trade.get("symbol", None):
                keys.append("symbol")
            for key in keys:
                assert len(set([trade[key] for trade in trades])) == 1
            # Aggregate
            last_trade["volume"] = sum([trade["volume"] for trade in trades])
            last_trade["notional"] = sum([trade["notional"] for trade in trades])
            last_trade["ticks"] = sum([trade["ticks"] for trade in trades])
        return last_trade
