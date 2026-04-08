from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True)
class TradeEvent:
    """Normalized exchange trade event."""

    exchange: str
    symbol: str
    uid: str
    timestamp: datetime
    received_at: datetime
    price: Decimal
    notional: Decimal
    volume: Decimal
    tick_rule: int
    nanoseconds: int = 0
    ticks: int = 1
    is_sequential: bool = False

    def to_dict(self) -> dict:
        """Convert the event to the aggregation payload shape."""
        return {
            "exchange": self.exchange,
            "uid": self.uid,
            "symbol": self.symbol,
            "timestamp": self.timestamp,
            "nanoseconds": self.nanoseconds,
            "price": self.price,
            "volume": self.volume,
            "notional": self.notional,
            "tickRule": self.tick_rule,
            "ticks": self.ticks,
            "isSequential": self.is_sequential,
        }
