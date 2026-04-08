from collections.abc import AsyncIterator

from ..events import TradeEvent
from ..client import ExchangeClient


class Coinbase(ExchangeClient):
    """Coinbase match websocket client."""

    exchange = "coinbase"
    url = "wss://ws-feed.exchange.coinbase.com"

    def __init__(self, symbols: list[str]) -> None:
        """Initialize trade ID tracking."""
        super().__init__(symbols)
        self.last_ids: dict[str, int] = {}

    def subscription_messages(self) -> list[dict]:
        """Subscribe to match messages."""
        return [
            {
                "type": "subscribe",
                "product_ids": self.symbols,
                "channels": ["matches"],
            }
        ]

    async def trades(self) -> AsyncIterator[TradeEvent]:
        """Yield normalized Coinbase matches."""
        async for msg, received_at in self.messages():
            if msg.get("type") not in ("match", "last_match"):
                continue
            symbol = msg["product_id"]
            trade_id = int(msg["trade_id"])
            prev_trade_id = self.last_ids.get(symbol)
            self.last_ids[symbol] = trade_id
            yield self.get_trade_event(
                uid=trade_id,
                symbol=symbol,
                timestamp=self.parse_datetime(msg["time"]),
                received_at=received_at,
                price=msg["price"],
                notional=msg["size"],
                tick_rule=-1 if msg["side"].lower() == "sell" else 1,
                is_sequential=prev_trade_id is None or trade_id == prev_trade_id + 1,
            )
