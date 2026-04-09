from collections.abc import AsyncIterator
from decimal import Decimal

from ..events import TradeEvent
from ..client import ExchangeClient


class Bitmex(ExchangeClient):
    """BitMEX trade websocket client."""

    exchange = "bitmex"
    url = "wss://ws.bitmex.com/realtime"

    def subscription_messages(self) -> list[dict]:
        """Subscribe to trade tables."""
        return [{"op": "subscribe", "args": [f"trade:{symbol}" for symbol in self.symbols]}]

    async def trades(self) -> AsyncIterator[TradeEvent]:
        """Yield normalized BitMEX trades."""
        async for msg, received_at in self.messages():
            if msg.get("table") != "trade" or msg.get("action") != "insert":
                continue
            for data in msg["data"]:
                price = Decimal(str(data["price"]))
                notional = data.get("homeNotional")
                if notional is None:
                    notional = Decimal(str(data["foreignNotional"])) / price
                yield self.get_trade_event(
                    uid=data["trdMatchID"],
                    symbol=data["symbol"],
                    timestamp=self.parse_datetime(data["timestamp"]),
                    received_at=received_at,
                    price=price,
                    notional=notional,
                    tick_rule=1 if data["side"].lower() == "buy" else -1,
                )
