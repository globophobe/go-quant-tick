from collections.abc import AsyncIterator

from ..events import TradeEvent
from ..client import ExchangeClient


class Hyperliquid(ExchangeClient):
    """Hyperliquid perp trade websocket client."""

    exchange = "hyperliquid"
    url = "wss://api.hyperliquid.xyz/ws"

    def subscription_messages(self) -> list[dict]:
        """Subscribe to perp trade streams."""
        return [
            {
                "method": "subscribe",
                "subscription": {"type": "trades", "coin": symbol},
            }
            for symbol in self.symbols
        ]

    async def trades(self) -> AsyncIterator[TradeEvent]:
        """Yield normalized Hyperliquid trades."""
        async for msg, received_at in self.messages():
            if msg.get("channel") != "trades":
                continue
            for data in msg.get("data", []):
                trade_time = int(data["time"])
                symbol = data["coin"]
                trade_id = data["tid"]
                side = str(data["side"]).lower()
                yield self.get_trade_event(
                    uid=f"{trade_time}:{symbol}:{trade_id}",
                    symbol=symbol,
                    timestamp=self.parse_datetime(trade_time),
                    received_at=received_at,
                    price=data["px"],
                    notional=data["sz"],
                    tick_rule=1 if side in {"b", "buy"} else -1,
                )
