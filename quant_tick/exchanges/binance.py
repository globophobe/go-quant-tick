from collections.abc import AsyncIterator

from ..events import TradeEvent
from ..client import ExchangeClient


class Binance(ExchangeClient):
    """Binance raw-trade websocket client."""

    exchange = "binance"
    url = "wss://stream.binance.com:9443/ws"

    def __init__(self, symbols: list[str]) -> None:
        """Initialize raw-trade ID tracking."""
        super().__init__(symbols)
        self.last_ids: dict[str, int] = {}

    def subscription_messages(self) -> list[dict]:
        """Subscribe to raw trade streams."""
        return [
            {
                "method": "SUBSCRIBE",
                "params": [f"{symbol.lower()}@trade" for symbol in self.symbols],
                "id": 1,
            }
        ]

    async def trades(self) -> AsyncIterator[TradeEvent]:
        """Yield normalized Binance raw trades."""
        async for msg, received_at in self.messages():
            if msg.get("e") != "trade":
                continue
            symbol = msg["s"]
            trade_id = int(msg["t"])
            prev_trade_id = self.last_ids.get(symbol)
            self.last_ids[symbol] = trade_id
            yield self.get_trade_event(
                uid=trade_id,
                symbol=symbol,
                timestamp=self.parse_datetime(msg["T"]),
                received_at=received_at,
                price=msg["p"],
                notional=msg["q"],
                tick_rule=-1 if msg["m"] else 1,
                is_sequential=prev_trade_id is None or trade_id == prev_trade_id + 1,
            )
