from collections.abc import AsyncIterator

from ..events import TradeEvent
from ..client import ExchangeClient


class Binance(ExchangeClient):
    """Binance aggregate-trade websocket client."""

    exchange = "binance"
    url = "wss://stream.binance.com:9443/ws"

    def __init__(self, symbols: list[str]) -> None:
        """Initialize aggregate-trade ID tracking."""
        super().__init__(symbols)
        self.last_ids: dict[str, int] = {}

    def subscription_messages(self) -> list[dict]:
        """Subscribe to aggregate trade streams."""
        return [
            {
                "method": "SUBSCRIBE",
                "params": [f"{symbol.lower()}@aggTrade" for symbol in self.symbols],
                "id": 1,
            }
        ]

    async def trades(self) -> AsyncIterator[TradeEvent]:
        """Yield normalized Binance aggregate trades."""
        async for msg, received_at in self.messages():
            if msg.get("e") != "aggTrade":
                continue
            symbol = msg["s"]
            first_id = int(msg["f"])
            last_id = int(msg["l"])
            prev_last_id = self.last_ids.get(symbol)
            self.last_ids[symbol] = last_id
            yield self.get_trade_event(
                uid=last_id,
                symbol=symbol,
                timestamp=self.parse_datetime(msg["T"]),
                received_at=received_at,
                price=msg["p"],
                notional=msg["q"],
                tick_rule=-1 if msg["m"] else 1,
                ticks=last_id - first_id + 1,
                is_sequential=prev_last_id is None or first_id == prev_last_id + 1,
            )
