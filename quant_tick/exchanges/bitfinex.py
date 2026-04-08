import logging
from collections.abc import AsyncIterator

from ..events import TradeEvent
from ..client import ExchangeClient

LOG = logging.getLogger(__name__)


class Bitfinex(ExchangeClient):
    """Bitfinex trades websocket feed."""

    exchange = "bitfinex"
    url = "wss://api-pub.bitfinex.com/ws/2"

    def __init__(self, symbols: list[str]) -> None:
        """Initialize."""
        super().__init__(symbols)
        self.channel_symbols: dict[int, str] = {}
        self.last_ids: dict[str, int] = {}

    def subscription_messages(self) -> list[dict]:
        """Return subscription messages."""
        return [
            {
                "event": "subscribe",
                "channel": "trades",
                "symbol": self.get_api_symbol(symbol),
            }
            for symbol in self.symbols
        ]

    def get_api_symbol(self, symbol: str) -> str:
        """Return Bitfinex API symbol."""
        return symbol if symbol.startswith("t") else f"t{symbol}"

    async def trades(self) -> AsyncIterator[TradeEvent]:
        """Yield normalized trade events."""
        async for msg, received_at in self.messages():
            if isinstance(msg, dict):
                if msg.get("event") == "subscribed":
                    self.channel_symbols[msg["chanId"]] = msg["symbol"]
                elif msg.get("event") == "error":
                    LOG.warning("%s subscription error: %s", self.exchange, msg)
                continue
            if not isinstance(msg, list) or len(msg) < 2:
                continue
            channel_id = msg[0]
            symbol = self.channel_symbols.get(channel_id)
            if symbol is None:
                continue
            tag = msg[1]
            if tag in ("hb", "tu"):
                continue
            if isinstance(tag, list):
                continue
            if tag != "te" or len(msg) < 3:
                LOG.warning("%s %s unexpected trade message: %s", self.exchange, symbol, msg)
                continue
            uid, ts, notional, price = msg[2]
            trade_id = int(uid)
            prev_trade_id = self.last_ids.get(symbol)
            self.last_ids[symbol] = trade_id
            yield self.get_trade_event(
                uid=trade_id,
                symbol=symbol,
                timestamp=self.parse_datetime(ts),
                received_at=received_at,
                price=price,
                notional=abs(notional),
                tick_rule=-1 if notional < 0 else 1,
                is_sequential=prev_trade_id is None or trade_id > prev_trade_id,
            )
