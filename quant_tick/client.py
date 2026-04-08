import asyncio
import json
import logging
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from .events import TradeEvent

LOG = logging.getLogger(__name__)


class ExchangeClient:
    """Base websocket exchange client."""

    exchange = ""
    url = ""

    def __init__(self, symbols: list[str]) -> None:
        """Initialize."""
        self.symbols = symbols

    async def messages(self) -> AsyncIterator[tuple[Any, datetime]]:
        """Yield decoded websocket messages with receive timestamps."""
        while True:
            try:
                async with connect(
                    self.url,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=1,
                    open_timeout=10,
                ) as websocket:
                    for message in self.subscription_messages():
                        await websocket.send(json.dumps(message))
                    async for message in websocket:
                        yield self.loads(message), datetime.now(UTC)
            except asyncio.CancelledError:
                raise
            except ConnectionClosed:
                LOG.warning("%s websocket disconnected", self.exchange)
            except Exception:
                LOG.exception("%s websocket error", self.exchange)
            await asyncio.sleep(1)

    def loads(self, message: str | bytes) -> Any:
        """Load websocket JSON message."""
        if isinstance(message, bytes):
            message = message.decode()
        return json.loads(message)

    def parse_datetime(self, value: int | float | str, unit: str = "ms") -> datetime:
        """Parse exchange timestamps."""
        if isinstance(value, str) and not value.isdigit():
            return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
        value = int(value)
        if unit == "ms":
            return datetime.fromtimestamp(value / 1_000, UTC)
        if unit == "us":
            return datetime.fromtimestamp(value / 1_000_000, UTC)
        if unit == "ns":
            return datetime.fromtimestamp(value / 1_000_000_000, UTC)
        return datetime.fromtimestamp(value, UTC)

    def get_trade_event(
        self,
        *,
        uid: str | int,
        symbol: str,
        timestamp: datetime,
        received_at: datetime,
        price: str | int | float | Decimal,
        notional: str | int | float | Decimal,
        tick_rule: int,
        nanoseconds: int = 0,
        ticks: int = 1,
        is_sequential: bool = False,
    ) -> TradeEvent:
        """Build normalized trade event."""
        price = Decimal(str(price))
        notional = Decimal(str(notional))
        return TradeEvent(
            exchange=self.exchange,
            uid=str(uid),
            symbol=symbol,
            timestamp=timestamp,
            received_at=received_at,
            price=price,
            volume=price * notional,
            notional=notional,
            tick_rule=tick_rule,
            nanoseconds=nanoseconds,
            ticks=ticks,
            is_sequential=is_sequential,
        )

    def subscription_messages(self) -> list[dict]:
        """Return websocket subscription messages."""
        raise NotImplementedError

    async def trades(self) -> AsyncIterator[TradeEvent]:
        """Yield normalized trade events."""
        raise NotImplementedError
