#!/usr/bin/env python3
import asyncio

from quant_tick.console import get_significant_trade_handler
from quant_tick.exchanges import Bitmex


async def main() -> None:
    """Print BitMEX significant trade events."""
    handler = get_significant_trade_handler()
    async for trade in Bitmex(["XBTUSD"]).trades():
        await handler(trade)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
