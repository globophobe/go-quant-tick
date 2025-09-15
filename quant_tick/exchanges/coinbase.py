from collections import defaultdict
from decimal import Decimal
from typing import Any

from cryptofeed.connection import (
    AsyncConnection,
    RestEndpoint,
    Routes,
    WebsocketEndpoint,
)
from cryptofeed.defines import ASK, BID, BUY, L3_BOOK, SELL, TRADES
from cryptofeed.exchanges import Coinbase as BaseCoinbase
from cryptofeed.symbols import Symbol
from cryptofeed.types import Trade
from yapic import json

from ..feed import Feed


class Coinbase(Feed, BaseCoinbase):
    """Coinbase."""

    websocket_endpoints = [
        WebsocketEndpoint(
            "wss://ws-feed.exchange.coinbase.com", options={"compression": None}
        )
    ]
    rest_endpoints = [
        RestEndpoint("https://api.exchange.coinbase.com", routes=Routes("/products"))
    ]

    @classmethod
    def _parse_symbol_data(cls, data: list) -> tuple[dict, dict]:
        """Parse symbol data."""
        ret = {}
        info = defaultdict(dict)

        for entry in data:
            sym = Symbol(entry["base_currency"], entry["quote_currency"])
            info["tick_size"][sym.normalized] = entry["quote_increment"]
            info["instrument_type"][sym.normalized] = sym.type
            ret[sym.normalized] = entry["id"]
        return ret, info

    async def subscribe(self, conn: AsyncConnection) -> None:
        """Subscribe."""
        self.__reset()
        all_pairs = list()

        async def _subscribe(chan: str, product_ids: list) -> None:
            """Subscribe."""
            if chan == "market_trades":
                chan = {"name": "ticker", "product_ids": product_ids}

            params = {"type": "subscribe", "product_ids": product_ids, "channels": chan}
            await conn.write(json.dumps(params))

        for channel in self.subscription:
            all_pairs += self.subscription[channel]
            await _subscribe(channel, self.subscription[channel])
        all_pairs = list(dict.fromkeys(all_pairs))
        await _subscribe("heartbeat", all_pairs)
        # Implementing heartbeat as per Best Practices doc: https://docs.cloud.coinbase.com/advanced-trade-api/docs/ws-best-practices

    async def _trade_update(self, msg: dict, timestamp: float):
        """
        {
            'trade_id': 43736593
            'side': 'BUY' or 'SELL',
            'size': '0.01235647',
            'price': '8506.26000000',
            'product_id': 'BTC-USD',
            'time': '2018-05-21T00:26:05.585000Z'
        }
        """
        pair = self.exchange_symbol_to_std_symbol(msg["product_id"])
        ts = self.timestamp_normalize(msg["time"])
        order_type = "market"
        t = Trade(
            self.id,
            pair,
            SELL if msg["side"] == "SELL" else BUY,
            Decimal(msg["size"]),
            Decimal(msg["price"]),
            ts,
            id=str(msg["trade_id"]),
            type=order_type,
            raw=msg,
        )
        import pdb

        pdb.set_trace()
        await self.callback(TRADES, t, timestamp)

    async def _book_update(self, msg: dict, timestamp: float) -> Any:
        """
        {
            'type': 'match', or last_match
            'trade_id': 43736593
            'maker_order_id': '2663b65f-b74e-4513-909d-975e3910cf22',
            'taker_order_id': 'd058d737-87f1-4763-bbb4-c2ccf2a40bde',
            'side': 'buy',
            'size': '0.01235647',
            'price': '8506.26000000',
            'product_id': 'BTC-USD',
            'sequence': 5928276661,
            'time': '2018-05-21T00:26:05.585000Z'
        }
        """
        pair = msg["product_id"]
        ts = self.timestamp_normalize(msg["time"])

        if (
            self.keep_l3_book
            and "full" in self.subscription
            and pair in self.subscription["full"]
        ):
            delta = {BID: [], ASK: []}
            price = Decimal(msg["price"])
            side = ASK if msg["side"] == "sell" else BID
            size = Decimal(msg["size"])
            maker_order_id = msg["maker_order_id"]

            _, new_size = self.order_map[maker_order_id]
            new_size -= size
            if new_size <= 0:
                del self.order_map[maker_order_id]
                self.order_type_map.pop(maker_order_id, None)
                delta[side].append((maker_order_id, price, 0))
                del self._l3_book[pair].book[side][price][maker_order_id]
                if len(self._l3_book[pair].book[side][price]) == 0:
                    del self._l3_book[pair].book[side][price]
            else:
                self.order_map[maker_order_id] = (price, new_size)
                self._l3_book[pair].book[side][price][maker_order_id] = new_size
                delta[side].append((maker_order_id, price, new_size))

            await self.book_callback(
                L3_BOOK,
                self._l3_book[pair],
                timestamp,
                timestamp=ts,
                delta=delta,
                raw=msg,
                sequence_number=self.seq_no[pair],
            )

        price = Decimal(msg["price"])
        notional = Decimal(msg["size"])
        volume = price * notional
        t = {
            "exchange": self.id.lower(),
            "uid": int(msg["trade_id"]),
            "symbol": msg["product_id"],  # Do not normalize
            "timestamp": self.parse_datetime(msg["time"]),
            "price": price,
            "volume": volume,
            "notional": notional,
            "tickRule": 1 if msg["side"].lower() == SELL else -1,
        }
        import pdb

        pdb.set_trace()
        await self.callback(TRADES, t, ts)
