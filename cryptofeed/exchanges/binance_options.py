'''
Copyright (C) 2017-2022 Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import logging
from asyncio import create_task, sleep
from collections import defaultdict
from decimal import Decimal
import requests
import time
from typing import Dict, Union, Tuple
from urllib.parse import urlencode

from yapic import json

from cryptofeed.connection import AsyncConnection, HTTPPoll, HTTPConcurrentPoll, RestEndpoint, Routes, WebsocketEndpoint
from cryptofeed.defines import ASK, BALANCES, BID, BINANCE_OPTIONS, OPTION, BUY, CANDLES, FUNDING, FUTURES, L2_BOOK, LIMIT, LIQUIDATIONS, MARKET, OPEN_INTEREST, ORDER_INFO, PERPETUAL, SELL, SPOT, TICKER, TRADES, FILLED, UNFILLED, CALL, PUT
from cryptofeed.feed import Feed
from cryptofeed.symbols import Symbol
from cryptofeed.exchanges.binance import Binance
from cryptofeed.exchanges.mixins.binance_rest import BinanceRestMixin
from cryptofeed.types import Trade, Ticker, Candle, Liquidation, Funding, OrderBook, OrderInfo, Balance

REFRESH_SNAPSHOT_MIN_INTERVAL_SECONDS = 60

LOG = logging.getLogger('feedhandler')


class BinanceOptions(Binance, BinanceRestMixin):
    id = BINANCE_OPTIONS
    websocket_endpoints = [WebsocketEndpoint('wss://nbstream.binance.com/eoptions', options={'compression': None})]
    rest_endpoints = [RestEndpoint('https://eapi.binance.com', routes=Routes('/eapi/v1/exchangeInfo', l2book='/eapi/v1/depth?symbol={}&limit={}', authentication='/eapi/v1/listenKey'))]

    valid_depths = [10, 20, 50, 100, 1000]
    # m -> minutes; h -> hours; d -> days; w -> weeks; M -> months
    valid_candle_intervals = {'1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w'}
    valid_depth_intervals = {'100ms', '250ms', '500ms'}
    websocket_channels = {
        L2_BOOK: 'depth',
        TRADES: 'trade',
        TICKER: 'ticker',
        CANDLES: 'kline_',
        BALANCES: BALANCES,
        ORDER_INFO: ORDER_INFO
    }

    @classmethod
    def _parse_symbol_data(cls, data: dict) -> Tuple[Dict, Dict]:
        ret = {}
        info = defaultdict(dict)
        for symbol in data['optionSymbols']:
            if symbol.get('status', 'TRADING') != "TRADING":
                continue
            if symbol.get('contractStatus', 'TRADING') != "TRADING":
                continue

            stype = OPTION
            expiration = symbol['symbol'].split("-")[1]
            base = symbol['symbol'].split("-")[0]
            strike_price = symbol['symbol'].split("-")[-2]
            option_type = CALL if symbol['side'] == "CALL" else PUT

            s = Symbol(base, base, type=stype, expiry_date=expiration, option_type=option_type, strike_price=strike_price)
            ret[s.normalized] = symbol['symbol']
            info['tick_size'][s.normalized] = symbol['filters'][0]['tickSize']
            info['instrument_type'][s.normalized] = stype
        return ret, info

    def __init__(self, depth_interval='100ms', **kwargs):
        """
        depth_interval: str
            time between l2_book/delta updates {'100ms', '1000ms'} (different from BINANCE_FUTURES & BINANCE_DELIVERY)
        """
        if depth_interval is not None and depth_interval not in self.valid_depth_intervals:
            raise ValueError(f"Depth interval must be one of {self.valid_depth_intervals}")

        super().__init__(**kwargs)
        self.depth_interval = depth_interval
        self._open_interest_cache = {}
        self._reset()

    async def _snapshot(self, pair: str) -> None:
        max_depth = self.max_depth if self.max_depth else 1000
        if max_depth not in self.valid_depths:
            for d in self.valid_depths:
                if d > max_depth:
                    max_depth = d
                    break

        resp = await self.http_conn.read(self.rest_endpoints[0].route('l2book', self.sandbox).format(pair, max_depth))
        resp = json.loads(resp, parse_float=Decimal)
        timestamp = self.timestamp_normalize(resp['T']) if 'T' in resp else None

        std_pair = self.exchange_symbol_to_std_symbol(pair)
        self.last_update_id[std_pair] = None
        self._l2_book[std_pair] = OrderBook(self.id, std_pair, max_depth=self.max_depth, bids={Decimal(u[0]): Decimal(u[1]) for u in resp['bids']}, asks={Decimal(u[0]): Decimal(u[1]) for u in resp['asks']})
        await self.book_callback(L2_BOOK, self._l2_book[std_pair], time.time(), timestamp=timestamp, raw=resp, sequence_number=self.last_update_id[std_pair])

    async def _book(self, msg: dict, pair: str, timestamp: float):
        """
        {
            "e":"depth",                    // event type 
            "T":1591695934010,              // transaction time 
            "s":"BTC-200630-9000-P",        // Option symbol   
            "b":[                           // Buy order   
            [
                "200",                    // Price
                "3",                      // quantity
            ],
            [
                "101",
                "1"
            ],
            [
                "100",
                "2"
            ]
            ],
            "a":[                           // Sell order   
                [
                    "1000",
                    "89"
                ]
            ]
        }
        """
        exchange_pair = pair
        pair = self.exchange_symbol_to_std_symbol(pair)

        if pair not in self._l2_book:
            # Initialise with book snapshot first
            await self._snapshot(exchange_pair)
            return
        else:
            # Update book with this update
            delta = {BID: [], ASK: []}

            for s, side in (('b', BID), ('a', ASK)):
                for update in msg[s]:
                    price = Decimal(update[0])
                    amount = Decimal(update[1])
                    delta[side].append((price, amount))

                    if amount == 0:
                        if price in self._l2_book[pair].book[side]:
                            del self._l2_book[pair].book[side][price]
                    else:
                        self._l2_book[pair].book[side][price] = amount

            await self.book_callback(L2_BOOK, self._l2_book[pair], timestamp, timestamp=self.timestamp_normalize(msg['T']), raw=msg, delta=delta, sequence_number=self.last_update_id[pair])
    
    async def _trade(self, msg: dict, timestamp: float):
        """
        {
            "e":"trade",                        // event type   
            "E":1591677941092,                  // event time   
            "s":"BTC-200630-9000-P",            // Option trading symbol   
            "t":1,                              // trade ID   
            "p":"1000",                         // price   
            "q":"-2",                           // quantity   
            "b":4611781675939004417,            // buy order ID   
            "a":4611781675939004418,            // sell order ID   
            "T":1591677567872,                  // trade completed time  
            "S":"-1"                            // direction   
        }
        """
        t = Trade(self.id,
                  self.exchange_symbol_to_std_symbol(msg['s']),
                  SELL if msg['S'] < 0 else BUY,
                  Decimal(msg['q']),
                  Decimal(msg['p']),
                  self.timestamp_normalize(msg['T']),
                  id=str(msg['t']),
                  raw=msg)
        await self.callback(TRADES, t, timestamp)
    
    async def message_handler(self, msg: str, conn, timestamp: float):
        msg = json.loads(msg, parse_float=Decimal)

        # Handle account updates from User Data Stream
        if self.requires_authentication:
            msg_type = msg['e']
            if msg_type == 'outboundAccountPosition':
                await self._account_update(msg, timestamp)
            elif msg_type == 'executionReport':
                await self._order_update(msg, timestamp)
            return
        # Combined stream events are wrapped as follows: {"stream":"<streamName>","data":<rawPayload>}
        # streamName is of format <symbol>@<channel>
        pair, _ = msg['stream'].split('@', 1)
        msg = msg['data']
        pair = pair.upper()
        if 'e' in msg:
            if msg['e'] == 'depth':
                await self._book(msg, pair, timestamp)
            elif msg['e'] == 'aggTrade':
                await self._trade(msg, timestamp)
            elif msg['e'] == 'forceOrder':
                await self._liquidations(msg, timestamp)
            elif msg['e'] == 'markPriceUpdate':
                await self._funding(msg, timestamp)
            elif msg['e'] == 'kline':
                await self._candle(msg, timestamp)
            else:
                LOG.warning("%s: Unexpected message received: %s", self.id, msg)

        elif 'A' in msg:
            await self._ticker(msg, timestamp)
        else:
            LOG.warning("%s: Unexpected message received: %s", self.id, msg)

    def _address(self) -> Union[str, Dict]:
        """
        Binance has a 200 pair/stream limit per connection, so we need to break the address
        down into multiple connections if necessary. Because the key is currently not used
        for the address dict, we can just set it to the last used stream, since this will be
        unique.

        The generic connect method supplied by Feed will take care of creating the
        correct connection objects from the addresses.
        """
        if self.requires_authentication:
            listen_key = self._generate_token()
            address = self.address
            address += '/ws/' + listen_key
        else:
            address = self.address
            address += '/stream?streams='
        subs = []

        is_any_private = any(self.is_authenticated_channel(chan) for chan in self.subscription)
        is_any_public = any(not self.is_authenticated_channel(chan) for chan in self.subscription)
        if is_any_private and is_any_public:
            raise ValueError("Private channels should be subscribed in separate feeds vs public channels")
        if all(self.is_authenticated_channel(chan) for chan in self.subscription):
            return address

        for chan in self.subscription:
            normalized_chan = self.exchange_channel_to_std(chan)
            if normalized_chan == OPEN_INTEREST:
                continue
            if self.is_authenticated_channel(normalized_chan):
                continue

            stream = chan
            if normalized_chan == CANDLES:
                stream = f"{chan}{self.candle_interval}"
            elif normalized_chan == L2_BOOK:
                stream = f"{chan}1000@{self.depth_interval}"

            for pair in self.subscription[chan]:
                # for everything but premium index the symbols need to be lowercase.
                if pair.startswith("p"):
                    if normalized_chan != CANDLES:
                        raise ValueError("Premium Index Symbols only allowed on Candle data feed")
                elif pair.endswith("C") or pair.endswith("P"):
                    # Options
                    pass
                else:
                    pair = pair.lower()
                subs.append(f"{pair}@{stream}")

        if 0 < len(subs) < 200:
            return address + '/'.join(subs)
        else:
            def split_list(_list: list, n: int):
                for i in range(0, len(_list), n):
                    yield _list[i:i + n]

            return [address + '/'.join(chunk) for chunk in split_list(subs, 200)]