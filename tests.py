import time
from typing import List, Dict, Union

import requests
import json
import os
from pydantic import BaseModel
from prometheus_client import Gauge, start_wsgi_server
from datetime import datetime
from decimal import Decimal
from urllib.parse import urljoin
from operator import itemgetter

BASE_API_URL = "https://api.binance.com/api/v3/"
REQUEST_SESSION = requests.Session()

CACHE = True


class Price(BaseModel):
    bid: Decimal
    ask: Decimal


class Symbol:
    name: str

    def __init__(self, name, price, volume, notional_value, spread, spread_abs_delta=0):
        self._name = name
        self._price = price
        self._volume = volume
        self._notional_value = notional_value
        self._spread = spread
        self._spread_abs_delta = spread_abs_delta

    def price(self, update=False):
        if update:
            self.update_price()
        return self._price

    def update_price(self):
        self._price = safe_request('depth', params={'symbol': self._name, 'limit': 1})

    def volume(self):
        pass

    def notional_value(self):
        pass

    def spread(self):
        pass

    def spread_abs_delta(self):
        pass


def safe_request(api, params: Dict = None, headers: Dict = None) -> Union[Dict, List]:
    """
    A wrapper for requests.get that handles errors and uses the urljoin to concatenate URI safely.
    """
    url = urljoin(BASE_API_URL, api)
    response = REQUEST_SESSION.get(url, params=params, headers=headers)
    try:
        response.raise_for_status()
    except Exception as e:
        raise f"Error while requesting {url}: {e}"
    return response.json()


def cached(filename: str, ttl: int = 60) -> callable:
    """
    A decorator that caches the result of a function call to json file.
    """

    def cache_wrapper(func):
        def wrapper(*args, **kwargs):
            if os.path.exists(filename) and CACHE:
                # check the modification time of the file if it exists, if it is not older than ttl, then use it
                file_mtime = os.path.getmtime(filename)
                current_time = time.time()
                if file_mtime + ttl >= current_time:
                    with open(filename) as f:
                        return json.load(f)

            result = func(*args, **kwargs)
            with open(filename, 'w') as f:
                json.dump(result, f)
            return result

        return wrapper

    return cache_wrapper


@cached('/tmp/exchange_info.json')
def get_exchange_info() -> Dict:
    """
    Returns the exchange info by every symbol.
    https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md#exchange-information
    """
    return safe_request('exchangeInfo')


@cached('/tmp/ticker.json')
def get_24h_ticker() -> List:
    """
    Returns the 24h ticker by every symbol.
    https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md#24hr-ticker-price-change-statistics
    """
    return safe_request('ticker/24hr')


# the function is using cache decorator, but it is changing the filename of the cache file with symbol name
def get_order_book(symbol: str) -> Dict:
    """
    Returns the order book by symbol.
    https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md#order-book
    """

    # another way to use the "cached" decorator by changing the filename

    # use limit=500 because this is the max limit for the order book with the lowest weight
    # that match the task description (top 200 bids and asks)
    return cached(f'/tmp/order_book_{symbol}.json')(safe_request)('depth', params={'symbol': symbol, 'limit': 500})


# use the ttl=10 to follow the task description (Every 10 seconds print the result of Q4)
@cached('/tmp/book_ticker.json', ttl=10)
def get_book_ticker() -> List:
    """
    Returns the book ticker by every symbol.
    https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md#symbol-order-book-ticker
    """
    return safe_request('ticker/bookTicker')


def get_symbols_by_quote_asset(quote_asset: str) -> List:
    """
    Get filtered symbols by quote asset.
    """
    exchange_info = get_exchange_info()
    return list(filter(lambda x: x['quoteAsset'] == quote_asset, exchange_info["symbols"]))


def get_top_symbols_by_quote_asset_by(
        quote_asset: str, top: int = 5, top_by: str = 'volume',
        cast_top_by_to: callable = Decimal, sort_order_reverse: bool = True
) -> Dict:
    """
    Get top n symbols by quote asset by given property.
    The full list of properties is available here:
    https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md#24hr-ticker-price-change-statistics
    """
    symbols_names = [(x['symbol']) for x in get_symbols_by_quote_asset(quote_asset)]
    symbols_with_top_by_value = []
    for ticker in get_24h_ticker():
        if ticker['symbol'] not in symbols_names:
            continue
        symbols_with_top_by_value.append(
            {
                "symbol": ticker['symbol'],
                # cast the value to the given type (by default cast "volume" to Decimal)
                # the Decimal type is more precise than floats (and we count money)
                "value": cast_top_by_to(ticker[top_by])
            }
        )

    # sort the list and get the top n symbols
    casted_filtered_top_symbols = sorted(
        symbols_with_top_by_value, key=itemgetter("value"), reverse=sort_order_reverse
    )[:top]
    # return data in the same format
    return {"data": casted_filtered_top_symbols}


def get_top_symbols_by_volume_in_24h(quote_asset: str) -> Dict:
    """
    Get top 5 symbols by volume in 24h.
    """
    return get_top_symbols_by_quote_asset_by(quote_asset, top=5, top_by='volume', sort_order_reverse=True)


def get_top_total_notional_value_by_symbols(symbols: List) -> Dict:
    """
    Get top symbols by total notional value in 24h.
    """
    notional_value = []
    for symbol in symbols:
        order_book = get_order_book(symbol)

        # The order book returns a list that contains PRICE as '0' and QTY as '1' indexes
        # cast the price and qty to Decimal to make them more precise
        cast_orders_tuple_to_decimal = lambda x: (Decimal(x[0]), Decimal(x[1]))
        asks = list(map(cast_orders_tuple_to_decimal, order_book['asks']))
        bids = list(map(cast_orders_tuple_to_decimal, order_book['asks']))

        # Sort by price (descending) and get the top 200 bids and asks
        sort_orders_by_price = lambda x: sorted(x, key=itemgetter(0), reverse=True)[:200]
        asks = sort_orders_by_price(asks)
        bids = sort_orders_by_price(asks)

        # count notional value by multiplying the price and quantity
        count_notional_value = lambda order: sum([x[0] * x[1] for x in order])
        notional_value.append(
            {'symbol': symbol,
             'type': 'bid',
             'value': count_notional_value(bids)
             }
        )
        notional_value.append(
            {'symbol': symbol,
             'type': 'ask',
             'value': count_notional_value(asks)
             }
        )

    return {"data": notional_value}


def get_price_spread_by_symbols(symbols: List) -> Dict:
    """
    Get price spread by symbols.
    """
    filtered_symbols_book_ticker = list(filter(lambda x: x['symbol'] in symbols, get_book_ticker()))
    return {"data": [{"symbol": x['symbol'], "value": Decimal(x['askPrice']) - Decimal(x['bidPrice'])} for x in
                     filtered_symbols_book_ticker]}


def print_formatted_data(data: Dict, notional: bool = False, spread_delta: Decimal = None) -> None:
    aligned_symbol_str = "{symbol: <9}"
    formatted_decimal_str = "{value:f}"
    if notional:
        formatted_decimal_str = " ".join(["{type} -", formatted_decimal_str])
    if spread_delta:
        formatted_decimal_str = " ".join(["spread -", formatted_decimal_str, ": delta - {delta:f}"])

    for symbol in data['data']:
        print(aligned_symbol_str.format(
            symbol=symbol['symbol']+':'),
            formatted_decimal_str.format(
                value=symbol['value'],
                type=symbol.get('type', None),
                delta=spread_delta
            )
        )


def main():
    print_formatted_symbols_data = lambda x: [print(f"{symbol['symbol'] + ':': <9} {symbol['value']:f}") for symbol in
                                              x['data']]
    # Top 5 BTC symbols with the highest volume in 24h
    top_5_btc_symbols_by_volume = get_top_symbols_by_volume_in_24h('BTC')
    print("Top 5 BTC symbols with the highest volume in 24h:")
    print_formatted_symbols_data(top_5_btc_symbols_by_volume)
    print_formatted_data(top_5_btc_symbols_by_volume)

    # Top 5 USDT symbols with the highest volume in 24h
    top_5_usdt_symbols_by_volume = get_top_symbols_by_volume_in_24h('USDT')
    print("\nTop 5 USDT symbols with the highest volume in 24h:")
    print_formatted_symbols_data(top_5_usdt_symbols_by_volume)
    print_formatted_data(top_5_usdt_symbols_by_volume)

    # Notional value for the top 5 BTC symbols
    symbols_for_notional_value = [x["symbol"] for x in top_5_btc_symbols_by_volume['data']]
    notional_values_by_top_5_btc_symbols = get_top_total_notional_value_by_symbols(symbols_for_notional_value)
    print("\nNotional value for the top 5 BTC symbols:")
    [print(f"{symbol['symbol'] + ':': <9} {symbol['type']} - {symbol['value']:f}") for symbol in
     notional_values_by_top_5_btc_symbols['data']]
    print_formatted_data(notional_values_by_top_5_btc_symbols, notional=True)

    # The price spread for each of the top 5 USDT symbols
    symbols_for_spread = [x["symbol"] for x in top_5_usdt_symbols_by_volume['data']]
    price_spread_by_top5_usdt_symbols = get_price_spread_by_symbols(symbols_for_spread)
    print("\nThe price spread for each of the top 5 USDT symbols:")
    [print(f"{symbol['symbol'] + ':': <9} {symbol['value']:f}") for symbol in price_spread_by_top5_usdt_symbols['data']]
    print_formatted_data(price_spread_by_top5_usdt_symbols)

    prom_spread = Gauge('price_spread', 'Price spread for each symbol', ['symbol'])
    spread_delta = Gauge('spread_delta', 'Absolute spread delta for each symbol', ['symbol'])
    start_wsgi_server(8081)

    # Every 10 seconds print the price spread and the absolute delta from the previous value for each symbol
    while True:
        new_price_spread_by_top5_usdt_symbols = get_price_spread_by_symbols(symbols_for_spread)
        time.sleep(10)

        print("\nThe price spread for each of the top 5 USDT symbols and delta:")
        for i in range(len(new_price_spread_by_top5_usdt_symbols['data'])):
            symbol = new_price_spread_by_top5_usdt_symbols['data'][i]
            delta = abs(
                new_price_spread_by_top5_usdt_symbols['data'][i]['value'] -
                price_spread_by_top5_usdt_symbols['data'][i]['value']
            )
            prom_spread.labels(symbol['symbol']).set(float(symbol['value']))
            spread_delta.labels(symbol['symbol']).set(float(delta))

            print(f"{symbol['symbol'] + ':': <9}: spread - {symbol['value']:f}, delta - {delta:f}")
            print_formatted_data(new_price_spread_by_top5_usdt_symbols, spread_delta=delta)
        price_spread_by_top5_usdt_symbols = new_price_spread_by_top5_usdt_symbols


if __name__ == '__main__':
    main()
