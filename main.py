#!/usr/bin/env python3
import os
import pickle
import time
from decimal import Decimal
from operator import itemgetter
from typing import Dict, List, Union
from urllib.parse import urljoin

import requests
from prometheus_client import Gauge, start_wsgi_server

# Base binance API URL
BASE_API_URL = "https://api.binance.com/api/v3/"

# Reusable session for requests
REQUEST_SESSION = requests.Session()

# The caching can be turned on/off by environment variable
CACHE = os.getenv("CACHE", True)
CACHE_TTL = os.getenv("CACHE_TTL", 60)

# The default timeout for the 5 task, used for tests
PRINT_DELTA_TIMEOUT = os.getenv("PRINT_DELTA_TIMEOUT", 10)

# The port to start the Prometheus server on
PORT = os.getenv("PORT", 8080)


# the decorator I used to not get a ban from the API during the tests and debugging
def cached(func: callable = None, ttl: int = CACHE_TTL) -> callable:
    """
    A decorator that caches the result of a function call to pickle file.
    The function can be easily replaced with cachetools library (it has TTLCache)
    it will work faster, but it more interesting try to write it by myself :)
    """

    def _decorate(func):
        def wrapper(*args, **kwargs):
            filename = f"/tmp/{func.__name__}.pickle"
            if os.path.exists(filename) and CACHE:
                # check the modification time of the file if it exists, if it is not older than ttl, then use it
                file_mtime = os.path.getmtime(filename)
                current_time = time.time()
                if file_mtime + ttl >= current_time:
                    with open(filename, "rb") as f:
                        return pickle.load(f)

            result = func(*args, **kwargs)
            with open(filename, "wb") as f:
                pickle.dump(result, f)
            return result

        return wrapper

    # The condition needs here to use the decorator with an optional argument
    if func:
        return _decorate(func)

    return _decorate


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


@cached
def get_exchange_info() -> Dict:
    """
    Returns the exchange info by every symbol.
    https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md#exchange-information
    """
    return safe_request("exchangeInfo")


@cached
def get_24h_ticker() -> List:
    """
    Returns the 24h ticker by every symbol.
    https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md#24hr-ticker-price-change-statistics
    """
    return safe_request("ticker/24hr")


@cached
def get_order_book(symbol: str) -> Dict:
    """
    Returns the order book by symbol.
    https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md#order-book
    """

    # use limit=500 because this is the max limit for the order book with the lowest weight
    # that match the task description (top 200 bids and asks)
    return safe_request("depth", params={"symbol": symbol, "limit": 500})


# use the ttl=10 to follow the task description (Every 10 seconds print the result of Q4)
@cached(ttl=10)
def get_book_ticker() -> List:
    """
    Returns the book ticker by every symbol.
    https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md#symbol-order-book-ticker
    """
    return safe_request("ticker/bookTicker")


def get_symbols_by_quote_asset(quote_asset: str) -> List:
    """
    Get filtered symbols by quote asset.
    """
    exchange_info = get_exchange_info()
    return list(
        filter(lambda x: x["quoteAsset"] == quote_asset, exchange_info["symbols"])
    )


def get_top_symbols_by_quote_asset_by(
    quote_asset: str,
    top: int = 5,
    top_by: str = "volume",
    cast_top_by_to: callable = Decimal,
    sort_order_reverse: bool = True,
) -> Dict:
    """
    Get top n symbols by quote asset by given property.
    The full list of properties is available here:
    https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md#24hr-ticker-price-change-statistics
    """
    symbols_names = [(x["symbol"]) for x in get_symbols_by_quote_asset(quote_asset)]
    symbols_with_top_by_value = []
    for ticker in get_24h_ticker():
        if ticker["symbol"] not in symbols_names:
            continue
        symbols_with_top_by_value.append(
            {
                "symbol": ticker["symbol"],
                # cast the value to the given type (by default cast "volume" to Decimal)
                # the Decimal type is more precise than floats (and we count money)
                "value": cast_top_by_to(ticker[top_by]),
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
    return get_top_symbols_by_quote_asset_by(
        quote_asset, top=5, top_by="volume", sort_order_reverse=True
    )


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
        asks = list(map(cast_orders_tuple_to_decimal, order_book["asks"]))
        bids = list(map(cast_orders_tuple_to_decimal, order_book["asks"]))

        # Sort by price (descending) and get the top 200 bids and asks
        sort_orders_by_price = lambda x: sorted(x, key=itemgetter(0), reverse=True)[
            :200
        ]
        asks = sort_orders_by_price(asks)
        bids = sort_orders_by_price(asks)

        # count notional value by multiplying the price and quantity
        count_notional_value = lambda order: sum([x[0] * x[1] for x in order])
        notional_value.append(
            {"symbol": symbol, "type": "bid", "value": count_notional_value(bids)}
        )
        notional_value.append(
            {"symbol": symbol, "type": "ask", "value": count_notional_value(asks)}
        )

    return {"data": notional_value}


def get_price_spread_by_symbols(symbols: List) -> Dict:
    """
    Get price spread by symbols.
    """
    filtered_symbols_book_ticker = list(
        filter(lambda x: x["symbol"] in symbols, get_book_ticker())
    )
    return {
        "data": [
            {
                "symbol": x["symbol"],
                "value": Decimal(x["askPrice"]) - Decimal(x["bidPrice"]),
            }
            for x in filtered_symbols_book_ticker
        ]
    }


def print_formatted_data(
    data: Dict, notional: bool = False, spread_delta: bool = False
) -> None:
    """
    Just print given data in a nice format with symbol name alignment.
    """
    # set minimum width for symbol name to 9 characters, to align the data
    aligned_symbol_str = "{symbol: <9}"
    # print decimal with all of zeros
    formatted_decimal_str = "{value:f}"

    if notional:
        formatted_decimal_str = " ".join(["{type} -", formatted_decimal_str])
    if spread_delta:
        formatted_decimal_str = " ".join(
            ["spread -", formatted_decimal_str, ": delta - {delta:f}"]
        )

    for symbol in data["data"]:
        print(
            aligned_symbol_str.format(symbol=symbol["symbol"] + ":"),
            formatted_decimal_str.format(
                value=symbol["value"],
                type=symbol.get("type", None),
                delta=symbol.get("delta", None),
            ),
        )


def main():
    # Top 5 BTC symbols with the highest volume in 24h
    top_5_btc_symbols_by_volume = get_top_symbols_by_volume_in_24h("BTC")
    print("Top 5 BTC symbols with the highest volume in 24h:")
    print_formatted_data(top_5_btc_symbols_by_volume)

    # Top 5 USDT symbols with the highest volume in 24h
    top_5_usdt_symbols_by_volume = get_top_symbols_by_volume_in_24h("USDT")
    print("\nTop 5 USDT symbols with the highest volume in 24h:")
    print_formatted_data(top_5_usdt_symbols_by_volume)

    # Notional value for the top 5 BTC symbols
    symbols_for_notional_value = [
        x["symbol"] for x in top_5_btc_symbols_by_volume["data"]
    ]
    notional_values_by_top_5_btc_symbols = get_top_total_notional_value_by_symbols(
        symbols_for_notional_value
    )
    print("\nNotional value for the top 5 BTC symbols:")
    print_formatted_data(notional_values_by_top_5_btc_symbols, notional=True)

    # The price spread for each of the top 5 USDT symbols
    symbols_for_spread = [x["symbol"] for x in top_5_usdt_symbols_by_volume["data"]]
    price_spread_by_top5_usdt_symbols = get_price_spread_by_symbols(symbols_for_spread)
    print("\nThe price spread for each of the top 5 USDT symbols:")
    print_formatted_data(price_spread_by_top5_usdt_symbols)

    # Register the metrics in prometheus library
    prom_spread = Gauge("price_spread", "Price spread for each symbol", ["symbol"])
    spread_delta = Gauge(
        "spread_delta", "Absolute spread delta for each symbol", ["symbol"]
    )

    # Run the web server only when the application is ready to give metrics
    start_wsgi_server(PORT)

    # Every 10 seconds print the price spread and the absolute delta from the previous value for each symbol
    while True:
        new_price_spread_by_top5_usdt_symbols = get_price_spread_by_symbols(
            symbols_for_spread
        )
        time.sleep(10)

        for i in range(len(new_price_spread_by_top5_usdt_symbols["data"])):
            delta = abs(
                new_price_spread_by_top5_usdt_symbols["data"][i]["value"]
                - price_spread_by_top5_usdt_symbols["data"][i]["value"]
            )
            new_price_spread_by_top5_usdt_symbols["data"][i]["delta"] = delta

            symbol = new_price_spread_by_top5_usdt_symbols["data"][i]
            prom_spread.labels(symbol["symbol"]).set(float(symbol["value"]))
            spread_delta.labels(symbol["symbol"]).set(float(delta))

        print("\nThe price spread for each of the top 5 USDT symbols and delta:")
        print_formatted_data(new_price_spread_by_top5_usdt_symbols, spread_delta=True)
        price_spread_by_top5_usdt_symbols = new_price_spread_by_top5_usdt_symbols


if __name__ == "__main__":
    main()
