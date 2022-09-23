import json
import websocket

from messages import (
    process_spread_message,
    process_ohlc_message,
    process_ticker_message,
    process_trade_message,
    process_book_message,
)


def stream_spread_data(*args: str):
    """
    Function subscribes to the Kraken WebSockets API and
    streams Spread data for a given currency pair/s. The function prints out the
    current bid/ask spread for each pair, as well as the time at which it was
    recorded.

    Args:
        *args:str: Pass a variable number of currency pairs to the function in
        "XXX/YYY" format.
    """

    pairs = json.dumps([arg.upper() for arg in args])

    def on_message(ws, message):
        process_spread_message(message)

    def on_error(ws, error):
        print(error)

    def on_open(ws):
        ws.send(
            f'{{"event":"subscribe", "subscription":{{"name":"spread"}}, "pair":{pairs}}}'
        )

    ws = websocket.WebSocketApp(
        "wss://ws.kraken.com/",
        on_message=on_message,
        on_error=on_error,
        on_open=on_open,
    )
    ws.run_forever()


def stream_ohlc_data(*args: str, interval=1):
    """
    Function subscribes to the Kraken WebSockets API and streams OHLC data for
    a given currency pair/s.

    Args:
        *args:str: Pass a variable number of currency pairs to the function in
        "XXX/YYY" format.
        :param interval: The time interval in minutes between each data point,
        defaults to 1 (optional).
    """

    if interval not in [1, 60, 1440]:
        print("Interval must be in: 1, 60, 1440")
        return

    pairs = json.dumps([arg.upper() for arg in args])

    def on_message(ws, message):
        process_ohlc_message(message)

    def on_error(ws, error):
        print(error)

    def on_open(ws):
        ws.send(
            f'{{"event":"subscribe", "subscription":{{"name":"ohlc","interval": {interval}}}, "pair":{pairs}}}'
        )

    ws = websocket.WebSocketApp(
        "wss://ws.kraken.com/",
        on_message=on_message,
        on_error=on_error,
        on_open=on_open,
    )
    ws.run_forever()


def stream_ticker_data(*args: str):
    """
    Function subscribes to the Kraken WebSockets API and streams Ticker data for
    a given currency pair/s.

    Args:
        *args:str: Pass a variable number of currency pairs to the function in
        "XXX/YYY" format.
    """

    pairs = json.dumps([arg.upper() for arg in args])

    def on_message(ws, message):
        process_ticker_message(message)

    def on_error(ws, error):
        print(error)

    def on_open(ws):
        ws.send(
            f'{{"event":"subscribe", "subscription":{{"name":"ticker"}}, "pair":{pairs}}}'
        )

    ws = websocket.WebSocketApp(
        "wss://ws.kraken.com/",
        on_message=on_message,
        on_error=on_error,
        on_open=on_open,
    )
    ws.run_forever()


def stream_trade_data(*args: str):
    """
    Function subscribes to the Kraken WebSockets API and streams Trade data for
    a given currency pair/s.

    Args:
        *args:str: Pass a variable number of currency pairs to the function in
        "XXX/YYY" format.
    """

    pairs = json.dumps([arg.upper() for arg in args])

    def on_message(ws, message):
        process_trade_message(message)

    def on_error(ws, error):
        print(error)

    def on_open(ws):
        ws.send(
            f'{{"event":"subscribe", "subscription":{{"name":"trade"}}, "pair":{pairs}}}'
        )

    ws = websocket.WebSocketApp(
        "wss://ws.kraken.com/",
        on_message=on_message,
        on_error=on_error,
        on_open=on_open,
    )
    ws.run_forever()


def stream_book_data(*args: str):
    """
    Function subscribes to the Kraken WebSockets API and streams Order Book data
    for a given currency pair/s.

    Args:
        *args:str: Pass a variable number of currency pairs to the function in
        "XXX/YYY" format.
    """

    pairs = [arg.upper() for arg in args]

    def on_message(ws, message):
        process_book_message(
            message,
            pairs=[
                "XBT/USD",
            ],
        )

    def on_error(ws, error):
        print(error)

    def on_open(ws):
        ws.send(
            f'{{"event":"subscribe", "subscription":{{"name":"book"}}, "pair":{json.dumps(pairs)}}} '
        )

    ws = websocket.WebSocketApp(
        "wss://ws.kraken.com/",
        on_message=on_message,
        on_error=on_error,
        on_open=on_open,
    )
    ws.run_forever()
