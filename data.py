import json
from typing import List

import websocket
import zmq
import logging

import settings
from messages import (
    process_spread_message,
    process_ohlc_message,
    process_ticker_message,
    process_trade_message,
    print_book_message,
)


def stream_spread_data(pairs: List[str], zmq_context: zmq.Context):
    """
    Subscribes to the Kraken WebSockets API and streams Spread data for a given
    currency pair/s.
    """

    pairs = json.dumps([pair.upper() for pair in pairs])

    # Create ZeroMQ PUSH Socket and connect it to IPC url
    # This PUSH Socket will push data from thread to a single PULL socket
    zmq_push_socket = zmq_context.socket(zmq.PUSH)
    zmq_push_socket.connect(settings.ZMQ_PUSH_PULL_IPC_URL)

    def on_message(ws, message):
        topic, processed_message = process_spread_message(message)

        if processed_message is not None:
            zmq_push_socket.send_multipart(
                [
                    topic.encode(),
                    processed_message.SerializeToString(),
                ],
            )

    def on_error(ws, error):
        logging.error(error)

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


def stream_ohlc_data(pairs: List[str], zmq_context: zmq.Context, interval=1):
    """
    Subscribes to the Kraken WebSockets API and streams OHLC data for a given
    currency pair/s.
    """

    if interval not in [1, 60, 1440]:
        print("Interval must be in: 1, 60, 1440")
        return

    pairs = json.dumps([pair.upper() for pair in pairs])

    # Create ZeroMQ PUSH Socket and connect it to IPC url
    # This PUSH Socket will push data from thread to a single PULL socket
    zmq_push_socket = zmq_context.socket(zmq.PUSH)
    zmq_push_socket.connect(settings.ZMQ_PUSH_PULL_IPC_URL)

    def on_message(ws, message):
        topic, processed_message = process_ohlc_message(message)

        if processed_message is not None:

            if interval == 1:
                topic = topic + "Minutely"
            if interval == 60:
                topic = topic + "Hourly"
            if interval == 1440:
                topic = topic + "Daily"

            zmq_push_socket.send_multipart(
                [
                    topic.encode(),
                    processed_message.SerializeToString(),
                ],
            )

    def on_error(ws, error):
        logging.error(error)

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


def stream_ticker_data(pairs: List[str], zmq_context: zmq.Context):
    """
    Subscribes to the Kraken WebSockets API and streams Ticker data for a given
    currency pair/s.
    """

    pairs = json.dumps([pair.upper() for pair in pairs])

    # Create ZeroMQ PUSH Socket and connect it to IPC url
    # This PUSH Socket will push data from thread to a single PULL socket
    zmq_push_socket = zmq_context.socket(zmq.PUSH)
    zmq_push_socket.connect(settings.ZMQ_PUSH_PULL_IPC_URL)

    def on_message(ws, message):
        topic, processed_message = process_ticker_message(message)

        if processed_message is not None:
            zmq_push_socket.send_multipart(
                [
                    topic.encode(),
                    processed_message.SerializeToString(),
                ],
            )

    def on_error(ws, error):
        logging.error(error)

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


def stream_trade_data(pairs: List[str], zmq_context: zmq.Context):
    """
    Subscribes to the Kraken WebSockets API and streams Trade data for a given
    currency pair/s.
    """

    pairs = json.dumps([pair.upper() for pair in pairs])

    # Create ZeroMQ PUSH Socket and connect it to IPC url
    # This PUSH Socket will push data from thread to a single PULL socket
    zmq_push_socket = zmq_context.socket(zmq.PUSH)
    zmq_push_socket.connect(settings.ZMQ_PUSH_PULL_IPC_URL)

    def on_message(ws, message):
        topic, processed_message = process_trade_message(message)

        if processed_message is not None:
            zmq_push_socket.send_multipart(
                [
                    topic.encode(),
                    processed_message.SerializeToString(),
                ],
            )

    def on_error(ws, error):
        logging.error(error)

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
        print_book_message(
            message,
            pairs=[
                "XBT/USD",
            ],
        )

    def on_error(ws, error):
        # TODO replace with logger.error()
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
