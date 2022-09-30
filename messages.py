import json
from datetime import datetime

from google.protobuf.text_format import MessageToString, PrintMessage

import kraken_msg_pb2


def process_ticker_message(message: str) -> kraken_msg_pb2.Ticker:
    """
    Function that takes a message from the Kraken websocket and converts it to a
    protobuf message.

    :param message: The message received from the websocket
    :return: A protobuf ticker message
    """

    message = json.loads(message)
    if not isinstance(message, dict):
        ticker = kraken_msg_pb2.Ticker()

        ticker.type = message[2]
        ticker.pair = message[3]
        ticker.price = float(message[1]["c"][0])

        # TODO: remove print() after zmq implementation
        print(MessageToString(ticker))
        return ticker


def process_spread_message(message: str) -> kraken_msg_pb2.Spread:
    """
    Function that takes a message from the Kraken websocket and converts it to a
    protobuf message.

    :param message: The message received from the websocket
    :return: A protobuf spread message
    """

    message = json.loads(message)
    if not isinstance(message, dict):
        spread_time = datetime.fromtimestamp(float(message[1][2]))

        spread = kraken_msg_pb2.Spread()

        spread.type = message[2]
        spread.pair = message[3]
        spread.bid = float(message[1][0])
        spread.ask = float(message[1][1])
        spread.time = spread_time.strftime("%Y-%m-%d %H:%M:%S")
        spread.bid_volume = float(message[1][3])
        spread.ask_volume = float(message[1][4])

        # TODO: remove print() after zmq implementation
        print(MessageToString(spread))

        return spread


def process_ohlc_message(message: str) -> kraken_msg_pb2.OHLC:
    """
    Function that takes a message from the Kraken websocket and converts it to a
    protobuf message, and prints it to the console

    :param message: The message received from the websocket
    :return: A protobuf ohlc message
    """

    message = json.loads(message)
    if not isinstance(message, dict):

        ohlc_begin_time = datetime.fromtimestamp(float(message[1][0]))
        ohlc_end_time = datetime.fromtimestamp(float(message[1][1]))
        ohlc = kraken_msg_pb2.OHLC()

        ohlc.type = message[2]
        ohlc.pair = message[3]
        ohlc.begin = ohlc_begin_time.strftime("%Y-%m-%d %H:%M:%S")
        ohlc.end = ohlc_end_time.strftime("%Y-%m-%d %H:%M:%S")
        ohlc.open = float(message[1][2])
        ohlc.high = float(message[1][3])
        ohlc.low = float(message[1][4])
        ohlc.close = float(message[1][5])
        ohlc.vwap = float(message[1][6])
        ohlc.volume = float(message[1][7])
        ohlc.num = int(message[1][8])

        # TODO: remove print() after zmq implementation
        print(MessageToString(ohlc))

        return ohlc


def process_trade_message(message: str) -> kraken_msg_pb2.Trade:
    """
    Function that takes a message from the Kraken websocket and converts it to a
    protobuf message, and prints it to the console

    :param message: The message received from the websocket
    :return: A protobuf trade message
    """

    message = json.loads(message)
    if not isinstance(message, dict):
        trade_time = datetime.fromtimestamp(float(message[1][0][2]))

        trade = kraken_msg_pb2.Trade()

        trade.type = message[2]
        trade.pair = message[3]
        trade.price = float(message[1][0][0])
        trade.volume = float(message[1][0][1])
        trade.time = trade_time.strftime("%Y-%m-%d %H:%M:%S")
        trade.side = message[1][0][3]
        trade.order_type = message[1][0][4]
        trade.misc = message[1][0][5]

        # TODO: remove print() after zmq implementation
        print(MessageToString(trade))

        return trade


def print_book_message(message, pairs):
    """
    Function prints Order book feed for a currency pair from Kraken WebSockets
    API in a human-readable format.It takes one argument, which is the JSON
    string that was sent by the server.The function then parses through it and
    prints out all of its contents.

    Args:
        message: Pass the message to the function.
    """

    message = json.loads(message)
    order_book = {}
    for pair in pairs:
        order_book[pair] = {"bid": {}, "ask": {}}

    if not isinstance(message, dict):

        data = message[1]
        pair = message[-1]

        if "as" in data or "bs" in data:
            ask, bid = {}, {}

            # construct new ask and bid for our orderbook that we maintain
            for record in data["as"]:
                ask[record[0]] = {"volume": record[1]}

            for record in data["bs"]:
                bid[record[0]] = {"volume": record[1]}

            order_book[pair]["ask"] = ask
            order_book[pair]["bid"] = bid

        if "a" in data:
            if pair not in order_book:
                order_book[pair] = {"bid": {}, "ask": {}}

            for record in data["a"]:
                # skip "republish" update messages
                if len(record) == 4 and record[3] == "r":
                    continue

                price = record[0]
                volume = record[1]

                # if volume is 0, that means we need to delete the record
                if int(float(volume)) == 0 and price in order_book[pair]["ask"]:
                    del order_book[pair]["ask"][price]

                if not order_book[pair]["ask"].get(price):
                    # insert new record and remove the last one out of scope
                    prices_sorted = sorted(order_book[pair]["ask"].keys())
                    if prices_sorted:
                        min_price = prices_sorted[-1]
                        del order_book[pair]["ask"][min_price]

                    order_book[pair]["ask"][price] = {"volume": volume}
                else:
                    # update the existing record
                    order_book[pair]["ask"][price]["volume"] = volume

        if "b" in data:
            if pair not in order_book:
                order_book[pair] = {"bid": {}, "ask": {}}

            for record in data["b"]:
                # skip "republish" update messages
                if len(record) == 4 and record[3] == "r":
                    continue

                price = record[0]
                volume = record[1]

                # if volume is 0, that means we need to delete the record
                if int(float(volume)) == 0 and price in order_book[pair]["bid"]:
                    del order_book[pair]["bid"][price]

                if not order_book[pair]["bid"].get(price):
                    # insert new record and remove the last one out of scope
                    prices_sorted = sorted(order_book[pair]["bid"].keys())
                    if prices_sorted:
                        min_price = prices_sorted[-1]
                        del order_book[pair]["bid"][min_price]

                    order_book[pair]["bid"][price] = {"volume": volume}
                else:
                    # update the existing record
                    order_book[pair]["bid"][price]["volume"] = volume

    print(order_book)
