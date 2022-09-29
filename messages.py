import json
from datetime import datetime

from google.protobuf.text_format import MessageToString, PrintMessage

import kraken_msg_pb2


def process_ticker_message(message):
    """
    Function takes a message from the Kraken websocket and converts it to a
    protobuf message.

    :param message: The message received from the websocket
    :return: A protobuf ticker message
    """

    message = json.loads(message)
    if not isinstance(message, dict):
        ticker = kraken_msg_pb2.Ticker()

        ticker.ticker = message[3]
        ticker.price = float(message[1]["c"][0])

        # TODO: remove print() after zmq implementation
        print(MessageToString(ticker))

        return ticker


def process_spread_message(message):
    """
    Function takes a message from the Kraken websocket and converts it to a
    protobuf message.

    :param message: The message received from the websocket
    :return: A protobuf spread message
    """

    message = json.loads(message)
    if not isinstance(message, dict):
        spread_time = datetime.fromtimestamp(float(message[1][2]))

        spread = kraken_msg_pb2.Spread()

        spread.spread = message[3]
        spread.bid = float(message[1][0])
        spread.ask = float(message[1][1])
        spread.time = spread_time.strftime("%Y-%m-%d %H:%M:%S")
        spread.bid_volume = float(message[1][3])
        spread.ask_volume = float(message[1][4])

        # TODO: remove print() after zmq implementation
        print(MessageToString(spread))

        return spread


def process_ohlc_message(message):
    """
    Function takes a message from the Kraken websocket, converts it to a
    protobuf message, and prints it to the console

    :param message: The message received from the websocket
    :return: A protobuf ohlc message
    """

    message = json.loads(message)
    if not isinstance(message, dict):

        ohlc_begin_time = datetime.fromtimestamp(float(message[1][0]))
        ohlc_end_time = datetime.fromtimestamp(float(message[1][1]))
        ohlc = kraken_msg_pb2.OHLC()

        ohlc.ohlc = message[3]
        ohlc.begin = ohlc_begin_time.strftime("%Y-%m-%d %H:%M:%S")
        ohlc.end = ohlc_end_time.strftime("%Y-%m-%d %H:%M:%S")
        ohlc.open = float(message[1][2])
        ohlc.high = float(message[1][3])
        ohlc.low = float(message[1][4])
        ohlc.close = float(message[1][5])
        ohlc.vwap = float(message[1][6])
        ohlc.volume = float(message[1][7])
        ohlc.number_of_trades = int(message[1][8])

        # TODO: remove print() after zmq implementation
        print(MessageToString(ohlc))

        return ohlc


def process_trade_message(message):
    """
    Function takes a message from the Kraken websocket, converts it to a
    protobuf message, and prints it to the console

    :param message: The message received from the websocket
    :return: A protobuf trade message
    """

    message = json.loads(message)
    if not isinstance(message, dict):
        trade_time = datetime.fromtimestamp(float(message[1][0][2]))

        trade = kraken_msg_pb2.Trade()
        trade.trade = message[3]
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


def print_spread_message(message):
    """
    Function prints Spread feed for a currency pair from Kraken WebSockets API
    in a human-readable format.
    It takes one argument, which is the JSON string that was sent by the server.
    The function then parses through it and prints out all of its contents.

    Args:
        message: Pass the message to the function.
    """

    message = json.loads(message)
    if not isinstance(message, dict):
        spread_time = datetime.fromtimestamp(float(message[1][2]))

        print(
            {
                f"Spread {message[3]}": {
                    "Bid price": message[1][0],
                    "Ask price": message[1][1],
                    "Time": spread_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "Bid volume": message[1][3],
                    "Ask volume": message[1][4],
                }
            }
        )


def print_ohlc_message(message):
    """
    Function prints Open High Low Close (Candle) feed for a currency pair
    and interval period from Kraken WebSockets API in a human-readable format.
    It takes one argument, which is the JSON string that was sent by the server.
    The function then parses through it and prints out all of its contents.

    Args:
        message: Pass the message to the function.
    """

    message = json.loads(message)
    if not isinstance(message, dict):
        ohlc_time = datetime.fromtimestamp(float(message[1][0]))
        ohlc_end_time = datetime.fromtimestamp(float(message[1][1]))

        print(
            {
                f"OHLC {message[3]}": {
                    "Begin time of interval": ohlc_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "End time of interval": ohlc_end_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "Open price of interval": message[1][2],
                    "High price within interval": message[1][3],
                    "Low price within interval": message[1][4],
                    "Close price of interval": message[1][5],
                    "Volume weighted average price within interval": message[1][6],
                    "Accumulated volume within interval": message[1][7],
                    "Number of trades within interval": message[1][8],
                }
            }
        )


def print_ticker_message(message):
    """
    Function prints Ticker information on currency pair from Kraken WebSockets
    API in a human-readable format.
    It takes one argument, which is the JSON string that was sent by the server.
    The function then parses through it and prints out all of its contents.

    Args:
        message: Pass the message to the function.
    """

    message = json.loads(message)
    if not isinstance(message, dict):
        print(
            {
                f"Ticker {message[3]}": {
                    "Ask": [
                        {
                            "Best ask price": message[1]["a"][0],
                            "Whole lot volume": message[1]["a"][1],
                            "Lot volume": message[1]["a"][2],
                        }
                    ],
                    "Bid": [
                        {
                            "Best bid price": message[1]["b"][0],
                            "Whole lot volume": message[1]["b"][1],
                            "Lot volume": message[1]["b"][2],
                        }
                    ],
                    "Close": [
                        {
                            "Price": message[1]["c"][0],
                            "Lot volume": message[1]["c"][1],
                        }
                    ],
                    "Volume": [
                        {
                            "Value today": message[1]["v"][0],
                            "Value over last 24 hours": message[1]["v"][1],
                        }
                    ],
                    "Volume weighted average price": [
                        {
                            "Value today": message[1]["p"][0],
                            "Value over last 24 hours": message[1]["p"][1],
                        }
                    ],
                    "Number Of Trades": [
                        {
                            "Value today": message[1]["t"][0],
                            "Value over last 24 hours": message[1]["t"][1],
                        }
                    ],
                    "Low Price": [
                        {
                            "Value today": message[1]["l"][0],
                            "Value over last 24 hours": message[1]["l"][1],
                        }
                    ],
                    "High Price": [
                        {
                            "Value today": message[1]["h"][0],
                            "Value over last 24 hours": message[1]["h"][1],
                        }
                    ],
                    "Open Price": [
                        {
                            "Value today": message[1]["o"][0],
                            "Value over last 24 hours": message[1]["o"][1],
                        }
                    ],
                }
            }
        )


def print_trade_message(message):
    """
    Function prints Trade feed for a currency pair from Kraken WebSockets API in
    a human-readable format.
    It takes one argument, which is the JSON string that was sent by the server.
    The function then parses through it and prints out all of its contents.

    Args:
        message: Pass the message to the function.
    """

    message = json.loads(message)
    print(message)
    if not isinstance(message, dict):
        trade_time = datetime.fromtimestamp(float(message[1][0][2]))

        print(
            {
                f"Trade {message[3]}": {
                    "Price": message[1][0][0],
                    "Volume": message[1][0][1],
                    "Time": trade_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "Triggering order side": message[1][0][3],
                    "Triggering order type": message[1][0][4],
                    "Misc": message[1][0][5],
                }
            }
        )
