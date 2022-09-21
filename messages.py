import json
from datetime import datetime


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
