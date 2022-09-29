import json
from google.protobuf import json_format
import kraken_msg_pb2
from messages import (
    process_ticker_message,
    process_spread_message,
    process_ohlc_message,
    process_trade_message,
)


def test_process_ticker_message():
    # given
    output_params = {"ticker": "XBT/USD", "price": 19556.20000}
    kraken_message = json.dumps(
        [
            340,
            {
                "a": ["19555.20000", 0, "0.11487174"],
                "b": ["19555.10000", 0, "0.84376922"],
                "c": ["19556.20000", "0.17710000"],
                "v": ["9663.60340294", "9980.88762714"],
                "p": ["19113.40193", "19111.15264"],
                "t": [28811, 30665],
                "l": ["18487.50000", "18487.50000"],
                "h": ["19651.20000", "19651.20000"],
                "o": ["19090.00000", "19002.30000"],
            },
            "ticker",
            "XBT/USD",
        ]
    )

    # when
    output_params_proto = json_format.ParseDict(output_params, kraken_msg_pb2.Ticker())
    kraken_message_proto = process_ticker_message(kraken_message)

    # then
    assert output_params_proto == kraken_message_proto


def test_process_ohlc_message():
    # given
    output_params = {
        "ohlc": "XBT/USD",
        "begin": "2022-09-29 21:16:15",
        "end": "2022-09-29 21:17:00",
        "open": 19403.0,
        "high": 19420.0,
        "low": 19403.0,
        "close": 19420.0,
        "vwap": 19414.938,
        "volume": 1.9854417,
        "number_of_trades": 52,
    }
    kraken_message = json.dumps(
        [
            343,
            [
                "1664478975.666711",
                "1664479020.000000",
                "19403.00000",
                "19420.00000",
                "19403.00000",
                "19420.00000",
                "19414.93677",
                "1.98544165",
                52,
            ],
            "ohlc-1",
            "XBT/USD",
        ]
    )

    # when
    output_params_proto = json_format.ParseDict(output_params, kraken_msg_pb2.OHLC())
    kraken_message_proto = process_ohlc_message(kraken_message)

    # then
    assert output_params_proto == kraken_message_proto


def test_process_spread_message():
    # given
    kraken_message = json.dumps(
        [
            341,
            [
                "19301.90000",
                "19302.00000",
                "1664477929.245247",
                "4.06014894",
                "0.00100000",
            ],
            "spread",
            "XBT/USD",
        ]
    )
    output_params = {
        "spread": "XBT/USD",
        "ask": 19302.0,
        "bid": 19301.9,
        "time": "2022-09-29 20:58:49",
        "bid_volume": 4.0601487,
        "ask_volume": 0.001,
    }

    # when
    output_params_proto = json_format.ParseDict(output_params, kraken_msg_pb2.Spread())
    kraken_message_proto = process_spread_message(kraken_message)

    # then
    assert output_params_proto == kraken_message_proto


def test_process_trade_message():
    # given
    kraken_message = json.dumps(
        [
            337,
            [["19416.20000", "0.00100000", "1664479174.047114", "s", "m", ""]],
            "trade",
            "XBT/USD",
        ]
    )
    output_params = {
        "trade": "XBT/USD",
        "price": 19416.2,
        "volume": 0.001,
        "time": "2022-09-29 21:19:34",
        "side": "s",
        "order_type": "m",
        "misc": "",
    }

    # when
    output_params_proto = json_format.ParseDict(output_params, kraken_msg_pb2.Trade())
    kraken_message_proto = process_trade_message(kraken_message)

    # then
    assert output_params_proto == kraken_message_proto
