import zmq

from kraken_msg_pb2 import Spread, Trade, OHLC, Ticker


def main() -> None:
    topics = [
        "Spread - XBT/USD",
        "Ticker - XBT/USD",
        "Trade - XBT/USD",
        "OHLC - XBT/USD - Minutely",
        "OHLC - XBT/USD - Hourly",
        "OHLC - XBT/USD - Daily",
    ]
    ctx = zmq.Context()
    s = ctx.socket(zmq.SUB)
    s.connect("tcp://127.0.0.1:5555")

    # manage subscriptions
    print(f"Receiving messages on topics: {topics}")
    for t in topics:
        s.setsockopt(zmq.SUBSCRIBE, t.encode("utf-8"))

    try:
        while True:
            topic, message = s.recv_multipart()

            if topic.decode("utf-8") == "Spread - XBT/USD":
                spread = Spread()
                spread.ParseFromString(message)
                print(f"topic: {topic.decode('utf-8')}\n{str(spread)}")

            if topic.decode("utf-8") == "Trade - XBT/USD":
                trade = Trade()
                trade.ParseFromString(message)
                print(f"topic: {topic.decode('utf-8')}\n{str(trade)}")

            if topic.decode("utf-8") == "Ticker - XBT/USD":
                ticker = Ticker()
                ticker.ParseFromString(message)
                print(f"topic: {topic.decode('utf-8')}\n{str(ticker)}")

            if topic.decode("utf-8") == "OHLC - XBT/USD - Minutely":
                ohlc = OHLC()
                ohlc.ParseFromString(message)
                print(f"topic: {topic.decode('utf-8')}\n{str(ohlc)}")

            if topic.decode("utf-8") == "OHLC - XBT/USD - Hourly":
                ohlc = OHLC()
                ohlc.ParseFromString(message)
                print(f"topic: {topic.decode('utf-8')}\n{str(ohlc)}")

            if topic.decode("utf-8") == "OHLC - XBT/USD - Daily":
                ohlc = OHLC()
                ohlc.ParseFromString(message)
                print(f"topic: {topic.decode('utf-8')}\n{str(ohlc)}")

    except KeyboardInterrupt:
        pass

    print("Done.")


if __name__ == "__main__":
    main()
