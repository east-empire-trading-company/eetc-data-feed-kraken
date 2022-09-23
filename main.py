import threading
from data import (
    stream_trade_data,
    stream_ohlc_data,
    stream_ticker_data,
    stream_spread_data,
    stream_book_data,
)

if __name__ == "__main__":
    trade = threading.Thread(target=stream_trade_data, args=("XBT/USD",))
    ohlc = threading.Thread(target=stream_ohlc_data, args=("XBT/USD",))
    ticker = threading.Thread(target=stream_ticker_data, args=("XBT/USD",))
    spread = threading.Thread(target=stream_spread_data, args=("XBT/USD",))
    book = threading.Thread(target=stream_book_data, args=("XBT/USD",))

    trade.start()
    ohlc.start()
    ticker.start()
    spread.start()
    book.start()
