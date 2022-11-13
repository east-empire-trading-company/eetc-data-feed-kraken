import threading

import zmq

import settings
from data import (
    stream_trade_data,
    stream_ohlc_data,
    stream_ticker_data,
    stream_spread_data,
)

if __name__ == "__main__":
    # create ZeroMQ Context which will be shared by all threads
    zmq_context = zmq.Context()

    # NOTE: IPC works on Linux only, for Windows use Inproc
    zmq_ipc_url = settings.ZMQ_PUSH_PULL_IPC_URL
    zmq_pub_url = settings.ZMQ_PUB_SOCKET_URL

    # create ZeroMQ PULL Socket and bind it to IPC url
    # this PULL socket will receive data from PUSH sockets in all threads
    zmq_pull_socket = zmq_context.socket(zmq.PULL)
    zmq_pull_socket.bind(zmq_ipc_url)

    # create ZeroMQ PUB Socket and bind it to port 5555
    # this PUB socket will receive data from the PUSH socket via zmq_proxy()
    zmq_pub_socket = zmq_context.socket(zmq.PUB)
    zmq_pub_socket.bind(zmq_pub_url)

    # TODO finish implementations for trade, ohlc and ticker (use spread as reference)
    trade = threading.Thread(target=stream_trade_data, args=("XBT/USD", zmq_context))
    ohlc = threading.Thread(target=stream_ohlc_data, args=(["XBT/USD"], zmq_context))
    ticker = threading.Thread(target=stream_ticker_data, args=(["XBT/USD"], zmq_context))
    spread = threading.Thread(target=stream_spread_data, args=(["XBT/USD"], zmq_context))

    # start threads
    trade.start()
    ohlc.start()
    ticker.start()
    spread.start()

    zmq.proxy(zmq_pull_socket, zmq_pub_socket)
