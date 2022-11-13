#!/usr/bin/env python
"""Simple example of publish/subscribe illustrating topics.
Publisher and subscriber can be started in any order, though if publisher
starts first, any messages sent before subscriber starts are lost.  More than
one subscriber can listen, and they can listen to  different topics.
Topic filtering is done simply on the start of the string, e.g. listening to
's' will catch 'sports...' and 'stocks'  while listening to 'w' is enough to
catch 'weather'.
"""

# -----------------------------------------------------------------------------
#  Copyright (c) 2010 Brian Granger, Fernando Perez
#
#  Distributed under the terms of the New BSD License.  The full license is in
#  the file COPYING.BSD, distributed as part of this software.
# -----------------------------------------------------------------------------

import zmq

from kraken_msg_pb2 import Spread


def main() -> None:
    topics = [
        "Spread - XBT/USD",
        "Ticker - XBT/USD",
        "Trade - XBT/USD",
        "OHLC - XBT/USD - Minute",
        "OHLC - XBT/USD - Hourly",
        "OHLC - XBT/USD - Daily",
    ]
    ctx = zmq.Context()
    s = ctx.socket(zmq.SUB)
    s.connect("tcp://127.0.0.1:5555")

    # manage subscriptions
    print("Receiving messages on topics: %s ..." % topics)
    for t in topics:
        s.setsockopt(zmq.SUBSCRIBE, t.encode('utf-8'))

    try:
        while True:
            topic, message = s.recv_multipart()
            spread = Spread()
            spread.ParseFromString(message)
            print(
                '   Topic: {}, msg:{}'.format(
                    topic.decode('utf-8'), str(spread)
                )
            )
    except KeyboardInterrupt:
        pass

    print("Done.")


if __name__ == "__main__":
    main()
