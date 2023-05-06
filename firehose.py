import dataclasses
import datetime
import dateutil.parser
import json
import os
from typing import Callable

import apache_beam as beam
from apache_beam.io import restriction_trackers
from apache_beam.transforms import core
from apache_beam.utils import timestamp
from apache_beam.transforms import window
from google.protobuf import json_format
from websocket import create_connection


class RestrictionProvider(core.RestrictionProvider):
    def initial_restriction(self, granularity: candles_pb2.Granularity):
        return restriction_trackers.OffsetRange(0, timestamp.MAX_TIMESTAMP.seconds())

    def create_tracker(self, restriction):
        return restriction_trackers.OffsetRestrictionTracker(restriction)

    def restriction_size(self, element, restriction):
        return restriction.stop - restriction.start


@dataclasses.dataclass(frozen=True)
class GetTiingoWebsocketMessagesRequest:
    ...


@beam.DoFn.unbounded_per_element()
@dataclasses.dataclass
class GetTiingoTrades(beam.DoFn):
    utcnow: Callable[[], datetime.datetime] = datetime.datetime.utcnow

    @staticmethod
    def _create_trade(data, trade_time) -> trading_pb2.Trade:
        symbol = data[1].upper()
        exchange = data[3]
        volume = data[4]
        price = data[5]
        instrument = instruments_pb2.Instrument(
            symbol=symbol, type=instruments_pb2.Instrument.Type.CRYPTO
        )
        exchange = exchanges_pb2.ExchangeMetadata(exchange_name=exchange)
        trade_descriptor = trading_pb2.TradeDescriptor(
            exchange=exchange, instrument=instrument
        )
        return trading_pb2.Trade(
            trade_descriptor=trade_descriptor,
            timestamp={"seconds": trade_time},
            price=price,
            volume=volume,
        )

    # noinspection PyMethodOverriding
    def process(
        self,
        element: GetTiingoWebsocketMessagesRequest,
        restriction_tracker=beam.DoFn.RestrictionParam(RestrictionProvider()),
    ):
        start_time = restriction_tracker.current_restriction().start
        now = int(self.utcnow().timestamp())

        if start_time > now:
            restriction_tracker.defer_remainder(
                timestamp.Duration(seconds=start_time - now),
            )
            return

        ws = create_connection("wss://api.tiingo.com/crypto")

        subscribe = {
            "eventName": "subscribe",
            "authorization": os.environ["TIINGO_API_KEY"],
            "eventData": {"thresholdLevel": 2},
        }

        ws.send(json.dumps(subscribe))
        last_trade_time = 0
        while True:
            # Pull records from an external service.
            record = json.loads(ws.recv())
            data = record.get("data", [])
            if len(data) != 6:
                continue

            trade_time = int(dateutil.parser.isoparse(data[2]).timestamp())
            if trade_time < start_time:
                continue

            if trade_time > last_trade_time and not restriction_tracker.try_claim(
                trade_time
            ):
                return

            last_trade_time = max(trade_time, last_trade_time)
            trade = self._create_trade(data, trade_time)
            yield window.TimestampedValue(json_format.MessageToDict(trade), trade_time)
