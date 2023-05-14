import dataclasses
import datetime
from datetime import timezone
from typing import Callable, Iterable, Optional, Text

import apache_beam as beam
from apache_beam.io import restriction_trackers
from apache_beam.transforms import core
from apache_beam.transforms import window
from apache_beam.utils import timestamp

from tiingo_beam import endpoints


@dataclasses.dataclass
class RestrictionProvider(core.RestrictionProvider):
    utcnow: Callable[[], datetime.datetime] = lambda: datetime.datetime.now(
        tz=timezone.utc
    )

    def initial_restriction(
        self,
        element: endpoints.Endpoint,
    ) -> restriction_trackers.OffsetRange:
        # noinspection PyTypeChecker
        now: datetime.datetime = self.utcnow()
        start_time = int(now.timestamp())
        return restriction_trackers.OffsetRange(
            start_time, timestamp.MAX_TIMESTAMP.seconds()
        )

    def create_tracker(
        self,
        restriction: restriction_trackers.OffsetRange,
    ) -> restriction_trackers.OffsetRestrictionTracker:
        return restriction_trackers.OffsetRestrictionTracker(restriction)

    def restriction_size(
        self,
        element: endpoints.Endpoint,
        restriction: restriction_trackers.OffsetRange,
    ):
        return restriction.stop - restriction.start


@beam.DoFn.unbounded_per_element()
@dataclasses.dataclass
class GetEndpointTrades(beam.DoFn):
    api_key: Text
    threshold_level: int
    utcnow: Callable[[], datetime.datetime] = lambda: datetime.datetime.now(
        tz=timezone.utc
    )

    def _get_rain_check(
        self, start_time: datetime.datetime
    ) -> Optional[timestamp.Duration]:
        # noinspection PyTypeChecker
        now: datetime.datetime = self.utcnow()
        seconds_until_start = max(0.0, (start_time - now).total_seconds())
        if not seconds_until_start:
            return

        return timestamp.Duration(seconds=seconds_until_start)

    # noinspection PyMethodOverriding
    def process(
        self,
        endpoint: endpoints.Endpoint,
        restriction_tracker=beam.DoFn.RestrictionParam(RestrictionProvider()),
    ) -> Iterable[window.TimestampedValue]:
        start_time = datetime.datetime.fromtimestamp(
            restriction_tracker.current_restriction().start,
            tz=timezone.utc,
        )
        rain_check = self._get_rain_check(start_time)
        if rain_check:
            restriction_tracker.defer_remainder(rain_check)
            return

        last_trade_time = 0
        trades = endpoint.trades(
            api_key=self.api_key, threshold_level=self.threshold_level
        )
        for trade in filter(lambda t: t.time >= start_time, trades):
            trade_time = int(trade.time.timestamp())
            if trade_time > last_trade_time and not restriction_tracker.try_claim(
                trade_time
            ):
                return

            last_trade_time = max(trade_time, last_trade_time)
            yield window.TimestampedValue(trade, trade_time)


@dataclasses.dataclass
class GetTrades(beam.PTransform):
    api_key: Text
    threshold_level: int
    endpoints: Iterable[endpoints.Endpoint]
    utcnow: Callable[[], datetime.datetime] = datetime.datetime.utcnow

    # noinspection PyMethodOverriding
    def expand(self, p_begin) -> beam.PCollection[window.TimestampedValue]:
        return (
            p_begin
            | core.Create(self.endpoints)
            | beam.ParDo(GetEndpointTrades(self.api_key, self.threshold_level))
        )
