import dataclasses
import datetime
import enum
from typing import Text


class InstrumentType(enum.Enum):
    CRYPTO = 0


@dataclasses.dataclass(frozen=True)
class Instrument:
    symbol: Text
    type: InstrumentType


@dataclasses.dataclass
class Trade:
    exchange_name: Text
    instrument: Instrument
    price: float
    volume: float
    time: datetime.datetime
