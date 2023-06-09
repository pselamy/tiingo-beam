import dataclasses
import dateutil.parser
import json
import logging
from typing import Optional, Protocol, Text

from tiingobeam import models


@dataclasses.dataclass(frozen=True)
class TradeParser(Protocol):
    def parse(self, message: Text) -> Optional[models.Trade]:
        ...


class CryptoTradeParser(TradeParser):
    def parse(self, message: Text) -> Optional[models.Trade]:
        try:
            # Pull records from an external service.
            record = json.loads(message)
            data = record.get("data", [])
            if len(data) != 6 or data[0] != "T" or not data[1]:
                return

            return models.Trade(
                exchange_name=data[3],
                instrument=models.Instrument(
                    symbol=data[1].upper(),
                    type=models.InstrumentType.CRYPTO,
                ),
                volume=float(data[4]),
                price=float(data[5]),
                time=dateutil.parser.isoparse(data[2]),
            )
        except (
            json.decoder.JSONDecodeError,
            AttributeError,
            IndexError,
            TypeError,
            ValueError,
        ):
            logging.error("Unable to parse trade from %s", message)
