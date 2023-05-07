import dataclasses
import enum
from typing import Callable, Iterable, Text

import websocket

from tiingo_beam import models
from tiingo_beam import parsers
from tiingo_beam import subscriptions


@dataclasses.dataclass(frozen=True)
class EndpointConfig:
    websocket_uri: Text
    subscription_factory: subscriptions.SubscriptionFactory
    trade_parser: parsers.TradeParser
    has_more_trades: Callable[[], bool] = lambda: True


class Endpoint(enum.Enum):
    CRYPTO = EndpointConfig(
        "wss://api.tiingo.com/crypto",
        subscription_factory=subscriptions.CryptoSubscriptionFactory(),
        trade_parser=parsers.CryptoTradeParser(),
    )

    def get_subscribe_message(self, api_key: Text, threshold_level: int) -> Text:
        return self.value.subscription_factory.create_subscription(
            api_key=api_key,
            threshold_level=threshold_level,
        )

    def has_more_trades(self) -> bool:
        return self.value.has_more_trades()

    def parse_trade(self, message) -> models.Trade:
        return self.value.trade_parser.parse(message)

    def trades(
        self,
        api_key: Text,
        threshold_level: int,
        create_websocket_connection=websocket.create_connection,
    ) -> Iterable[models.Trade]:
        subscribe_message = self.get_subscribe_message(
            api_key=api_key,
            threshold_level=threshold_level,
        )
        ws = create_websocket_connection(self.uri)
        ws.send(subscribe_message)
        while self.has_more_trades():
            message = ws.recv()
            trade = self.value.trade_parser.parse(message)
            if not trade:
                continue

            yield trade

    @property
    def uri(self) -> Text:
        return self.value.websocket_uri
