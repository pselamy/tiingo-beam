import dataclasses
import enum
from typing import Callable, Iterable, Text

import websocket

from tiingobeam import models
from tiingobeam import parsers
from tiingobeam import subscriptions


@dataclasses.dataclass(frozen=True)
class EndpointConfig:
    websocket_uri: Text
    subscription_factory: subscriptions.SubscriptionFactory
    parser: parsers.TradeParser
    has_more_trades: Callable[[], bool] = lambda: True


class Endpoint(enum.Enum):
    CRYPTO = enum.auto()

    def get_subscribe_message(self, api_key: Text, threshold_level: int) -> Text:
        return self.endpoint_config.subscription_factory.create_subscription(
            api_key=api_key,
            threshold_level=threshold_level,
        )

    @property
    def endpoint_config(self) -> EndpointConfig:
        return ENDPOINT_CONFIGS[self]

    def has_more_trades(self) -> bool:
        return self.endpoint_config.has_more_trades()

    def parse(self, message) -> models.Trade:
        return self.endpoint_config.parser.parse(message)

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
            trade = self.endpoint_config.parser.parse(message)
            if not trade:
                continue

            yield trade

    @property
    def uri(self) -> Text:
        return self.endpoint_config.websocket_uri


ENDPOINT_CONFIGS = {
    Endpoint.CRYPTO: EndpointConfig(
        "wss://api.tiingo.com/crypto",
        subscription_factory=subscriptions.CryptoSubscriptionFactory(),
        parser=parsers.CryptoTradeParser(),
    )
}
