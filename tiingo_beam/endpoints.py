import dataclasses
import enum
from typing import Callable, Text

from tiingo_beam import models
from tiingo_beam import parsers
from tiingo_beam import subscriptions


@dataclasses.dataclass(frozen=True)
class EndpointConfig:
    websocket_uri: Text
    subscription_factory: subscriptions.SubscriptionFactory
    parser: parsers.TradeParser
    has_more_trades: Callable[[], bool] = lambda: True


class Endpoint(enum.Enum):
    CRYPTO = EndpointConfig(
        "wss://api.tiingo.com/crypto",
        subscription_factory=subscriptions.CryptoSubscriptionFactory(),
        parser=parsers.CryptoTradeParser(),
    )

    def get_subscribe_message(self, api_key: Text, threshold_level: int) -> Text:
        return self.value.subscription_factory.create_subscription(
            api_key=api_key,
            threshold_level=threshold_level,
        )

    def has_more_trades(self) -> bool:
        return self.value.has_more_trades()

    def parse(self, message) -> models.Trade:
        return self.value.parser.parse(message)

    @property
    def uri(self) -> Text:
        return self.value.websocket_uri
