import dataclasses
import json
from typing import Protocol, Text


class SubscriptionFactory(Protocol):
    """
    A class that creates subscription requests for Tiingo Websocket APIs.
    """

    def create_subscription(self, api_key: Text, threshold_level: int) -> Text:
        ...


@dataclasses.dataclass(frozen=True)
class CryptoSubscriptionFactory(SubscriptionFactory):
    """
    A class that creates subscription requests for a cryptocurrency exchange.
    """

    def create_subscription(self, api_key: Text, threshold_level: int) -> Text:
        """
        Creates a subscription request for a cryptocurrency exchange using the given API key and threshold level.

        :param api_key: The API key for the user's account on the exchange.
        :type api_key: str
        :param threshold_level: The threshold level for the subscription.
        :type threshold_level: int
        :return: The subscription request as a JSON-formatted string.
        :rtype: str
        """
        return json.dumps(
            {
                "eventName": "subscribe",
                "authorization": api_key,
                "eventData": {"thresholdLevel": threshold_level},
            }
        )
