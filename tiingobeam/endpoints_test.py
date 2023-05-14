import datetime
import unittest

from tiingobeam import parsers, subscriptions
from tiingobeam import endpoints
from tiingobeam import models


FAKE_SUBSCRIPTION = "test-subscription"
FAKE_TRADE = models.Trade(
    exchange_name="ExchangeA",
    instrument=models.Instrument(
        symbol="BTC-USD",
        type=models.InstrumentType.CRYPTO,
    ),
    price=50000.0,
    volume=0.1,
    time=datetime.datetime(2022, 4, 1, 12, 0, 0),
)


class FakeCryptoSubscriptionFactory(subscriptions.CryptoSubscriptionFactory):
    def create_subscription(self, api_key, threshold_level):
        return FAKE_SUBSCRIPTION


class FakeCryptoTradeParser(parsers.TradeParser):
    def parse(self, message) -> models.Trade:
        return FAKE_TRADE


class TestEndpoint(unittest.TestCase):
    def test_endpoint_crypto_subscription_factory(self):
        # Arrange
        endpoint_config = endpoints.EndpointConfig(
            "wss://api.tiingo.com/crypto",
            subscription_factory=subscriptions.CryptoSubscriptionFactory(),
            parser=parsers.CryptoTradeParser(),
        )

        # Act
        subscription_factory = endpoint_config.subscription_factory

        # Assert
        self.assertIsInstance(
            subscription_factory, subscriptions.CryptoSubscriptionFactory
        )

    def test_endpoint_crypto_uri(self):
        # Arrange
        endpoint = endpoints.Endpoint.CRYPTO

        # Act
        uri = endpoint.uri

        # Assert
        self.assertEqual(uri, "wss://api.tiingo.com/crypto")

    def test_endpoint_crypto_subscribe_message(self):
        # Arrange
        fake_factory = FakeCryptoSubscriptionFactory()
        endpoints.Endpoint.CRYPTO.value.subscription_factory = fake_factory
        endpoint = endpoints.Endpoint.CRYPTO

        # Act
        result = endpoint.get_subscribe_message("test-api-key", 2)

        # Assert
        self.assertEqual(result, FAKE_SUBSCRIPTION)

    def test_endpoint_crypto_has_more_trades(self):
        # Arrange
        endpoint = endpoints.Endpoint.CRYPTO

        # Act
        result = endpoint.has_more_trades()

        # Assert
        self.assertTrue(result)  # default lambda function returns True

    def test_endpoint_crypto_parse(self):
        # Arrange
        fake_parser = FakeCryptoTradeParser()
        endpoints.Endpoint.CRYPTO.value.parser = fake_parser
        endpoint = endpoints.Endpoint.CRYPTO

        # Act
        result = endpoint.parse({"message": "test-message"})

        # Assert
        self.assertEqual(result, FAKE_TRADE)
