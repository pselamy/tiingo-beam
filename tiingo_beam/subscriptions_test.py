import json
import unittest
from typing import Text

from hypothesis import given
from hypothesis import strategies as st

from tiingo_beam import subscriptions


class CryptoSubscriptionFactoryTestCase(unittest.TestCase):
    def setUp(self):
        self.factory = subscriptions.CryptoSubscriptionFactory()

    @given(api_key=st.text(), threshold_level=st.integers(min_value=0, max_value=10))
    def test_create_subscription(self, api_key: Text, threshold_level: int):
        # Arrange
        expected_output = json.dumps(
            {
                "eventName": "subscribe",
                "authorization": api_key,
                "eventData": {"thresholdLevel": threshold_level},
            }
        )

        # Act
        actual = self.factory.create_subscription(api_key, threshold_level)

        # Assert
        self.assertEqual(actual, expected_output)


if __name__ == "__main__":
    unittest.main()
