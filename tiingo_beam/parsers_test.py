import json
from datetime import datetime
from typing import Text
import unittest

import pytz
from hypothesis import given
from hypothesis import strategies as st

from tiingo_beam import models
from tiingo_beam import parsers


class TestCryptoTradeParser(unittest.TestCase):
    def setUp(self):
        self.parser = parsers.CryptoTradeParser()

    @given(
        symbol=st.text(min_size=1).filter(lambda t: t.isalnum()),
        year=st.integers(min_value=2000, max_value=9999),
        month=st.integers(min_value=1, max_value=12),
        day=st.integers(min_value=1, max_value=28),
        hour=st.integers(min_value=0, max_value=23),
        minute=st.integers(min_value=0, max_value=59),
        second=st.integers(min_value=0, max_value=59),
        exchange_name=st.text(),
        volume=st.floats(min_value=0),
        price=st.floats(min_value=0),
    )
    def test_parse_valid_trade(
        self,
        symbol: Text,
        year: int,
        month: int,
        day: int,
        hour: int,
        minute: int,
        second: int,
        exchange_name: Text,
        volume: float,
        price: float,
    ):
        # Arrange
        message = {
            "data": [
                "T",
                symbol,
                f"{year}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z",
                exchange_name,
                volume,
                price,
            ]
        }
        expected = models.Trade(
            exchange_name=exchange_name,
            instrument=models.Instrument(
                symbol=symbol.upper(),
                type=models.InstrumentType.CRYPTO,
            ),
            volume=volume,
            price=price,
            time=datetime(year, month, day, hour, minute, second, tzinfo=pytz.UTC),
        )

        # Act
        actual = self.parser.parse(json.dumps(message))

        # Assert
        self.assertEqual(actual, expected)

    def test_parse_invalid_trade(self):
        message = '{"data": ["T", "BTC/USD", "2022-04-01T12:00:00Z", "ExchangeA", "invalid", "50000.0"]}'
        self.assertIsNone(self.parser.parse(message))

    def test_parse_valid_trade_with_positive_price_and_volume(self):
        # Arrange
        message = {
            "data": [
                "T",
                "BTC/USD",
                "2022-04-01T12:00:00Z",
                "ExchangeA",
                10.5,
                50000.0,
            ]
        }
        expected = models.Trade(
            exchange_name="ExchangeA",
            instrument=models.Instrument(
                symbol="BTC/USD",
                type=models.InstrumentType.CRYPTO,
            ),
            volume=10.5,
            price=50000.0,
            time=datetime(2022, 4, 1, 12, 0, 0, tzinfo=pytz.UTC),
        )

        # Act
        actual = self.parser.parse(json.dumps(message))

        # Assert
        self.assertEqual(actual, expected)

    def test_parse_valid_trade_with_zero_volume(self):
        # Arrange
        message = {
            "data": [
                "T",
                "BTC/USD",
                "2022-04-01T12:00:00Z",
                "ExchangeA",
                0,
                50000.0,
            ]
        }
        expected = models.Trade(
            exchange_name="ExchangeA",
            instrument=models.Instrument(
                symbol="BTC/USD",
                type=models.InstrumentType.CRYPTO,
            ),
            volume=0,
            price=50000.0,
            time=datetime(2022, 4, 1, 12, 0, 0, tzinfo=pytz.UTC),
        )

        # Act
        actual = self.parser.parse(json.dumps(message))

        # Assert
        self.assertEqual(actual, expected)

    def test_parse_valid_trade_with_zero_price(self):
        # Arrange
        message = {
            "data": [
                "T",
                "BTC/USD",
                "2022-04-01T12:00:00Z",
                "ExchangeA",
                10.5,
                0,
            ]
        }
        expected = models.Trade(
            exchange_name="ExchangeA",
            instrument=models.Instrument(
                symbol="BTC/USD",
                type=models.InstrumentType.CRYPTO,
            ),
            volume=10.5,
            price=0,
            time=datetime(2022, 4, 1, 12, 0, 0, tzinfo=pytz.UTC),
        )

        # Act
        actual = self.parser.parse(json.dumps(message))

        # Assert
        self.assertEqual(actual, expected)

    def test_parse_invalid_trade_with_empty_data_field(self):
        message = '{"data": []}'
        self.assertIsNone(self.parser.parse(message))

    def test_parse_invalid_trade_with_missing_fields(self):
        message = '{"data": ["T", "BTC/USD", "2022-04-01T12:00:00Z", "ExchangeA"]}'
        self.assertIsNone(self.parser.parse(message))

    def test_parse_invalid_trade_with_invalid_symbol(self):
        message = '{"data": ["T", "", "2022-04-01T12:00:00Z", "ExchangeA", "10.5", "50000.0"]}'
        self.assertIsNone(self.parser.parse(message))

    def test_parse_invalid_trade_with_invalid_timestamp(self):
        message = '{"data": ["T", "BTC/USD", "2022-04-0112:00:00Z", "ExchangeA", "10.5", "50000.0"]'
        self.assertIsNone(self.parser.parse(message))


if __name__ == "__main__":
    unittest.main()
