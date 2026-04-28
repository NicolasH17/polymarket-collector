#!/usr/bin/env python3
"""
Minimal tests for error handling paths in continuous_collector.

Uses unittest + unittest.mock from stdlib (no pytest dependency).
Run with: python -m tests.test_error_paths
"""

import unittest
from unittest.mock import patch, MagicMock
import json
import logging

# Suppress logging during tests
logging.disable(logging.CRITICAL)


class TestBatchFetchErrorHandling(unittest.TestCase):
    """Test error handling in fetch_order_books_batch."""

    def setUp(self):
        self.logger = logging.getLogger("test")
        self.token_ids = ["token1", "token2"]

    @patch("scripts.continuous_collector.requests.post")
    def test_timeout_retries_with_backoff(self, mock_post):
        """Timeout should trigger retries with exponential backoff."""
        from scripts.continuous_collector import fetch_order_books_batch
        import requests

        mock_post.side_effect = requests.exceptions.Timeout()

        with patch("scripts.continuous_collector.time.sleep") as mock_sleep:
            result = fetch_order_books_batch(self.token_ids, self.logger)

        # Should have retried MAX_RETRIES times
        self.assertEqual(mock_post.call_count, 3)
        # Should have failed
        self.assertFalse(result["http_success"])
        self.assertEqual(result["books_received"], 0)
        # Should have used backoff (sleep called between retries)
        self.assertEqual(mock_sleep.call_count, 2)

    @patch("scripts.continuous_collector.requests.post")
    def test_rate_limit_429_handling(self, mock_post):
        """429 response should trigger backoff and retry."""
        from scripts.continuous_collector import fetch_order_books_batch

        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_response.headers = {"Retry-After": "5"}
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        with patch("scripts.continuous_collector.time.sleep") as mock_sleep:
            result = fetch_order_books_batch(self.token_ids, self.logger)

        # Should have retried
        self.assertEqual(mock_post.call_count, 3)
        self.assertFalse(result["http_success"])

    @patch("scripts.continuous_collector.requests.post")
    def test_connection_error_no_response_object(self, mock_post):
        """ConnectionError has no response object - should not crash."""
        from scripts.continuous_collector import fetch_order_books_batch
        import requests

        mock_post.side_effect = requests.exceptions.ConnectionError("DNS failure")

        with patch("scripts.continuous_collector.time.sleep"):
            result = fetch_order_books_batch(self.token_ids, self.logger)

        self.assertFalse(result["http_success"])
        self.assertIsNone(result["http_status"])

    @patch("scripts.continuous_collector.requests.post")
    def test_http_error_has_status_code(self, mock_post):
        """HTTPError should log the status code correctly."""
        from scripts.continuous_collector import fetch_order_books_batch
        import requests

        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError()
        mock_post.return_value = mock_response

        with patch("scripts.continuous_collector.time.sleep"):
            result = fetch_order_books_batch(self.token_ids, self.logger)

        self.assertFalse(result["http_success"])
        self.assertEqual(result["http_status"], 500)

    @patch("scripts.continuous_collector.requests.post")
    def test_unexpected_asset_ids_logged(self, mock_post):
        """Unexpected asset_ids in response should be tracked."""
        from scripts.continuous_collector import fetch_order_books_batch

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = [
            {"asset_id": "token1", "bids": [], "asks": []},
            {"asset_id": "unexpected_token", "bids": [], "asks": []},
        ]
        mock_post.return_value = mock_response

        result = fetch_order_books_batch(self.token_ids, self.logger)

        self.assertTrue(result["http_success"])
        self.assertEqual(len(result["unexpected_ids"]), 1)
        self.assertEqual(result["missing_count"], 1)  # token2 missing

    @patch("scripts.continuous_collector.requests.post")
    def test_malformed_json_response(self, mock_post):
        """Malformed JSON should return failure result."""
        from scripts.continuous_collector import fetch_order_books_batch

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status.return_value = None
        mock_response.json.side_effect = json.JSONDecodeError("test", "doc", 0)
        mock_post.return_value = mock_response

        result = fetch_order_books_batch(self.token_ids, self.logger)

        self.assertFalse(result["http_success"])

    @patch("scripts.continuous_collector.requests.post")
    def test_success_returns_correct_metrics(self, mock_post):
        """Successful response should return correct metrics."""
        from scripts.continuous_collector import fetch_order_books_batch

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = [
            {"asset_id": "token1", "bids": [{"price": "0.5", "size": "100"}], "asks": [{"price": "0.6", "size": "100"}]},
            {"asset_id": "token2", "bids": [{"price": "0.4", "size": "50"}], "asks": [{"price": "0.5", "size": "50"}]},
        ]
        mock_post.return_value = mock_response

        result = fetch_order_books_batch(self.token_ids, self.logger)

        self.assertTrue(result["http_success"])
        self.assertEqual(result["http_status"], 200)
        self.assertEqual(result["books_received"], 2)
        self.assertEqual(result["books_requested"], 2)
        self.assertEqual(result["missing_count"], 0)
        self.assertEqual(len(result["unexpected_ids"]), 0)
        self.assertIn("token1", result["books_by_token"])
        self.assertIn("token2", result["books_by_token"])


class TestBackoffCalculation(unittest.TestCase):
    """Test exponential backoff calculation."""

    def test_backoff_increases_exponentially(self):
        """Backoff should increase with each attempt."""
        from scripts.continuous_collector import _calculate_backoff

        # Run multiple times to average out jitter
        delays = []
        for attempt in [1, 2, 3]:
            samples = [_calculate_backoff(attempt) for _ in range(10)]
            delays.append(sum(samples) / len(samples))

        # Each delay should be roughly 2x the previous (accounting for jitter)
        self.assertLess(delays[0], delays[1])
        self.assertLess(delays[1], delays[2])

    def test_backoff_respects_max_delay(self):
        """Backoff should be capped at RETRY_MAX_DELAY."""
        from scripts.continuous_collector import _calculate_backoff, RETRY_MAX_DELAY

        # Very high attempt number
        delay = _calculate_backoff(100)
        self.assertLessEqual(delay, RETRY_MAX_DELAY + 1)  # +1 for jitter


class TestQAFlags(unittest.TestCase):
    """Test QA flag detection in calculate_metrics."""

    def test_missing_side_bids_empty(self):
        """Empty bids should trigger qa_missing_side."""
        from scripts.continuous_collector import calculate_metrics

        result = calculate_metrics({"bids": [], "asks": [{"price": "0.5", "size": "100"}]})
        self.assertTrue(result["qa_missing_side"])

    def test_missing_side_asks_empty(self):
        """Empty asks should trigger qa_missing_side."""
        from scripts.continuous_collector import calculate_metrics

        result = calculate_metrics({"bids": [{"price": "0.5", "size": "100"}], "asks": []})
        self.assertTrue(result["qa_missing_side"])

    def test_crossed_book(self):
        """Bid >= ask should trigger qa_crossed_book."""
        from scripts.continuous_collector import calculate_metrics

        result = calculate_metrics({
            "bids": [{"price": "0.6", "size": "100"}],
            "asks": [{"price": "0.5", "size": "100"}],
        })
        self.assertTrue(result["qa_crossed_book"])

    def test_price_out_of_range_high(self):
        """Price > 1.0 should trigger qa_price_out_of_range."""
        from scripts.continuous_collector import calculate_metrics

        result = calculate_metrics({
            "bids": [{"price": "1.5", "size": "100"}],
            "asks": [{"price": "1.6", "size": "100"}],
        })
        self.assertTrue(result["qa_price_out_of_range"])

    def test_price_out_of_range_negative(self):
        """Price < 0 should trigger qa_price_out_of_range."""
        from scripts.continuous_collector import calculate_metrics

        result = calculate_metrics({
            "bids": [{"price": "-0.1", "size": "100"}],
            "asks": [{"price": "0.5", "size": "100"}],
        })
        self.assertTrue(result["qa_price_out_of_range"])

    def test_malformed_levels_skipped(self):
        """Malformed levels should be counted and skipped."""
        from scripts.continuous_collector import calculate_metrics

        result = calculate_metrics({
            "bids": [{"price": "0.4", "size": "100"}, {"price": "bad", "size": "50"}],
            "asks": [{"price": "0.5", "size": "100"}, {"foo": "bar"}],
        })
        self.assertEqual(result["qa_malformed_levels"], 2)
        self.assertEqual(result["bid_levels"], 1)  # Only valid level
        self.assertEqual(result["ask_levels"], 1)  # Only valid level

    def test_normal_data_no_flags(self):
        """Normal data should not trigger any QA flags."""
        from scripts.continuous_collector import calculate_metrics

        result = calculate_metrics({
            "bids": [{"price": "0.4", "size": "100"}],
            "asks": [{"price": "0.5", "size": "100"}],
        })
        self.assertFalse(result["qa_missing_side"])
        self.assertFalse(result["qa_crossed_book"])
        self.assertFalse(result["qa_price_out_of_range"])
        self.assertEqual(result["qa_malformed_levels"], 0)

    def test_null_book_data(self):
        """None book_data should trigger qa_missing_side."""
        from scripts.continuous_collector import calculate_metrics

        result = calculate_metrics(None)
        self.assertTrue(result["qa_missing_side"])


if __name__ == "__main__":
    unittest.main()
