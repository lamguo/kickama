#!/usr/bin/env python3
"""
Test retry/backoff and circuit breaker logic for health_check.py.
"""
import os
import sys
import time
import unittest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from health_check import (
    CircuitBreaker, CircuitState, retry_with_backoff,
    check_http_service, get_circuit, _service_circuits,
    parse_args, DEFAULT_MAX_RETRIES, DEFAULT_BACKOFF_FACTOR,
    DEFAULT_CIRCUIT_THRESHOLD, DEFAULT_CIRCUIT_COOLDOWN,
)


class TestCircuitBreaker(unittest.TestCase):
    def setUp(self):
        _service_circuits.clear()

    def test_initial_state_closed(self):
        cb = CircuitBreaker(threshold=2, cooldown=0.1)
        self.assertEqual(cb.state, CircuitState.CLOSED)
        self.assertTrue(cb.allow_request())

    def test_opens_after_threshold_failures(self):
        cb = CircuitBreaker(threshold=3, cooldown=60)
        cb.record_failure(); cb.record_failure(); cb.record_failure()
        self.assertEqual(cb.state, CircuitState.OPEN)
        self.assertFalse(cb.allow_request())

    def test_half_open_after_cooldown(self):
        cb = CircuitBreaker(threshold=1, cooldown=0.01)
        cb.record_failure()
        self.assertEqual(cb.state, CircuitState.OPEN)
        time.sleep(0.02)
        self.assertEqual(cb.state, CircuitState.HALF_OPEN)

    def test_success_resets_to_closed(self):
        cb = CircuitBreaker(threshold=2, cooldown=60)
        cb.record_failure(); cb.record_failure()
        self.assertEqual(cb.state, CircuitState.OPEN)
        cb.record_success()
        self.assertEqual(cb.state, CircuitState.CLOSED)
        self.assertEqual(cb._failure_count, 0)

    def test_success_before_threshold_keeps_closed(self):
        cb = CircuitBreaker(threshold=5, cooldown=60)
        cb.record_failure()
        cb.record_success()
        self.assertEqual(cb.state, CircuitState.CLOSED)
        self.assertEqual(cb._failure_count, 0)

    def test_to_dict_includes_state(self):
        cb = CircuitBreaker(threshold=3, cooldown=30.0)
        d = cb.to_dict()
        self.assertEqual(d["state"], "closed")
        self.assertEqual(d["threshold"], 3)


class TestRetryWithBackoff(unittest.TestCase):
    def test_exponential_growth(self):
        d1 = retry_with_backoff(0, base_delay=1.0, backoff_factor=2.0, jitter=False)
        d2 = retry_with_backoff(1, base_delay=1.0, backoff_factor=2.0, jitter=False)
        d3 = retry_with_backoff(2, base_delay=1.0, backoff_factor=2.0, jitter=False)
        self.assertAlmostEqual(d1, 1.0, places=1)
        self.assertAlmostEqual(d2, 2.0, places=1)
        self.assertAlmostEqual(d3, 4.0, places=1)

    def test_different_backoff_factor(self):
        d = retry_with_backoff(2, base_delay=1.0, backoff_factor=3.0, jitter=False)
        self.assertAlmostEqual(d, 9.0, places=1)

    def test_jitter_adds_variation(self):
        delays = [retry_with_backoff(0, base_delay=1.0, backoff_factor=2.0, jitter=True)
                  for _ in range(100)]
        self.assertGreater(max(delays), min(delays) * 1.1)
        for d in delays:
            self.assertGreaterEqual(d, 0.4)
            self.assertLessEqual(d, 1.1)


class TestCheckHttpServiceWithRetry(unittest.TestCase):
    def setUp(self):
        _service_circuits.clear()

    @patch("http.client.HTTPConnection")
    def test_ok_on_first_try(self, mock_conn):
        mock_resp = MagicMock(status=200, read=lambda: b'{"status":"ok"}')
        mock_conn.return_value.getresponse.return_value = mock_resp
        status, detail, code = check_http_service("localhost", 8080, "/health", 5)
        self.assertEqual(status, "OK")
        self.assertEqual(code, 200)

    @patch("http.client.HTTPConnection")
    def test_client_error_no_retry(self, mock_conn):
        mock_resp = MagicMock(status=404, read=lambda: b'not found')
        mock_conn.return_value.getresponse.return_value = mock_resp
        status, detail, code = check_http_service("localhost", 8080, "/health", 5)
        self.assertEqual(status, "WARNING")
        self.assertEqual(code, 404)
        self.assertEqual(mock_conn.call_count, 1)

    @patch("http.client.HTTPConnection")
    def test_server_error_triggers_retry_then_succeeds(self, mock_conn):
        mock_attempts = [
            MagicMock(status=503, read=lambda: b'service unavailable'),
            MagicMock(status=200, read=lambda: b'{"status":"ok"}'),
        ]
        mock_conn.return_value.getresponse.side_effect = mock_attempts
        status, detail, code = check_http_service(
            "localhost", 8080, "/health", 5,
            max_retries=2, backoff_factor=1.0, circuit_threshold=5, circuit_cooldown=30
        )
        self.assertEqual(status, "OK")
        self.assertEqual(mock_conn.call_count, 2)

    @patch("http.client.HTTPConnection")
    def test_all_retries_fail_returns_critical(self, mock_conn):
        mock_resp = MagicMock(status=503, read=lambda: b'service unavailable')
        mock_conn.return_value.getresponse.return_value = mock_resp
        status, detail, code = check_http_service(
            "localhost", 8080, "/health", 5,
            max_retries=2, backoff_factor=1.0, circuit_threshold=5, circuit_cooldown=30
        )
        self.assertEqual(status, "CRITICAL")
        self.assertIn("After 2 retries", detail)

    @patch("http.client.HTTPConnection")
    def test_circuit_opens_after_consecutive_failures(self, mock_conn):
        mock_resp = MagicMock(status=503, read=lambda: b'error')
        mock_conn.return_value.getresponse.return_value = mock_resp
        for _ in range(2):
            check_http_service("localhost", 8080, "/health", 5,
                               max_retries=1, backoff_factor=1.0,
                               circuit_threshold=2, circuit_cooldown=30)
        status, detail, code = check_http_service("localhost", 8080, "/health", 5,
                                                   max_retries=1, backoff_factor=1.0,
                                                   circuit_threshold=2, circuit_cooldown=30)
        self.assertEqual(status, "CRITICAL")
        self.assertIn("Circuit breaker open", detail)


class TestCliFlags(unittest.TestCase):
    def test_default_values(self):
        with patch.object(sys, 'argv', ['health_check.py']):
            args = parse_args()
            self.assertEqual(args.max_retries, DEFAULT_MAX_RETRIES)
            self.assertEqual(args.backoff_factor, DEFAULT_BACKOFF_FACTOR)
            self.assertEqual(args.circuit_threshold, DEFAULT_CIRCUIT_THRESHOLD)
            self.assertEqual(args.circuit_cooldown, DEFAULT_CIRCUIT_COOLDOWN)

    def test_custom_values(self):
        with patch.object(sys, 'argv', [
            'health_check.py', '--max-retries', '5', '--backoff-factor', '3.0',
            '--circuit-threshold', '10', '--circuit-cooldown', '60',
        ]):
            args = parse_args()
            self.assertEqual(args.max_retries, 5)
            self.assertEqual(args.backoff_factor, 3.0)
            self.assertEqual(args.circuit_threshold, 10)
            self.assertEqual(args.circuit_cooldown, 60)


if __name__ == "__main__":
    unittest.main(verbosity=2)
