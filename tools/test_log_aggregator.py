import unittest
from log_aggregator import NginxLogParser, TextLogParser, JSONLogParser


class TestLogAggregator(unittest.TestCase):
    def setUp(self):
        self.nginx_parser = NginxLogParser()
        self.text_parser = TextLogParser()
        self.json_parser = JSONLogParser()

    def test_nginx_parser_valid_log(self):
        # Sample anonymized production Nginx log
        log_line = (
            '192.168.1.100 - user1 [10/Oct/2023:13:55:36 -0700] '
            '"GET /api/v1/status HTTP/1.1" 200 1024 '
            '"https://example.com" "Mozilla/5.0"'
        )
        result = self.nginx_parser.parse(log_line)
        self.assertIsNotNone(result)
        self.assertEqual(result['service'], 'nginx')
        self.assertEqual(result['level'], 'info')
        self.assertEqual(result['fields']['status'], 200)
        self.assertEqual(result['fields']['body_bytes'], '1024')

    def test_nginx_parser_malformed_line(self):
        # Edge case 1: Malformed line without proper quotes
        log_line = (
            '192.168.1.100 - - [10/Oct/2023:13:55:36 -0700] '
            'GET /api/v1/status HTTP/1.1 200 1024'
        )
        result = self.nginx_parser.parse(log_line)
        self.assertIsNone(result)

    def test_nginx_parser_missing_fields(self):
        # Edge case 2: Truncated log line (missing referer and user-agent)
        log_line = (
            '192.168.1.100 - user1 [10/Oct/2023:13:55:36 -0700] '
            '"GET /api/v1/status HTTP/1.1" 200'
        )
        result = self.nginx_parser.parse(log_line)
        self.assertIsNone(result)

    def test_text_parser_error_level(self):
        # Sample production app log with ERROR level
        log_line = (
            '2023-10-10T13:55:36Z [payment_service] ERROR '
            'Failed to process transaction 12345'
        )
        result = self.text_parser.parse(log_line)
        self.assertIsNotNone(result)
        self.assertEqual(result['level'], 'error')
        self.assertEqual(result['service'], 'payment_service')

    def test_text_parser_malformed_empty(self):
        # Edge case 3: Empty string or whitespace only
        result = self.text_parser.parse('   \n  ')
        self.assertIsNone(result)

    def test_json_parser_valid_log(self):
        # Sample production JSON log
        log_line = (
            '{"timestamp": "2023-10-10T13:55:36Z", '
            '"level": "error", "service": "auth", '
            '"message": "Login failed"}'
        )
        result = self.json_parser.parse(log_line)
        self.assertIsNotNone(result)
        self.assertEqual(result['level'], 'error')
        self.assertEqual(result['service'], 'auth')

    def test_json_parser_invalid_json(self):
        # Edge case 4: Corrupted/truncated JSON
        log_line = (
            '{"timestamp": "2023-10-10T13:55:36Z", '
            '"level": "error", "service": "auth", '
            '"message": "Login f'
        )
        result = self.json_parser.parse(log_line)
        self.assertIsNone(result)


if __name__ == '__main__':
    unittest.main()
