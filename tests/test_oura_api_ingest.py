"""
Tests for Oura API ingestion Lambda.

Covers:
- CSV transformer: column flattening, MET computation, header order
- API client: mocked urllib responses, pagination, error handling
- Handler: mocked SSM + S3, verifies correct S3 keys + AES256 encryption
"""

import unittest
from unittest.mock import patch, MagicMock, call
import json
import urllib.error
from datetime import datetime

# Import modules under test
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../lambda/oura_api_ingest'))

from handler import lambda_handler
from csv_transformer import records_to_csv, COLUMNS
from oura_client import fetch_daily_data


class TestCSVTransformer(unittest.TestCase):
    """Test JSON → CSV transformation correctness."""
    
    def test_readiness_flattening(self):
        """Verify readiness contributors flatten correctly."""
        api_data = [{
            "id": "test-id-1",
            "day": "2026-02-17",
            "score": 85,
            "temperature_deviation": -0.08,
            "temperature_trend_deviation": 0.04,
            "timestamp": "2026-02-17T00:00:00+00:00",
            "contributors": {
                "activity_balance": 80,
                "body_temperature": 100,
                "hrv_balance": 78,
                "previous_day_activity": 80,
                "previous_night": 77,
                "recovery_index": 94,
                "resting_heart_rate": 100,
                "sleep_balance": 86,
                "sleep_regularity": 83
            }
        }]
        
        csv_str = records_to_csv(api_data, 'readiness')
        lines = csv_str.strip().split('\n')
        
        # Verify header
        header = lines[0]
        self.assertIn('contributors_activity_balance', header)
        self.assertIn('contributors_body_temperature', header)
        self.assertIn('contributors_hrv_balance', header)
        
        # Verify data row
        data = lines[1]
        self.assertIn('85', data)  # score
        self.assertIn('80', data)  # activity_balance or previous_day_activity
        self.assertIn('100', data)  # body_temperature or resting_heart_rate
    
    def test_sleep_flattening(self):
        """Verify sleep contributors flatten correctly."""
        api_data = [{
            "id": "test-id-2",
            "day": "2026-02-17",
            "score": 82,
            "timestamp": "2026-02-17T00:00:00+00:00",
            "contributors": {
                "deep_sleep": 85,
                "efficiency": 92,
                "latency": 88,
                "rem_sleep": 80,
                "restfulness": 75,
                "timing": 90,
                "total_sleep": 87
            }
        }]
        
        csv_str = records_to_csv(api_data, 'sleep')
        lines = csv_str.strip().split('\n')
        
        header = lines[0]
        self.assertIn('contributors_deep_sleep', header)
        self.assertIn('contributors_efficiency', header)
        self.assertIn('contributors_timing', header)
        
        data = lines[1]
        self.assertIn('82', data)
        self.assertIn('85', data)  # deep_sleep
    
    def test_activity_met_computation(self):
        """Verify MET interval computation matches split_oura_data.py logic."""
        api_data = [{
            "id": "test-id-3",
            "day": "2026-02-17",
            "score": 75,
            "timestamp": "2026-02-17T00:00:00+00:00",
            "active_calories": 450,
            "steps": 8500,
            "high_activity_time": 1800,
            "medium_activity_time": 3600,
            "low_activity_time": 7200,
            "sedentary_time": 72000,
            "total_calories": 2200,
            "met": {
                "interval": 60,
                "items": [1.0, 1.5, 2.0, 3.0, 1.2]
            }
        }]
        
        csv_str = records_to_csv(api_data, 'activity')
        lines = csv_str.strip().split('\n')
        
        header = lines[0]
        self.assertIn('met_interval', header)
        self.assertIn('met_avg', header)
        self.assertIn('met_max', header)
        self.assertIn('met_count', header)
        
        data = lines[1]
        # met_avg = mean([1.0, 1.5, 2.0, 3.0, 1.2]) = 1.74
        # met_max = 3.0
        # met_count = 5
        self.assertIn('60', data)  # interval
        self.assertIn('1.74', data)  # avg
        self.assertIn('3.0', data)  # max
        self.assertIn('5', data)  # count
    
    def test_header_compatibility(self):
        """Verify generated CSV headers match expected schema."""
        # Sample data for each type
        readiness_data = [{
            "id": "r1", "day": "2026-02-17", "score": 85,
            "temperature_deviation": 0.0, "temperature_trend_deviation": 0.0,
            "timestamp": "2026-02-17T00:00:00+00:00",
            "contributors": {
                "activity_balance": 80, "body_temperature": 100,
                "hrv_balance": 78, "previous_day_activity": 80,
                "previous_night": 77, "recovery_index": 94,
                "resting_heart_rate": 100, "sleep_balance": 86,
                "sleep_regularity": 83
            }
        }]
        
        sleep_data = [{
            "id": "s1", "day": "2026-02-17", "score": 82,
            "timestamp": "2026-02-17T00:00:00+00:00",
            "contributors": {
                "deep_sleep": 85, "efficiency": 92, "latency": 88,
                "rem_sleep": 80, "restfulness": 75, "timing": 90,
                "total_sleep": 87
            }
        }]
        
        activity_data = [{
            "id": "a1", "day": "2026-02-17", "score": 75,
            "timestamp": "2026-02-17T00:00:00+00:00",
            "active_calories": 450, "steps": 8500,
            "high_activity_time": 1800, "medium_activity_time": 3600,
            "low_activity_time": 7200, "sedentary_time": 72000,
            "total_calories": 2200,
            "met": {"interval": 60, "items": [1.0, 2.0]}
        }]
        
        # Generate CSVs
        readiness_csv = records_to_csv(readiness_data, 'readiness')
        sleep_csv = records_to_csv(sleep_data, 'sleep')
        activity_csv = records_to_csv(activity_data, 'activity')
        
        # Extract headers (strip to handle \r\n line endings)
        readiness_header = [h.strip() for h in readiness_csv.split('\n')[0].split(',')]
        sleep_header = [h.strip() for h in sleep_csv.split('\n')[0].split(',')]
        activity_header = [h.strip() for h in activity_csv.split('\n')[0].split(',')]
        
        # Verify against COLUMNS from transformer
        expected_readiness = COLUMNS['readiness']
        expected_sleep = COLUMNS['sleep']
        expected_activity = COLUMNS['activity']
        
        self.assertEqual(readiness_header, expected_readiness)
        self.assertEqual(sleep_header, expected_sleep)
        self.assertEqual(activity_header, expected_activity)


class TestOuraClient(unittest.TestCase):
    """Test API client with mocked urllib."""
    
    @patch('urllib.request.urlopen')
    def test_successful_fetch(self, mock_urlopen):
        """Verify successful API call and response parsing."""
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({
            "data": [{"id": "test", "day": "2026-02-17", "score": 85}],
            "next_token": None
        }).encode('utf-8')
        mock_response.__enter__.return_value = mock_response
        mock_urlopen.return_value = mock_response
        
        result = fetch_daily_data("test-token", "readiness", "2026-02-17", "2026-02-17")
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['id'], 'test')
    
    @patch('urllib.request.urlopen')
    def test_pagination(self, mock_urlopen):
        """Verify pagination handling with next_token."""
        # First response with next_token
        mock_response1 = MagicMock()
        mock_response1.read.return_value = json.dumps({
            "data": [{"id": "1", "day": "2026-02-17", "score": 85}],
            "next_token": "token123"
        }).encode('utf-8')
        mock_response1.__enter__.return_value = mock_response1
        
        # Second response (final)
        mock_response2 = MagicMock()
        mock_response2.read.return_value = json.dumps({
            "data": [{"id": "2", "day": "2026-02-18", "score": 87}],
            "next_token": None
        }).encode('utf-8')
        mock_response2.__enter__.return_value = mock_response2
        
        mock_urlopen.side_effect = [mock_response1, mock_response2]
        
        result = fetch_daily_data("test-token", "readiness", "2026-02-17", "2026-02-18")
        
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['id'], '1')
        self.assertEqual(result[1]['id'], '2')
    
    @patch('urllib.request.urlopen')
    def test_401_error_handling(self, mock_urlopen):
        """Verify 401 raises clear error."""
        mock_urlopen.side_effect = urllib.error.HTTPError(
            url='test', code=401, msg='Unauthorized', hdrs={}, fp=None
        )
        
        with self.assertRaises(ValueError) as context:
            fetch_daily_data("bad-token", "readiness", "2026-02-17", "2026-02-17")
        
        self.assertIn('401', str(context.exception))


class TestLambdaHandler(unittest.TestCase):
    """Test Lambda handler with mocked AWS services."""
    
    @patch('handler.fetch_daily_data')
    @patch('handler.s3')
    @patch('handler.ssm')
    def test_successful_ingestion(self, mock_ssm, mock_s3, mock_fetch):
        """Verify handler fetches data, transforms, and uploads to S3."""
        # Mock SSM
        mock_ssm.get_parameter.return_value = {
            'Parameter': {'Value': 'test-token'}
        }
        
        # Mock fetch_daily_data
        mock_fetch.return_value = [{
            "id": "r1", "day": "2026-02-17", "score": 85,
            "temperature_deviation": 0.0, "temperature_trend_deviation": 0.0,
            "timestamp": "2026-02-17T00:00:00+00:00",
            "contributors": {
                "activity_balance": 80, "body_temperature": 100,
                "hrv_balance": 78, "previous_day_activity": 80,
                "previous_night": 77, "recovery_index": 94,
                "resting_heart_rate": 100, "sleep_balance": 86,
                "sleep_regularity": 83
            }
        }]
        
        # Run handler
        event = {"date": "2026-02-17"}
        context = MagicMock()
        
        result = lambda_handler(event, context)
        
        # Verify SSM call
        mock_ssm.get_parameter.assert_called_once_with(
            Name='/bio-lakehouse/oura-api-token',
            WithDecryption=True
        )
        
        # Verify fetch called for each type
        self.assertEqual(mock_fetch.call_count, 3)
        
        # Verify S3 PutObject calls
        self.assertEqual(mock_s3.put_object.call_count, 3)
        
        # Verify AES256 encryption on all calls
        for call_item in mock_s3.put_object.call_args_list:
            kwargs = call_item[1]
            self.assertEqual(kwargs['ServerSideEncryption'], 'AES256')
        
        # Verify S3 key structure
        first_call = mock_s3.put_object.call_args_list[0][1]
        key = first_call['Key']
        self.assertIn('oura/readiness/year=2026/month=02/day=17/', key)
        
        # Verify success response
        self.assertEqual(result['statusCode'], 200)
        body = json.loads(result['body'])
        self.assertEqual(body['results'][0]['records'], 1)


if __name__ == '__main__':
    unittest.main()
