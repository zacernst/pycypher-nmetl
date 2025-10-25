import pytest
from unittest.mock import Mock, patch
from typing import Dict, Any
import json
from pathlib import Path

from threshold_recommend.config import ThresholdRecommendSettings
from threshold_recommend.models.request_models import (
    ThresholdRecommendationRequest,
    ThresholdContext
)

@pytest.fixture
def mock_settings():
    """Mock settings for testing"""
    return ThresholdRecommendSettings(
        aws_access_key_id="test_key",
        aws_secret_access_key="test_secret",
        api_key="test_api_key",
        default_account_id="123456789",
        artifactory_username="test_user",
        artifactory_password="test_password"
    )

@pytest.fixture
def sample_nrql_query():
    """Sample NRQL query for testing"""
    return "SELECT percentile(duration, 99) FROM PageView FACET appName"

@pytest.fixture
def sample_threshold_request(sample_nrql_query):
    """Sample threshold recommendation request"""
    return ThresholdRecommendationRequest(
        account_id="123456789",
        nrql_query=sample_nrql_query,
        context=ThresholdContext(
            threshold_operator="above",
            sensitivity=0.8
        )
    )

@pytest.fixture
def mock_nrdb_response():
    """Mock NRDB response data"""
    return {
        "data": {
            "actor": {
                "account": {
                    "nrql": {
                        "results": [
                            {"facet": "app1", "percentile": 1500.0, "timestamp": 1640995200000},
                            {"facet": "app2", "percentile": 2000.0, "timestamp": 1640995200000},
                        ]
                    }
                }
            }
        }
    }

@pytest.fixture
def sample_background_context():
    """Sample background context configuration"""
    return {
        "slowest_contentful_paint_99th": {
            "historical_window": "PT8H",
            "timeseries_window": "PT5M",
            "minimum_data_points": 10,
            "nrql": "SELECT percentile(duration, 99) FROM PageView"
        }
    }

@pytest.fixture
def sample_rules():
    """Sample rules configuration"""
    return '''
    "slowest_contentful_paint_99th" ->
    IsHost = (HistogramGap * 10.0)
    (IsAPMThroughput AND NOT IsCPUUsage) = HistogramGap
    TRUE = (HistogramGap * Sensitivity)
    '''

@pytest.fixture
def mock_s3_client():
    """Mock S3 client for testing"""
    with patch('boto3.client') as mock_client:
        mock_s3 = Mock()
        mock_client.return_value = mock_s3
        yield mock_s3
