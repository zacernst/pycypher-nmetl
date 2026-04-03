"""Query performance analytics for fastopendata.

Provides real-time metrics collection, historical trend analysis,
bottleneck identification, and optimization recommendations for
Cypher query execution.
"""

from fastopendata.analytics.collector import MetricsCollector, QueryMetric
from fastopendata.analytics.engine import AnalyticsEngine, PerformanceSummary
from fastopendata.analytics.regression import (
    RegressionAlert,
    RegressionDetector,
)

__all__ = [
    "AnalyticsEngine",
    "MetricsCollector",
    "PerformanceSummary",
    "QueryMetric",
    "RegressionAlert",
    "RegressionDetector",
]
