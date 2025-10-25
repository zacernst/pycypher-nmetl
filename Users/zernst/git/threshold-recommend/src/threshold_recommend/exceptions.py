from typing import Optional, Dict, Any

class ThresholdRecommendError(Exception):
    """Base exception for threshold recommendation errors"""
    
    def __init__(self, message: str, error_code: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
        self.message = message
        self.error_code = error_code
        self.details = details or {}
        super().__init__(self.message)

class NRQLSyntaxError(ThresholdRecommendError):
    """Raised when NRQL query has syntax errors"""
    
    def __init__(self, query: str, parse_error: str):
        super().__init__(
            message=f"Invalid NRQL syntax: {parse_error}",
            error_code="NRQL_SYNTAX_ERROR",
            details={"query": query, "parse_error": parse_error}
        )

class ConfigurationNotFoundError(ThresholdRecommendError):
    """Raised when no matching configuration is found for a query"""
    
    def __init__(self, query_pattern: str):
        super().__init__(
            message=f"No configuration found for query pattern: {query_pattern}",
            error_code="CONFIG_NOT_FOUND",
            details={"query_pattern": query_pattern}
        )

class InsufficientDataError(ThresholdRecommendError):
    """Raised when there's insufficient data for analysis"""
    
    def __init__(self, data_points: int, minimum_required: int):
        super().__init__(
            message=f"Insufficient data points: {data_points} (minimum: {minimum_required})",
            error_code="INSUFFICIENT_DATA",
            details={"data_points": data_points, "minimum_required": minimum_required}
        )

class NRDBQueryError(ThresholdRecommendError):
    """Raised when NRDB query fails"""
    
    def __init__(self, query: str, nrdb_error: str):
        super().__init__(
            message=f"NRDB query failed: {nrdb_error}",
            error_code="NRDB_QUERY_ERROR",
            details={"query": query, "nrdb_error": nrdb_error}
        )
