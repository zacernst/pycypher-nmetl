from prometheus_client import Counter, Histogram, Gauge, start_http_server

REQUEST_TIME: Histogram = Histogram('request_processing_seconds', 'Time spent processing request')
ROWS_QUEUED: Counter = Counter('rows_queued', 'Rows queued')
ROW_PROCESSING_TIME: Histogram = Histogram('row_processing_seconds', 'Time spent processing row')
FACTS_APPENDED: Counter = Counter('facts_appended', 'Facts appended')
FACTS_APPENDED.inc(0)
SOLUTIONS_TIMER: Histogram = Histogram('solutions_histogram', 'Time spent solving constraints')

start_http_server(8000)