from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    Summary,
    start_http_server,
)

REQUEST_TIME: Histogram = Histogram(
    "request_processing_seconds", "Time spent processing request"
)
ROWS_QUEUED: Counter = Counter("rows_queued", "Rows queued")
TRIGGER_CHECK_COUNT: Counter = Counter("triggers_checked", "Triggers checked")
ROW_PROCESSING_TIME: Histogram = Histogram(
    "row_processing_seconds", "Time spent processing row"
)
FACTS_APPENDED: Counter = Counter("facts_appended", "Facts appended")
# FACTS_APPENDED.inc(0)
SOLUTIONS_TIMER: Histogram = Histogram(
    "solutions_timer", "Time spent solving constraints"
)
TIME_IN_FDB_ITERATOR: Histogram = Histogram(
    "seconds_in_iterator", "How long each call to the FDB iterator takes"
)
NUMBER_OF_KEYS_SCANNED: Histogram = Histogram(
    "keys_scanned_histogram", "Number of keys scanned per request to iterator"
)

RAW_DATA_COUNTER: Counter = Counter(
    "raw_data_results", "Items queued by RawDataProcessor"
)

FDB_WRITE_TIME = Summary("write_fact_seconds", "Time spent writing to FDB")


try:
    start_http_server(8000)
except:
    pass
