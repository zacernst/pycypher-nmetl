"""
Resolve the current filename of the USAspending FY2025 "All Contracts Full"
archive.

The Snakefile hardcodes a dated filename
(``FY2025_All_Contracts_Full_20260706.zip``) because the archive is
periodically regenerated under a new date suffix (see
``rule download_fy2025_contracts`` in ``Snakefile``, source:
https://files.usaspending.gov/award_data_archive/). That source path is a
public S3 bucket ("dti-usaspending-monthly-downloads") fronted by
files.usaspending.gov, and querying it without a specific object name
returns a standard S3 ListObjectsV2/ListBucket XML listing that can be
filtered with a ``prefix`` query parameter -- no API key required.

Usage
-----
As a CLI::

    uv run python -m fastopendata.processing.resolve_fy2025_contracts_filename

As a module::

    from fastopendata.processing.resolve_fy2025_contracts_filename import (
        resolve_latest_filename,
    )
    filename = resolve_latest_filename()  # "FY2025_All_Contracts_Full_20260706.zip"
"""

from __future__ import annotations

import re
import sys
import urllib.parse
import urllib.request
from xml.etree import ElementTree

BASE_URL = "https://files.usaspending.gov/award_data_archive/"
PREFIX = "FY2025_All_Contracts_Full"
FILENAME_RE = re.compile(r"^FY2025_All_Contracts_Full_(\d{8})\.zip$")
_S3_NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
_USER_AGENT = "fastopendata-pipeline/1.0"
_TIMEOUT_SECONDS = 30


def _list_bucket_keys(base_url: str, prefix: str) -> list[str]:
    """Page through the S3 bucket listing and return every object key
    under ``prefix``."""
    keys: list[str] = []
    marker = ""
    while True:
        params = {"prefix": prefix}
        if marker:
            params["marker"] = marker
        url = f"{base_url}?{urllib.parse.urlencode(params)}"
        request = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
        with urllib.request.urlopen(request, timeout=_TIMEOUT_SECONDS) as response:
            body = response.read()

        root = ElementTree.fromstring(body)
        contents = root.findall("s3:Contents", _S3_NS)
        page_keys = [c.findtext("s3:Key", namespaces=_S3_NS) for c in contents]
        keys.extend(k for k in page_keys if k is not None)

        is_truncated = root.findtext("s3:IsTruncated", namespaces=_S3_NS) == "true"
        if not is_truncated or not page_keys:
            break
        marker = page_keys[-1]

    return keys


def resolve_latest_filename(base_url: str = BASE_URL, prefix: str = PREFIX) -> str:
    """Return the most recent ``FY2025_All_Contracts_Full_<YYYYMMDD>.zip``
    filename currently hosted at ``base_url``.

    Raises ``RuntimeError`` if no matching file is found.
    """
    keys = _list_bucket_keys(base_url, prefix)
    matches = [k for k in keys if FILENAME_RE.match(k)]
    if not matches:
        raise RuntimeError(
            f"No files matching '{prefix}_<YYYYMMDD>.zip' found under {base_url}"
        )
    # YYYYMMDD zero-pads and sorts lexicographically, so the max string is
    # the most recently dated file.
    return max(matches)


if __name__ == "__main__":
    try:
        print(resolve_latest_filename())
    except (RuntimeError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        sys.exit(1)
