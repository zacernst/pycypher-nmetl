"""Tests for SSRF (Server-Side Request Forgery) protection in URI validation.

Verifies that validate_uri_scheme() blocks requests to private/internal IP
ranges including RFC 1918, loopback, link-local, and internal hostnames.

Run with:
    uv run pytest tests/test_ssrf_protection.py -v
"""

from __future__ import annotations

import pytest
from pycypher.ingestion.security import (
    SecurityError,
    _check_ssrf_hostname,
    validate_uri_scheme,
)

# ---------------------------------------------------------------------------
# Direct _check_ssrf_hostname tests — literal IP addresses
# ---------------------------------------------------------------------------


class TestSSRFBlocksPrivateIPv4:
    """RFC 1918 private IPv4 ranges must be blocked."""

    def test_10_0_0_0_range(self) -> None:
        with pytest.raises(SecurityError, match="SSRF.*private"):
            _check_ssrf_hostname("10.0.0.1")

    def test_10_255_255_255(self) -> None:
        with pytest.raises(SecurityError, match="SSRF.*private"):
            _check_ssrf_hostname("10.255.255.255")

    def test_172_16_range(self) -> None:
        with pytest.raises(SecurityError, match="SSRF.*private"):
            _check_ssrf_hostname("172.16.0.1")

    def test_172_31_range(self) -> None:
        with pytest.raises(SecurityError, match="SSRF.*private"):
            _check_ssrf_hostname("172.31.255.255")

    def test_192_168_range(self) -> None:
        with pytest.raises(SecurityError, match="SSRF.*private"):
            _check_ssrf_hostname("192.168.1.1")

    def test_192_168_0_0(self) -> None:
        with pytest.raises(SecurityError, match="SSRF.*private"):
            _check_ssrf_hostname("192.168.0.0")


class TestSSRFBlocksLoopback:
    """Loopback addresses must be blocked."""

    def test_127_0_0_1(self) -> None:
        with pytest.raises(SecurityError, match="SSRF.*private"):
            _check_ssrf_hostname("127.0.0.1")

    def test_127_0_0_2(self) -> None:
        """Entire 127.x.x.x range is loopback."""
        with pytest.raises(SecurityError, match="SSRF.*private"):
            _check_ssrf_hostname("127.0.0.2")

    def test_127_255_255_255(self) -> None:
        with pytest.raises(SecurityError, match="SSRF.*private"):
            _check_ssrf_hostname("127.255.255.255")


class TestSSRFBlocksLinkLocal:
    """Link-local addresses (169.254.x.x) must be blocked."""

    def test_169_254_range(self) -> None:
        with pytest.raises(SecurityError, match="SSRF.*private"):
            _check_ssrf_hostname("169.254.1.1")

    def test_aws_metadata_endpoint(self) -> None:
        """AWS metadata endpoint 169.254.169.254 is a prime SSRF target."""
        with pytest.raises(SecurityError, match="SSRF.*private"):
            _check_ssrf_hostname("169.254.169.254")


class TestSSRFBlocksSpecialAddresses:
    """Other special addresses must be blocked."""

    def test_0_0_0_0(self) -> None:
        with pytest.raises(SecurityError, match="SSRF"):
            _check_ssrf_hostname("0.0.0.0")


class TestSSRFBlocksInternalHostnames:
    """Well-known internal hostnames must be blocked."""

    def test_localhost(self) -> None:
        with pytest.raises(SecurityError, match="SSRF.*local"):
            _check_ssrf_hostname("localhost")

    def test_localhost_case_insensitive(self) -> None:
        with pytest.raises(SecurityError, match="SSRF.*local"):
            _check_ssrf_hostname("LOCALHOST")

    def test_localhost_localdomain(self) -> None:
        with pytest.raises(SecurityError, match="SSRF.*local"):
            _check_ssrf_hostname("localhost.localdomain")

    def test_ip6_localhost(self) -> None:
        with pytest.raises(SecurityError, match="SSRF.*local"):
            _check_ssrf_hostname("ip6-localhost")


class TestSSRFBlocksIPv6Private:
    """IPv6 private/loopback addresses must be blocked."""

    def test_ipv6_loopback(self) -> None:
        with pytest.raises(SecurityError, match="SSRF.*private"):
            _check_ssrf_hostname("::1")

    def test_ipv6_link_local(self) -> None:
        with pytest.raises(SecurityError, match="SSRF.*private"):
            _check_ssrf_hostname("fe80::1")


# ---------------------------------------------------------------------------
# Integration via validate_uri_scheme
# ---------------------------------------------------------------------------


class TestSSRFViaValidateUriScheme:
    """SSRF checks must be triggered by validate_uri_scheme for HTTP(S)."""

    def test_http_private_ip_blocked(self) -> None:
        with pytest.raises(SecurityError, match="SSRF"):
            validate_uri_scheme("http://192.168.1.1:8080/data.csv")

    def test_https_private_ip_blocked(self) -> None:
        with pytest.raises(SecurityError, match="SSRF"):
            validate_uri_scheme("https://10.0.0.5/api/data")

    def test_http_localhost_blocked(self) -> None:
        with pytest.raises(SecurityError, match="SSRF"):
            validate_uri_scheme("http://localhost:3000/internal")

    def test_http_loopback_blocked(self) -> None:
        with pytest.raises(SecurityError, match="SSRF"):
            validate_uri_scheme("http://127.0.0.1:9090/metrics")

    def test_http_aws_metadata_blocked(self) -> None:
        """Classic cloud SSRF attack vector."""
        with pytest.raises(SecurityError, match="SSRF"):
            validate_uri_scheme("http://169.254.169.254/latest/meta-data/")

    def test_non_http_schemes_skip_ssrf_check(self) -> None:
        """SSRF check only applies to HTTP(S) — S3, GCS etc. are fine."""
        validate_uri_scheme("s3://my-bucket/data.csv")
        validate_uri_scheme("gs://my-bucket/data.csv")

    def test_file_scheme_skips_ssrf_check(self) -> None:
        validate_uri_scheme("file:///home/user/data.csv")

    def test_bare_path_skips_ssrf_check(self) -> None:
        validate_uri_scheme("/home/user/data.csv")

    def test_database_scheme_skips_ssrf_check(self) -> None:
        validate_uri_scheme("postgresql://user:pass@db.example.com/mydb")


class TestSSRFAllowsPublicAddresses:
    """Public IP addresses and hostnames must be allowed."""

    def test_public_ip(self) -> None:
        _check_ssrf_hostname("8.8.8.8")

    def test_public_hostname(self) -> None:
        """Well-known public hostnames should pass."""
        # This just checks it doesn't raise — actual DNS may vary.
        _check_ssrf_hostname("example.com")

    def test_172_15_is_public(self) -> None:
        """172.15.x.x is NOT in the private range (172.16-31 is)."""
        _check_ssrf_hostname("172.15.255.255")

    def test_172_32_is_public(self) -> None:
        """172.32.x.x is NOT in the private range."""
        _check_ssrf_hostname("172.32.0.1")
