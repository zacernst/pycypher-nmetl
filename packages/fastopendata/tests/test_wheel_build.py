"""Regression test for the fastopendata wheel-build file selection.

`pyproject.toml`'s ``[tool.hatch.build.targets.wheel]`` table once had a typo
(``tool.hatchling`` instead of ``tool.hatch``) that hatchling silently
ignored, so the ``exclude`` patterns never applied and the untracked,
gitignored ``src/fastopendata/output/`` scratch directory (tens of MB of CSVs)
shipped inside every built wheel. This test builds the wheel and asserts
that doesn't happen again.
"""

from __future__ import annotations

import shutil
import subprocess
import zipfile
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]

# Generous ceiling: the real wheel is under 1MB; this just needs to catch a
# ~90MB regression, not enforce a tight budget.
_MAX_WHEEL_SIZE_BYTES = 5 * 1024 * 1024


@pytest.mark.slow
@pytest.mark.timeout(60)
@pytest.mark.skipif(shutil.which("uv") is None, reason="uv not on PATH")
def test_wheel_excludes_output_and_stays_small(tmp_path: Path) -> None:
    subprocess.run(
        [
            "uv",
            "build",
            "--wheel",
            "--out-dir",
            str(tmp_path),
            str(REPO_ROOT / "packages" / "fastopendata"),
        ],
        check=True,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )

    wheels = list(tmp_path.glob("fastopendata-*.whl"))
    assert len(wheels) == 1, f"expected exactly one wheel, got {wheels}"
    wheel = wheels[0]

    assert wheel.stat().st_size < _MAX_WHEEL_SIZE_BYTES, (
        f"{wheel.name} is {wheel.stat().st_size} bytes, "
        f"exceeding the {_MAX_WHEEL_SIZE_BYTES} byte ceiling"
    )

    with zipfile.ZipFile(wheel) as zf:
        names = zf.namelist()
    output_entries = [n for n in names if "/output/" in n]
    assert not output_entries, f"wheel unexpectedly contains: {output_entries}"
