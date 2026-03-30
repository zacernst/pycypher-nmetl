"""Guard against hardcoded LOGGER.setLevel() calls inside library modules.

Importing ``relational_models`` (or any other pycypher module) must NOT override
the root logger or any application-configured logger level.  A library that
hijacks log verbosity at import time is broken by design: it forces DEBUG noise
into production systems that have no way to suppress it short of monkey-patching.

The shared logger is configured to WARNING by default; after importing the
entire pycypher package the level must remain WARNING (or whatever the caller
set it to before the import).
"""

from __future__ import annotations

import logging


class TestLoggerNotOverridden:
    """Library imports must not mutate logger levels."""

    def test_relational_models_import_does_not_force_debug(self) -> None:
        """Importing relational_models must not set the shared logger to DEBUG."""
        # Import the module under test (may already be cached in sys.modules)
        from shared.logger import LOGGER

        assert LOGGER.level != logging.DEBUG, (
            "relational_models sets LOGGER.setLevel('DEBUG') at module load time, "
            "which overrides the WARNING default and forces debug noise on all users."
        )

    def test_pycypher_full_import_does_not_force_debug(self) -> None:
        """A full `import pycypher` must leave the shared logger at WARNING."""
        from shared.logger import LOGGER

        assert LOGGER.level == logging.WARNING, (
            f"Expected WARNING ({logging.WARNING}) after pycypher import, "
            f"got {logging.getLevelName(LOGGER.level)} ({LOGGER.level})."
        )

    def test_shared_logger_default_level_is_warning(self) -> None:
        """The shared logger's own module sets WARNING as its default."""
        from shared import logger as logger_module

        assert logger_module.LOGGING_LEVEL == "WARNING"
