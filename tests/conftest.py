import sys
import os
import pytest

# Add project root to path so tests can import helpers, config, etc.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def pytest_addoption(parser):
    parser.addoption("--run-integration", action="store_true", default=False)


def pytest_collection_modifyitems(config, items):
    # Only skip tests that are explicitly marked @pytest.mark.integration (unit tests have no marker)
    on_databricks = "DATABRICKS_RUNTIME_VERSION" in os.environ
    run_integration = config.getoption("--run-integration") or on_databricks

    if not run_integration:
        skip = pytest.mark.skip(reason="needs --run-integration (or run on Databricks)")
        for item in items:
            if item.get_closest_marker("integration") is not None:
                item.add_marker(skip)
