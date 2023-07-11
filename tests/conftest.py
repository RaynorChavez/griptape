import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--include-integration-tests", action="store_true", default=False, help="Run integration tests"
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--include-integration-tests"):
        return
    skip_integration_tests = pytest.mark.skip(reason="need --include-integration-tests option to run")
    for item in items:
        if "integrationtest" in item.keywords:
            item.add_marker(skip_integration_tests)