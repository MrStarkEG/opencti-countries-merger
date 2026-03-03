"""Shared fixtures for tests."""

import pytest

from opencti_country_merger.config import Settings


@pytest.fixture
def settings() -> Settings:
    return Settings(
        elasticsearch_url="http://localhost:9200",
        elasticsearch_username="",
        elasticsearch_password="",
        elasticsearch_index_prefix="opencti_",
    )
