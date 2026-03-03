"""Exhaustive test: every pycountry entry and custom alias resolves correctly."""

import pytest
import pycountry

from opencti_country_merger.services.country_mapper import CountryMapper, _CUSTOM_ALIASES


@pytest.fixture(scope="module")
def mapper() -> CountryMapper:
    """Single mapper instance shared across all tests in this module."""
    return CountryMapper()


# ---------------------------------------------------------------------------
# Build parametrized data from pycountry
# ---------------------------------------------------------------------------

_ALL_COUNTRIES = list(pycountry.countries)

_BY_NAME = [(c.name, c.alpha_2) for c in _ALL_COUNTRIES]

_BY_ALPHA2 = [(c.alpha_2, c.alpha_2) for c in _ALL_COUNTRIES]

_BY_ALPHA3 = [(c.alpha_3, c.alpha_2) for c in _ALL_COUNTRIES]

_BY_OFFICIAL_NAME = [
    (c.official_name, c.alpha_2)
    for c in _ALL_COUNTRIES
    if hasattr(c, "official_name")
]

_BY_COMMON_NAME = [
    (c.common_name, c.alpha_2)
    for c in _ALL_COUNTRIES
    if hasattr(c, "common_name")
]

_CUSTOM_ALIAS_PAIRS = [
    (alias, expected_code) for alias, expected_code in _CUSTOM_ALIASES.items()
]


# ---------------------------------------------------------------------------
# Tests — every country by name
# ---------------------------------------------------------------------------

class TestAllCountriesByName:
    @pytest.mark.parametrize("name,expected", _BY_NAME, ids=[c[0] for c in _BY_NAME])
    def test_resolve_by_name(self, mapper: CountryMapper, name: str, expected: str) -> None:
        assert mapper.resolve(name) == expected

    @pytest.mark.parametrize("name,expected", _BY_NAME, ids=[f"{c[0]}_lower" for c in _BY_NAME])
    def test_resolve_by_name_lowercase(self, mapper: CountryMapper, name: str, expected: str) -> None:
        assert mapper.resolve(name.lower()) == expected

    @pytest.mark.parametrize("name,expected", _BY_NAME, ids=[f"{c[0]}_upper" for c in _BY_NAME])
    def test_resolve_by_name_uppercase(self, mapper: CountryMapper, name: str, expected: str) -> None:
        assert mapper.resolve(name.upper()) == expected


# ---------------------------------------------------------------------------
# Tests — every country by alpha-2
# ---------------------------------------------------------------------------

class TestAllCountriesByAlpha2:
    @pytest.mark.parametrize("code,expected", _BY_ALPHA2, ids=[c[0] for c in _BY_ALPHA2])
    def test_resolve_by_alpha2(self, mapper: CountryMapper, code: str, expected: str) -> None:
        assert mapper.resolve(code) == expected

    @pytest.mark.parametrize("code,expected", _BY_ALPHA2, ids=[f"{c[0]}_lower" for c in _BY_ALPHA2])
    def test_resolve_by_alpha2_lowercase(self, mapper: CountryMapper, code: str, expected: str) -> None:
        assert mapper.resolve(code.lower()) == expected


# ---------------------------------------------------------------------------
# Tests — every country by alpha-3
# ---------------------------------------------------------------------------

class TestAllCountriesByAlpha3:
    @pytest.mark.parametrize("code,expected", _BY_ALPHA3, ids=[c[0] for c in _BY_ALPHA3])
    def test_resolve_by_alpha3(self, mapper: CountryMapper, code: str, expected: str) -> None:
        assert mapper.resolve(code) == expected

    @pytest.mark.parametrize("code,expected", _BY_ALPHA3, ids=[f"{c[0]}_lower" for c in _BY_ALPHA3])
    def test_resolve_by_alpha3_lowercase(self, mapper: CountryMapper, code: str, expected: str) -> None:
        assert mapper.resolve(code.lower()) == expected


# ---------------------------------------------------------------------------
# Tests — official names
# ---------------------------------------------------------------------------

class TestAllCountriesByOfficialName:
    @pytest.mark.parametrize(
        "name,expected",
        _BY_OFFICIAL_NAME,
        ids=[c[0] for c in _BY_OFFICIAL_NAME],
    )
    def test_resolve_by_official_name(self, mapper: CountryMapper, name: str, expected: str) -> None:
        assert mapper.resolve(name) == expected


# ---------------------------------------------------------------------------
# Tests — common names
# ---------------------------------------------------------------------------

class TestAllCountriesByCommonName:
    @pytest.mark.parametrize(
        "name,expected",
        _BY_COMMON_NAME,
        ids=[c[0] for c in _BY_COMMON_NAME],
    )
    def test_resolve_by_common_name(self, mapper: CountryMapper, name: str, expected: str) -> None:
        assert mapper.resolve(name) == expected


# ---------------------------------------------------------------------------
# Tests — every custom CTI alias
# ---------------------------------------------------------------------------

class TestAllCustomAliases:
    @pytest.mark.parametrize(
        "alias,expected",
        _CUSTOM_ALIAS_PAIRS,
        ids=[c[0] for c in _CUSTOM_ALIAS_PAIRS],
    )
    def test_custom_alias(self, mapper: CountryMapper, alias: str, expected: str) -> None:
        assert mapper.resolve(alias) == expected

    @pytest.mark.parametrize(
        "alias,expected",
        _CUSTOM_ALIAS_PAIRS,
        ids=[f"{c[0]}_title" for c in _CUSTOM_ALIAS_PAIRS],
    )
    def test_custom_alias_title_case(self, mapper: CountryMapper, alias: str, expected: str) -> None:
        assert mapper.resolve(alias.title()) == expected


# ---------------------------------------------------------------------------
# Tests — entity resolution for every country
# ---------------------------------------------------------------------------

class TestAllCountriesEntityResolution:
    @pytest.mark.parametrize(
        "name,expected",
        _BY_NAME,
        ids=[f"entity_{c[0]}" for c in _BY_NAME],
    )
    def test_resolve_entity_by_name(self, mapper: CountryMapper, name: str, expected: str) -> None:
        entity = {"name": name, "entity_type": "Country"}
        assert mapper.resolve_entity(entity) == expected

    @pytest.mark.parametrize(
        "alias,expected",
        _CUSTOM_ALIAS_PAIRS,
        ids=[f"entity_alias_{c[0]}" for c in _CUSTOM_ALIAS_PAIRS],
    )
    def test_resolve_entity_via_alias_field(self, mapper: CountryMapper, alias: str, expected: str) -> None:
        entity = {"name": "Unknown", "x_opencti_aliases": [alias]}
        assert mapper.resolve_entity(entity) == expected
