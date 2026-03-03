"""Tests for the CountryMapper service."""

from opencti_country_merger.services.country_mapper import CountryMapper


class TestExactResolution:
    def test_full_name(self) -> None:
        mapper = CountryMapper()
        assert mapper.resolve("United States") == "US"
        assert mapper.resolve("France") == "FR"
        assert mapper.resolve("Germany") == "DE"

    def test_case_insensitive(self) -> None:
        mapper = CountryMapper()
        assert mapper.resolve("united states") == "US"
        assert mapper.resolve("FRANCE") == "FR"

    def test_alpha2_code(self) -> None:
        mapper = CountryMapper()
        assert mapper.resolve("US") == "US"
        assert mapper.resolve("GB") == "GB"
        assert mapper.resolve("fr") == "FR"

    def test_alpha3_code(self) -> None:
        mapper = CountryMapper()
        assert mapper.resolve("USA") == "US"
        assert mapper.resolve("GBR") == "GB"
        assert mapper.resolve("fra") == "FR"


class TestCustomAliases:
    def test_america(self) -> None:
        mapper = CountryMapper()
        assert mapper.resolve("America") == "US"

    def test_uk(self) -> None:
        mapper = CountryMapper()
        assert mapper.resolve("UK") == "GB"
        assert mapper.resolve("Britain") == "GB"
        assert mapper.resolve("England") == "GB"

    def test_russia(self) -> None:
        mapper = CountryMapper()
        assert mapper.resolve("Russia") == "RU"

    def test_iran(self) -> None:
        mapper = CountryMapper()
        assert mapper.resolve("Iran") == "IR"

    def test_south_korea(self) -> None:
        mapper = CountryMapper()
        assert mapper.resolve("South Korea") == "KR"

    def test_north_korea(self) -> None:
        mapper = CountryMapper()
        assert mapper.resolve("North Korea") == "KP"
        assert mapper.resolve("DPRK") == "KP"

    def test_czech_republic(self) -> None:
        mapper = CountryMapper()
        assert mapper.resolve("Czech Republic") == "CZ"
        assert mapper.resolve("Czechia") == "CZ"

    def test_burma(self) -> None:
        mapper = CountryMapper()
        assert mapper.resolve("Burma") == "MM"


class TestFuzzyResolution:
    def test_misspelling(self) -> None:
        mapper = CountryMapper(fuzzy_threshold=80)
        # Close enough to "Germany"
        result = mapper.resolve("Germny")
        assert result == "DE"

    def test_below_threshold(self) -> None:
        mapper = CountryMapper(fuzzy_threshold=95)
        # Too far from anything meaningful
        result = mapper.resolve("xyzabc")
        assert result is None


class TestResolveEntity:
    def test_from_name(self) -> None:
        mapper = CountryMapper()
        entity = {"name": "United States"}
        assert mapper.resolve_entity(entity) == "US"

    def test_from_alias(self) -> None:
        mapper = CountryMapper()
        entity = {"name": "Unknown Place", "x_opencti_aliases": ["USA"]}
        assert mapper.resolve_entity(entity) == "US"

    def test_from_aliases_field(self) -> None:
        mapper = CountryMapper()
        entity = {"name": "Nowhere", "aliases": ["America"]}
        assert mapper.resolve_entity(entity) == "US"

    def test_unresolvable(self) -> None:
        mapper = CountryMapper()
        entity = {"name": "Planet Mars"}
        assert mapper.resolve_entity(entity) is None

    def test_empty_name(self) -> None:
        mapper = CountryMapper()
        assert mapper.resolve("") is None
        assert mapper.resolve("  ") is None
