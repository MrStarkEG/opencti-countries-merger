"""Map country names (and aliases) to ISO 3166-1 alpha-2 codes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pycountry
from thefuzz import fuzz


# Common CTI aliases that pycountry doesn't know about.
_CUSTOM_ALIASES: dict[str, str] = {
    "america": "US",
    "united states of america": "US",
    "usa": "US",
    "u.s.a.": "US",
    "u.s.": "US",
    "uk": "GB",
    "united kingdom of great britain and northern ireland": "GB",
    "britain": "GB",
    "great britain": "GB",
    "england": "GB",
    "russia": "RU",
    "russian federation": "RU",
    "iran": "IR",
    "islamic republic of iran": "IR",
    "south korea": "KR",
    "republic of korea": "KR",
    "north korea": "KP",
    "democratic people's republic of korea": "KP",
    "dprk": "KP",
    "taiwan": "TW",
    "china": "CN",
    "people's republic of china": "CN",
    "prc": "CN",
    "vietnam": "VN",
    "viet nam": "VN",
    "syria": "SY",
    "syrian arab republic": "SY",
    "venezuela": "VE",
    "bolivia": "BO",
    "tanzania": "TZ",
    "ivory coast": "CI",
    "cote d'ivoire": "CI",
    "czechia": "CZ",
    "czech republic": "CZ",
    "palestine": "PS",
    "state of palestine": "PS",
    "eswatini": "SZ",
    "swaziland": "SZ",
    "burma": "MM",
    "myanmar": "MM",
    "laos": "LA",
    "lao people's democratic republic": "LA",
    "macau": "MO",
    "macao": "MO",
    "hong kong": "HK",
    "the netherlands": "NL",
    "holland": "NL",
    # Misnamed codes found in OpenCTI data
    "sp": "ES",        # Spain (wrong code)
    "tu": "TR",        # Turkey (wrong code)
    "an": "NL",        # Netherlands Antilles (dissolved → NL)
    # Kosovo — user-assigned code XK (not official ISO but widely used)
    "kosovo": "XK",
    "xk": "XK",
}

# Entities that are not real countries and should be deleted (with their
# relationships cleaned up).  Keyed by name as stored in ES.
JUNK_COUNTRY_NAMES: set[str] = {
    "??",
    "EU",
    "QC",
}


@dataclass
class CountryInfo:
    """Full country metadata for a resolved ISO code."""
    name: str          # Standard name (e.g. "United States")
    alpha_2: str       # ISO 3166-1 alpha-2 (e.g. "US")
    alpha_3: str       # ISO 3166-1 alpha-3 (e.g. "USA")


# Special entries not in pycountry
_SPECIAL_COUNTRIES: dict[str, CountryInfo] = {
    "XK": CountryInfo(name="Kosovo", alpha_2="XK", alpha_3="XKX"),
}


class CountryMapper:
    """Resolve free-text country names to ISO 3166-1 alpha-2 codes."""

    def __init__(self, fuzzy_threshold: int = 80) -> None:
        self._threshold = fuzzy_threshold
        self._exact: dict[str, str] = {}
        self._build_lookup()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def resolve(self, name: str) -> str | None:
        """Return the ISO alpha-2 code for *name*, or ``None``."""
        normalised = name.strip().lower()
        if not normalised:
            return None

        # 1. Exact match (includes custom aliases)
        if normalised in self._exact:
            return self._exact[normalised]

        # 2. Check if name is already a valid alpha-2 or alpha-3 code
        upper = name.strip().upper()
        if len(upper) == 2:
            try:
                pycountry.countries.get(alpha_2=upper)
                return upper
            except (LookupError, KeyError):
                pass
        if len(upper) == 3:
            try:
                country = pycountry.countries.get(alpha_3=upper)
                if country:
                    return country.alpha_2
            except (LookupError, KeyError):
                pass

        # 3. Fuzzy match against all known names
        best_score = 0
        best_code: str | None = None
        for known_name, code in self._exact.items():
            score = fuzz.token_sort_ratio(normalised, known_name)
            if score > best_score:
                best_score = score
                best_code = code
        if best_score >= self._threshold:
            return best_code
        return None

    def get_country_info(self, alpha_2: str) -> CountryInfo:
        """Return full country metadata for an alpha-2 code."""
        if alpha_2 in _SPECIAL_COUNTRIES:
            return _SPECIAL_COUNTRIES[alpha_2]
        country = pycountry.countries.get(alpha_2=alpha_2)
        if country is None:
            return CountryInfo(name=alpha_2, alpha_2=alpha_2, alpha_3=alpha_2)
        name = getattr(country, "common_name", None) or country.name
        return CountryInfo(
            name=name,
            alpha_2=country.alpha_2,
            alpha_3=country.alpha_3,
        )

    def resolve_entity(self, entity: dict[str, Any]) -> str | None:
        """Try to resolve an entity dict (``name``, aliases, etc.)."""
        # Try the name field first
        name = entity.get("name", "")
        code = self.resolve(name)
        if code:
            return code

        # Try x_opencti_aliases
        for alias in entity.get("x_opencti_aliases", []) or []:
            code = self.resolve(alias)
            if code:
                return code

        # Try aliases
        for alias in entity.get("aliases", []) or []:
            code = self.resolve(alias)
            if code:
                return code

        return None

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _build_lookup(self) -> None:
        """Populate the exact-match lookup from pycountry + custom aliases."""
        for country in pycountry.countries:
            code = country.alpha_2
            self._exact[country.name.lower()] = code
            if hasattr(country, "official_name"):
                self._exact[country.official_name.lower()] = code
            if hasattr(country, "common_name"):
                self._exact[country.common_name.lower()] = code
            self._exact[country.alpha_2.lower()] = code
            self._exact[country.alpha_3.lower()] = code

        for alias, code in _CUSTOM_ALIASES.items():
            self._exact[alias.lower()] = code
