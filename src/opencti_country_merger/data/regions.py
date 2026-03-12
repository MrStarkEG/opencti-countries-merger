"""UN M49 region reference data and normalisation helpers."""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class RegionEntry:
    name: str
    m49_code: str  # empty string if no UN M49 code


def _normalize(name: str) -> str:
    """Lowercase, replace ``-`` and ``_`` with space, strip."""
    return re.sub(r"[-_]+", " ", name.strip()).lower()


# 27 UN M49 sub-regions + 2 non-standard entries kept because they
# commonly appear with relationships in OpenCTI.
UN_REGIONS: list[RegionEntry] = [
    # --- UN M49 macro-regions ---
    RegionEntry("Africa", "002"),
    RegionEntry("Americas", "019"),
    RegionEntry("Asia", "142"),
    RegionEntry("Europe", "150"),
    RegionEntry("Oceania", "009"),
    # --- UN M49 sub-regions ---
    RegionEntry("Northern Africa", "015"),
    RegionEntry("Eastern Africa", "014"),
    RegionEntry("Middle Africa", "017"),
    RegionEntry("Southern Africa", "018"),
    RegionEntry("Western Africa", "011"),
    RegionEntry("Caribbean", "029"),
    RegionEntry("Central America", "013"),
    RegionEntry("South America", "005"),
    RegionEntry("Northern America", "021"),
    RegionEntry("Central Asia", "143"),
    RegionEntry("Eastern Asia", "030"),
    RegionEntry("South-eastern Asia", "035"),
    RegionEntry("Southern Asia", "034"),
    RegionEntry("Western Asia", "145"),
    RegionEntry("Eastern Europe", "151"),
    RegionEntry("Northern Europe", "154"),
    RegionEntry("Southern Europe", "039"),
    RegionEntry("Western Europe", "155"),
    RegionEntry("Australia and New Zealand", "053"),
    RegionEntry("Melanesia", "054"),
    RegionEntry("Micronesia", "057"),
    RegionEntry("Polynesia", "061"),
    # --- Non-standard (no M49 code, but commonly present) ---
    RegionEntry("European Union", ""),
    RegionEntry("Middle East", ""),
]

# Non-standard region names that should be merged into their canonical entry.
# Key: normalised alias → Value: normalised canonical name (must exist in UN_REGIONS).
REGION_ALIASES: dict[str, str] = {
    "north america": "northern america",
}

REGIONS_BY_NORM: dict[str, RegionEntry] = {
    _normalize(r.name): r for r in UN_REGIONS
}

REGIONS_BY_M49: dict[str, RegionEntry] = {
    r.m49_code: r for r in UN_REGIONS if r.m49_code
}

# Sub-region M49 code → parent macro-region M49 code.
SUBREGION_TO_MACRO: dict[str, str] = {
    # Africa
    "015": "002", "014": "002", "017": "002", "018": "002", "011": "002",
    # Americas
    "021": "019", "013": "019", "029": "019", "005": "019",
    # Asia
    "143": "142", "030": "142", "035": "142", "034": "142", "145": "142",
    # Europe
    "151": "150", "154": "150", "039": "150", "155": "150",
    # Oceania
    "053": "009", "054": "009", "057": "009", "061": "009",
}

# ISO alpha-2 → UN M49 sub-region code.
# Every sovereign/dependent territory mapped to its immediate sub-region.
COUNTRY_TO_SUBREGION: dict[str, str] = {
    # --- Northern Africa (015) ---
    "DZ": "015", "EG": "015", "LY": "015", "MA": "015",
    "SD": "015", "TN": "015", "EH": "015",
    # --- Eastern Africa (014) ---
    "BI": "014", "KM": "014", "DJ": "014", "ER": "014",
    "ET": "014", "IO": "014", "KE": "014", "MG": "014",
    "MW": "014", "MU": "014", "MZ": "014", "RE": "014",
    "RW": "014", "SC": "014", "SO": "014", "SS": "014",
    "TF": "014", "TZ": "014", "UG": "014", "YT": "014",
    "ZM": "014", "ZW": "014",
    # --- Middle Africa (017) ---
    "AO": "017", "CM": "017", "CF": "017", "TD": "017",
    "CG": "017", "CD": "017", "GQ": "017", "GA": "017",
    "ST": "017",
    # --- Southern Africa (018) ---
    "BW": "018", "SZ": "018", "LS": "018", "NA": "018",
    "ZA": "018",
    # --- Western Africa (011) ---
    "BJ": "011", "BF": "011", "CV": "011", "CI": "011",
    "GM": "011", "GH": "011", "GN": "011", "GW": "011",
    "LR": "011", "ML": "011", "MR": "011", "NE": "011",
    "NG": "011", "SH": "011", "SN": "011", "SL": "011",
    "TG": "011",
    # --- Northern America (021) ---
    "BM": "021", "CA": "021", "GL": "021", "PM": "021",
    "US": "021",
    # --- Central America (013) ---
    "BZ": "013", "CR": "013", "SV": "013", "GT": "013",
    "HN": "013", "MX": "013", "NI": "013", "PA": "013",
    # --- Caribbean (029) ---
    "AG": "029", "AI": "029", "AW": "029", "BL": "029",
    "BQ": "029", "BS": "029", "BB": "029", "CU": "029",
    "CW": "029", "DM": "029", "DO": "029", "GD": "029",
    "GP": "029", "HT": "029", "JM": "029", "KN": "029", "KY": "029",
    "LC": "029", "MF": "029", "MQ": "029", "MS": "029",
    "PR": "029", "SX": "029", "TC": "029", "TT": "029",
    "VC": "029", "VG": "029", "VI": "029",
    # --- South America (005) ---
    "AR": "005", "BO": "005", "BR": "005", "BV": "005",
    "CL": "005", "CO": "005", "EC": "005", "FK": "005",
    "GF": "005", "GS": "005", "GY": "005", "PY": "005",
    "PE": "005", "SR": "005", "UY": "005", "VE": "005",
    # --- Central Asia (143) ---
    "KZ": "143", "KG": "143", "TJ": "143", "TM": "143",
    "UZ": "143",
    # --- Eastern Asia (030) ---
    "CN": "030", "HK": "030", "JP": "030", "MO": "030",
    "MN": "030", "KP": "030", "KR": "030", "TW": "030",
    # --- South-eastern Asia (035) ---
    "BN": "035", "CC": "035", "CX": "035", "KH": "035",
    "ID": "035", "LA": "035", "MY": "035", "MM": "035",
    "PH": "035", "SG": "035", "TH": "035", "TL": "035",
    "VN": "035",
    # --- Southern Asia (034) ---
    "AF": "034", "BD": "034", "BT": "034", "IN": "034",
    "MV": "034", "NP": "034", "PK": "034", "LK": "034",
    # --- Western Asia (145) ---
    "AM": "145", "AZ": "145", "BH": "145", "CY": "145",
    "GE": "145", "IQ": "145", "IL": "145", "JO": "145",
    "KW": "145", "LB": "145", "OM": "145", "PS": "145",
    "QA": "145", "SA": "145", "SY": "145", "TR": "145",
    "AE": "145", "IR": "145", "YE": "145",
    # --- Eastern Europe (151) ---
    "BY": "151", "BG": "151", "CZ": "151", "HU": "151",
    "MD": "151", "PL": "151", "RO": "151", "RU": "151",
    "SK": "151", "UA": "151",
    # --- Northern Europe (154) ---
    "AX": "154", "DK": "154", "EE": "154", "FI": "154",
    "FO": "154", "GG": "154", "IM": "154", "IS": "154",
    "IE": "154", "JE": "154", "LV": "154", "LT": "154",
    "NO": "154", "SJ": "154", "SE": "154", "GB": "154",
    # --- Southern Europe (039) ---
    "AL": "039", "AD": "039", "BA": "039", "GI": "039",
    "GR": "039", "HR": "039", "IT": "039", "MT": "039",
    "ME": "039", "MK": "039", "PT": "039", "RS": "039",
    "SM": "039", "SI": "039", "ES": "039", "VA": "039",
    "XK": "039",
    # --- Western Europe (155) ---
    "AT": "155", "BE": "155", "FR": "155", "DE": "155",
    "LI": "155", "LU": "155", "MC": "155", "NL": "155",
    "CH": "155",
    # --- Australia and New Zealand (053) ---
    "AU": "053", "HM": "053", "NF": "053", "NZ": "053",
    # --- Melanesia (054) ---
    "FJ": "054", "NC": "054", "PG": "054", "SB": "054",
    "VU": "054",
    # --- Micronesia (057) ---
    "FM": "057", "GU": "057", "KI": "057", "MH": "057",
    "MP": "057", "NR": "057", "PW": "057",
    # --- Polynesia (061) ---
    "AS": "061", "CK": "061", "NU": "061", "PF": "061",
    "PN": "061", "TK": "061", "TO": "061", "TV": "061",
    "UM": "061", "WF": "061", "WS": "061",
}
