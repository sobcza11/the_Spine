from __future__ import annotations

from typing import Any, Mapping

STATE_FIPS = {
    "01": "Alabama",
    "02": "Alaska",
    "04": "Arizona",
    "05": "Arkansas",
    "06": "California",
    "08": "Colorado",
    "09": "Connecticut",
    "10": "Delaware",
    "11": "District of Columbia",
    "12": "Florida",
    "13": "Georgia",
    "15": "Hawaii",
    "16": "Idaho",
    "17": "Illinois",
    "18": "Indiana",
    "19": "Iowa",
    "20": "Kansas",
    "21": "Kentucky",
    "22": "Louisiana",
    "23": "Maine",
    "24": "Maryland",
    "25": "Massachusetts",
    "26": "Michigan",
    "27": "Minnesota",
    "28": "Mississippi",
    "29": "Missouri",
    "30": "Montana",
    "31": "Nebraska",
    "32": "Nevada",
    "33": "New Hampshire",
    "34": "New Jersey",
    "35": "New Mexico",
    "36": "New York",
    "37": "North Carolina",
    "38": "North Dakota",
    "39": "Ohio",
    "40": "Oklahoma",
    "41": "Oregon",
    "42": "Pennsylvania",
    "44": "Rhode Island",
    "45": "South Carolina",
    "46": "South Dakota",
    "47": "Tennessee",
    "48": "Texas",
    "49": "Utah",
    "50": "Vermont",
    "51": "Virginia",
    "53": "Washington",
    "54": "West Virginia",
    "55": "Wisconsin",
    "56": "Wyoming",
}


def normalize_geography(row: Mapping[str, Any], *, provider: str, expected_level: str) -> dict[str, Any]:
    source_id = str(row.get("geography_code") or row.get("place_id") or row.get("state") or row.get("county") or row.get("state_code") or "")
    source_name = str(row.get("geography_name") or row.get("NAME") or row.get("place_name") or row.get("state_name") or "")
    if expected_level == "country":
        code = str(row.get("country") or row.get("geography_code") or row.get("place_id") or "US").upper()
        if code == "USA":
            code = "US"
        return _geo("country", f"country:{code}", "United States" if code == "US" else code, code, None, None, None, None, source_id or code, source_name or code)
    if expected_level == "state":
        state = str(row.get("state") or row.get("state_fips") or "").zfill(2)
        name = str(row.get("NAME") or row.get("state_name") or STATE_FIPS.get(state) or "").strip()
        if state and state != "00":
            return _geo("state", f"state:{state}", name or STATE_FIPS.get(state, state), "US", None, state, None, None, source_id or state, source_name or name)
    if expected_level == "county":
        state = str(row.get("state") or row.get("state_fips") or "")[:2].zfill(2)
        raw_county = str(row.get("county") or row.get("county_fips") or "")
        county = raw_county.zfill(3) if raw_county else ""
        if state and county:
            return _geo("county", f"county:{state}{county}", str(row.get("NAME") or ""), "US", None, state, f"{state}{county}", None, source_id or f"{state}{county}", source_name)
    if expected_level == "metro" and row.get("metro_code"):
        metro = str(row["metro_code"])
        return _geo("metro", f"metro:{metro}", source_name or metro, "US", None, None, None, metro, source_id or metro, source_name or metro)
    return _geo("unknown", f"unknown:{provider}", "Unknown", "US", None, None, None, None, source_id, source_name)


def _geo(level: str, geography_id: str, name: str, country: str, state_code: str | None, state_fips: str | None, county_fips: str | None, metro_code: str | None, source_id: str | None, source_name: str | None) -> dict[str, Any]:
    return {
        "geography_level": level,
        "geography_id": geography_id,
        "geography_name": name,
        "country_code": country,
        "state_code": state_code,
        "state_fips": state_fips,
        "county_fips": county_fips,
        "metro_code": metro_code,
        "source_geography_id": source_id,
        "source_geography_name": source_name,
    }
