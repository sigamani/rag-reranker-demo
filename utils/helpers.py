import re
import pycountry
from typing import Optional
import requests

# Constants
COUNTRY_OVERRIDES = {"Turkey": "TR", "Türkiye": "TR", "European Union": "EU"}
STATUS_MAPPING = {"active": True, "inactive": False}
POLICY_PATTERN = re.compile(r"\.(?:document\.i|executive\.)0*(?P<id>\d+)", re.VERBOSE)

# Country mapping functions
def get_country_override(name: str) -> Optional[str]:
    return COUNTRY_OVERRIDES.get(name.strip())

def search_country_fuzzy(name: str) -> Optional[str]:
    try:
        return pycountry.countries.search_fuzzy(name)[0].alpha_2
    except (LookupError, AttributeError):
        return None

def get_country_exact(name: str) -> Optional[str]:
    country = pycountry.countries.get(name=name.strip())
    return country.alpha_2 if country else None

def map_country_code(name: str) -> str:
    return (
        get_country_override(name) or
        search_country_fuzzy(name) or
        get_country_exact(name) or
        (_ for _ in ()).throw(ValueError(f"Invalid country name: {name}"))
    )

# Policy ID extraction
def extract_policy_id(value: str) -> int:
    match = POLICY_PATTERN.search(value)
    if not match:
        raise ValueError(f"Could not extract policy_id from: {value}")
    return int(match.group("id"))

# Policy type validation
def validate_policy_type(value: str) -> str:
    if value.startswith("CCLW.document"):
        return "legislative"
    if value.startswith("CCLW.executive"):
        return "executive"
    raise ValueError(f"Unrecognized policy_type: {value}")

# Status parsing
def parse_status_value(value: str) -> bool:
    if value not in STATUS_MAPPING:
        raise ValueError(f"Invalid status value: {value}. Expected 'active' or 'inactive'.")
    return STATUS_MAPPING[value]

# URL validation
def check_url_response(url: str) -> str:
    resp = requests.get(url, timeout=5, allow_redirects=True)
    if resp.status_code != 200:
        raise ValueError(f"URL returned status code: {resp.status_code}")
    return url

def validate_url_reachable(url: str) -> str:
    try:
        return check_url_response(url)
    except requests.RequestException:
        raise ValueError("Invalid or unreachable URL")

# Generic validation helpers
def validate_non_empty_int(value, field_name: str) -> int:
    if not value:
        raise ValueError(f"{field_name} cannot be empty")
    return int(value)

def validate_non_empty_str(value, field_name: str) -> str:
    if not value:
        raise ValueError(f"{field_name} cannot be empty")
    return str(value)