import logging
import re
import requests
import pycountry
from datetime import datetime, date
from typing import List, Optional
from pydantic import BaseModel, Field, field_validator, HttpUrl

logger = logging.getLogger(__name__)


# --- Utility Functions ---


def map_country_code(name: str) -> str:
    """Map country names to ISO alpha-2 codes with overrides for edge cases."""
    overrides = {"Turkey": "TR", "Türkiye": "TR", "European Union": "EU"}
    try:
        return pycountry.countries.search_fuzzy(name)[0].alpha_2
    except Exception:
        code = overrides.get(name.strip())
        if code:
            return code
        country = pycountry.countries.get(name=name.strip())
        if country:
            return country.alpha_2
        logger.error("Invalid country name: %s", name)
        raise ValueError(f"Invalid country name: {name}")


policydoc_pattern = re.compile(
    r"""
    \.                          # a literal dot
    (?:document\.i|executive\.) # valid policy prefixes
    0*                          # optional leading zeroes
    (?P<id>\d+)                 # numeric ID
    """,
    re.VERBOSE,
)


# --- Data Models ---


class Company(BaseModel):
    company_id: int = Field(description="Unique identifier for the company")
    name: str = Field(description="Name of the company")
    operating_jurisdiction: str = Field(
        description="Jurisdiction where the company operates"
    )
    sector: str = Field(description="Sector in which the company operates")
    last_login: datetime = Field(description="Last login timestamp of the company")

    @field_validator("company_id", mode="before")
    def parse_int(cls, v):
        if not v:
            raise ValueError("company_id cannot be empty")
        return int(v)

    @field_validator("operating_jurisdiction", mode="before")
    def parse_country(cls, v):
        if not v:
            raise ValueError("Operating jurisdiction cannot be empty")
        return map_country_code(v)


class Policy(BaseModel):
    policy_id: int = Field(
        description="Extracted numeric ID from policy document reference"
    )
    name: str = Field(description="Policy document title")
    geography: str = Field(description="ISO country/region code")
    published_date: date = Field(description="Published date in dd/mm/YYYY format")
    updated_date: datetime = Field(description="Last updated timestamp")
    status: bool = Field(description="True if active, False if inactive")
    description: str = Field(description="Short summary of the policy")
    sectors: str = Field(description="Comma-separated sectors affected")
    topics: List[str] = Field(description="Policy-related topics")
    source_url: Optional[HttpUrl] = Field(description="Canonical URL to the policy")
    policy_type: Optional[str] = Field(description="Categorised policy type")

    @field_validator("policy_id", mode="before")
    def parse_policy_id(cls, v):
        if not v:
            raise ValueError("policy_id cannot be empty")
        match = policydoc_pattern.search(v)
        if not match:
            raise ValueError(f"Could not extract policy_id from: {v}")
        return int(match.group("id"))

    @field_validator("policy_type", mode="before")
    def detect_type(cls, v):
        if not v:
            raise ValueError("policy_type cannot be empty")
        if v.startswith("CCLW.document"):
            return "legislative"
        if v.startswith("CCLW.executive"):
            return "executive"
        raise ValueError(f"Unrecognized policy_type: {v}")

    @field_validator("status", mode="before")
    def parse_status(cls, v):
        mapping = {"active": True, "inactive": False}
        try:
            return mapping[v]
        except KeyError:
            raise ValueError(
                f"Invalid status value: {v}. Expected 'active' or 'inactive'."
            )

    @field_validator("geography", mode="before")
    def validate_geography(cls, v: str) -> str:
        return map_country_code(v)

    @field_validator("source_url", mode="before")
    def validate_url(cls, v):
        try:
            resp = requests.get(v, timeout=5, allow_redirects=True)
            if resp.status_code != 200:
                raise ValueError(f"URL returned status code: {resp.status_code}")
        except requests.RequestException:
            raise ValueError("Invalid or unreachable URL")
        return v
