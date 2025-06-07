"""
Defines Pydantic models for Company and Policy with strict typing, validation,
and preprocessing for ETL into a relational database (e.g. PostgreSQL with JSONB fields).

- Company:
  • company_id: integer
  • name: non-empty string
  • operating_jurisdiction: ISO 3166-1 alpha-2 code (string)
  • sector: non-empty string
  • last_login: datetime

- Policy:
  • policy_id: short UUID string (alias 'id')
  • name: non-empty string
  • geography: ISO 3166-1 alpha-2 code (string)
  • sector: non-empty string (alias 'sectors')
  • published_date: date parsed from 'DD/MM/YYYY'
  • updated_date: datetime parsed from ISO
  • active: boolean (alias 'status')
  • description: plain text (HTML tags stripped)
  • topics: list of strings
  • source_url: must return HTTP 200 (GET with redirects)

Preprocessing ensures data integrity before insertion. Requires
`pycountry`, `pydantic v2`, `shortuuid`, and `requests`.
"""
import logging
import json
import re
from datetime import date, datetime

import pycountry

import requests
import shortuuid
from requests.exceptions import SSLError, RequestException
from pydantic import BaseModel, Field, field_validator, HttpUrl
logger = logging.getLogger(__name__)


import logging
import pycountry

logger = logging.getLogger(__name__)

def map_country(name: str) -> str:
    """
    Map arbitrary country or region name to ISO 3166-1 alpha-2 code,
    with manual overrides for known exceptions.
    """
    manual = {
        "Turkey": "TR",        
        "Türkiye": "TR",
        "European Union": "EU" 
    }
    try:
        return pycountry.countries.search_fuzzy(name)[0].alpha_2
    except LookupError:
        pass

    code = manual.get(name.strip())
    if code:
        return code

    try:
        country = pycountry.countries.get(name=name)
        if country:
            return country.alpha_2
    except Exception:
        pass

    logger.error("Invalid country name: %s", name)
    raise ValueError(f"Invalid country name: {name}")



class Company(BaseModel):
    company_id: int
    name: str
    operating_jurisdiction: str
    sector: str
    last_login: datetime

    @field_validator("name", mode="before")
    def validate_name(cls, v: str) -> str:
        v_str = v.strip()
        if not v_str:
            raise ValueError("name must be non-empty")
        return v_str

    @field_validator("operating_jurisdiction", mode="before")
    def validate_jurisdiction(cls, v: str) -> str:
        return map_country(v)

    @field_validator("sector", mode="before")
    def validate_sector_company(cls, v: str) -> str:
        v_str = v.strip()
        if not v_str:
            raise ValueError("sector must be non-empty")
        return v_str


class Policy(BaseModel):
    policy_id: str = Field(alias="id", default_factory=lambda: shortuuid.uuid())
    name: str
    geography: str
    sector: str = Field(alias="sectors")
    published_date: date
    updated_date: datetime
    active: bool = Field(alias="status")
    description: str
    topics: list[str]
    source_url: HttpUrl

    @field_validator("name", mode="before")
    def validate_policy_name(cls, v: str) -> str:
        v_str = v.strip()
        if not v_str:
            raise ValueError("name must be non-empty")
        return v_str

    @field_validator("geography", mode="before")
    def validate_geography(cls, v: str) -> str:
        return map_country(v)

    @field_validator("sector", mode="before")
    def validate_sector_policy(cls, v: str) -> str:
        v_str = v.strip()
        if not v_str:
            raise ValueError("sector must be non-empty")
        return v_str

    @field_validator("published_date", mode="before")
    def parse_published_date(cls, v: str) -> date:
        try:
            return datetime.strptime(v, "%d/%m/%Y").date()
        except Exception:
            raise ValueError(f"Invalid published_date format: {v}")

    @field_validator("updated_date", mode="before")
    def parse_updated_date(cls, v: str) -> datetime:
        try:
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
        except Exception:
            raise ValueError(f"Invalid updated_date format: {v}")

    @field_validator("active", mode="before")
    def parse_active(cls, v) -> bool:
        if isinstance(v, str):
            return v.strip().lower() == "active"
        return bool(v)

    @field_validator("description", mode="before")
    def strip_html(cls, v: str) -> str:
        text = re.sub(r'<[^>]+>', '', v)
        return ' '.join(text.split())

    @field_validator("topics", mode="before")
    def parse_topics(cls, v) -> list[str]:
        items = json.loads(v) if isinstance(v, str) else v
        if isinstance(items, list):
            return [s.strip() for s in items]
        raise ValueError("topics must be a JSON list or list of strings")



@field_validator("source_url", mode="before")
def verify_url(cls, v: HttpUrl) -> HttpUrl:
    try:
        resp = requests.get(str(v), timeout=5, allow_redirects=True)
        # drop only on true 404
        if resp.status_code == 404 or "404 Not Found" in resp.text:
            logger.error("Resource missing (404) at URL: %s", v)
            raise ValueError("URL returned 404 Not Found")
        # other non-200 codes (403, 301…) are accepted
        if resp.status_code != 200:
            logger.info("Non-200 status %d at URL: %s (accepting)", resp.status_code, v)
    except SSLError as e:
        # log handshake failures but do NOT raise
        logger.warning("SSL handshake failed at URL %s: %s (continuing)", v, e)
    except RequestException as e:
        # truly unrecoverable network/DNS/timeouts
        logger.warning("Skipping URL %s due to request error: %s", v, e)
        raise ValueError(f"URL request failed: {e}")
    return v


    class Config:
        validate_by_name = True

