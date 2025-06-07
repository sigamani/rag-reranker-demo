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
  • source_url: must return HTTP 200

Preprocessing steps ensure data integrity before insertion.
Requires `pycountry`, `pydantic`, `shortuuid`, and `requests`.
"""

import logging
import json
import re
from datetime import date, datetime

import pycountry
import requests
import shortuuid
from pydantic import BaseModel, Field, validator, HttpUrl

logger = logging.getLogger(__name__)


def map_country(name: str) -> str:
    """Turkey has changed its name in 2022
    which is not present in the fuzzy search library"""

    # TODO: Potentially use LLM to do mapping.
    _manual = {"Turkey": "TR", "Türkiye": "TR"}
    try:
        return pycountry.countries.search_fuzzy(name)[0].alpha_2
    except Exception:
        code = _manual.get(name.strip())
        if code:
            return code
        raise ValueError(f"Invalid country name: {name}")


class Company(BaseModel):
    company_id: int
    name: str
    operating_jurisdiction: str
    sector: str
    last_login: datetime

    @validator("name", pre=True)
    def non_empty_name(cls, v: str) -> str:
        v_str = v.strip()
        if not v_str:
            raise ValueError("must be a non-empty string")
        return v_str

    @validator("operating_jurisdiction", pre=True)
    def map_operating_jurisdiction(cls, v: str) -> str:
        return map_country(v)

    @validator("sector", pre=True)
    def non_empty_sector(cls, v: str) -> str:
        v_str = v.strip()
        if not v_str:
            raise ValueError("sector must be a non-empty string")
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

    @validator("name", pre=True)
    def non_empty_policy_name(cls, v: str) -> str:
        v_str = v.strip()
        if not v_str:
            raise ValueError("must be a non-empty string")
        return v_str

    @validator("geography", pre=True)
    def map_geography(cls, v: str) -> str:
        return map_country(v)

    @validator("sector", pre=True)
    def non_empty_sector_policy(cls, v: str) -> str:
        v_str = v.strip()
        if not v_str:
            raise ValueError("sector must be a non-empty string")
        return v_str

    @validator("published_date", pre=True)
    def parse_published_date(cls, v: str) -> date:
        try:
            return datetime.strptime(v, "%d/%m/%Y").date()
        except Exception:
            raise ValueError(f"Invalid published_date format: {v}")

    @validator("updated_date", pre=True)
    def parse_updated_date(cls, v: str) -> datetime:
        try:
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
        except Exception:
            raise ValueError(f"Invalid updated_date format: {v}")

    @validator("active", pre=True)
    def parse_active(cls, v) -> bool:
        if isinstance(v, str):
            return v.strip().lower() == "active"
        return bool(v)

    @validator("description", pre=True)
    def strip_html(cls, v: str) -> str:
        text = re.sub(r"<[^>]+>", "", v)
        return " ".join(text.split())

    @validator("topics", pre=True)
    def parse_topics(cls, v) -> list[str]:
        try:
            items = json.loads(v) if isinstance(v, str) else v
            if isinstance(items, list):
                return [s.strip() for s in items]
        except Exception:
            pass
        raise ValueError("topics must be JSON list string or Python list")

    def topics_json(self) -> str:
        return json.dumps(self.topics)

    @validator("source_url")
    def check_url(cls, v: HttpUrl) -> HttpUrl:
        try:
            resp = requests.get(str(v), timeout=5, allow_redirects=True)
            if resp.status_code != 200:
                logger.warning(
                    "Skipping URL %s: returned status %d", v, resp.status_code
                )
                raise ValueError(f"URL returned status {resp.status_code}")
        except requests.RequestException as e:
            logger.warning("Skipping URL %s due to request error: %s", v, e)
            raise ValueError(f"URL request failed: {e}")
        return v

    class Config:
        validate_by_name = True
