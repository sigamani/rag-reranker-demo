import logging
import json

from datetime import date, datetime
from typing import List
from pydantic.dataclasses import dataclass
from pydantic import field_validator, HttpUrl
from utils.helpers import map_country_code

logger = logging.getLogger(__name__)

@dataclass
class Policy:
    policy_id: str
    name: str
    geography: str
    sector: str
    published_date: date
    updated_date: datetime
    active: bool
    description: str
    topics: List[str]
    source_url: HttpUrl

    @field_validator("name", "sector", "description", mode="before")
    def _non_empty_str(cls, v, info):
        v2 = v.strip()
        if not v2:
            raise ValueError(f"{info.field_name!r} may not be blank")
        return v2

    @field_validator("geography", mode="before")
    def _validate_geography(cls, v: str) -> str:
        return map_country_code(v)

    @field_validator("published_date", mode="before")
    def _parse_pub_date(cls, v: str) -> date:
        return datetime.strptime(v, "%d/%m/%Y").date()

    @field_validator("active", mode="before")
    def _parse_status(cls, v):
        try:
            t = v.strip().lower()
            if t == "active":   return True
            if t == "inactive": return False
            raise ValueError()
        except AttributeError:
            return bool(v)

    @field_validator("topics", mode="before")
    def _parse_topics(cls, v) -> List[str]:
        lst = json.loads(v) if isinstance(v, str) else v
        if not isinstance(lst, list):
            raise ValueError("topics must be a list")
        return [s.strip() for s in lst]