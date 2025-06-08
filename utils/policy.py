import json
import logging
import pycountry

from datetime import date, datetime
from typing import List
from pydantic.dataclasses import dataclass
from pydantic import HttpUrl, field_validator
from utils.helpers import non_empty_str
logger = logging.getLogger(__name__)

@dataclass
class Policy:
    id: str
    name: str
    geography: str
    sectors: str
    published_date: date
    updated_date: datetime
    status: str
    description: str
    topics: List[str]
    source_url: HttpUrl

    # @field_validator("status", mode="before")
    # def parse_status(cls, v):
    #     if isinstance(v, str):
    #         text = v.strip().lower()
    #         if text == "active":   return 1
    #         if text == "inactive": return 0
    #         raise ValueError(f"Unknown status: {v!r}")
    #     return int(v)

    @field_validator("sectors", mode="before")
    def validate_sectors(cls, v: str) -> str:
        return non_empty_str(v, "sectors")

    @field_validator("published_date", mode="before")
    def parse_pub_date(cls, v):
        # assuming CSV dates are dd/mm/YYYY
        return datetime.strptime(v, "%d/%m/%Y").date()

    @field_validator("updated_date", mode="before")
    def parse_updated_date(cls, v):
        return datetime.fromisoformat(v)

    @field_validator("topics", mode="before")
    def parse_topics(cls, v):
        if isinstance(v, str):
            items = json.loads(v)
        else:
            items = v
        if not isinstance(items, list):
            raise ValueError("topics must be a JSON list")
        return [str(item).strip() for item in items]