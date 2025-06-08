import re
import json
import logging
import requests
from datetime import datetime, date
from typing import List
from pydantic.dataclasses import dataclass
from pydantic import Field, field_validator, HttpUrl
from requests.exceptions import RequestException, SSLError
from utils.helpers import non_empty_str, strip_html_tags, map_country_code

from datetime import datetime
from pydantic import field_validator
from pydantic import field_validator, parse_obj_as
logger = logging.getLogger(__name__)

from pydantic import field_validator, TypeAdapter
_date_adapter = TypeAdapter(date)
_datetime_adapter = TypeAdapter(datetime)

@dataclass(config={"populate_by_name": True})
class Policy:
    policy_id: int
    name: str
    geography: str                   
    published_date: date
    updated_date: datetime
    status: int
    description: str
    sectors: str
    topics: List[str]
    source_url: HttpUrl

    """
    policy_type distinguishes between legislative 
    and executive documents:
    1) “legislative” covers bills/acts that carry 
    legal penalties for non-compliance
    2) “executive” covers orders or directives, 
    which can be issued and implemented more quickly
    """
    policy_type: str = Field(default=None)

    @field_validator("policy_id", mode="before")
    def parse_policy_id(cls, v):
        if isinstance(v, str):
            m = re.search(r"\.(?:document\.i|executive\.)0*?(\d+)", v)
            if m:
                return int(m.group(1))
            raise ValueError(f"Cannot parse policy_id from {v!r}")
        return v

    @field_validator("policy_type", mode="before")
    def detect_type(cls, v, info):
        # look at the same raw_id we parsed above:
        raw = info.data.get("raw_id")
        if raw.startswith("CCLW.document"):
            return "legislative"
        if raw.startswith("CCLW.executive"):
            return "executive"
        # as a catch-all you could also search for the substring:
        if "document" in raw:
            return "legislative"
        return "executive"

    @field_validator("status", mode="before")
    def parse_status(cls, v):
        if isinstance(v, str):
            text = v.strip().lower()
            if text == "active":   return 1
            if text == "inactive": return 0
            raise ValueError(f"Unknown status: {v!r}")
        return int(v)

    @field_validator("geography", mode="before")
    def _validate_geography(cls, v: str) -> str:
        try:
            return map_country_code(v)
        except Exception as e:
            logger.warning("Policy.geography mapping failed: %s", e)
            raise

    @field_validator("published_date", mode="before")
    def parse_pub_date(cls, v):
        # first your custom dd/mm/YYYY, then fallback to Pydantic
        if isinstance(v, str):
            try:
                return datetime.strptime(v, "%d/%m/%Y").date()
            except ValueError:
                return _date_adapter.validate_python(v)
        return v

    @field_validator("updated_date", mode="before")
    def parse_updated_date(cls, v):
        # let Pydantic handle any ISO-3339, “Z”-suffix, offsets, etc.
        if isinstance(v, str):
            return _datetime_adapter.validate_python(v)
        return v

    @field_validator("description", mode="before")
    def _strip_description(cls, v: str) -> str:
        return strip_html_tags(v)

    @field_validator("topics", mode="before")
    def parse_topics(cls, v) -> List[str]:
        items = json.loads(v) if isinstance(v, str) else v
        if not isinstance(items, list):
            raise ValueError("topics must be a JSON list")
        return [str(item).strip() for item in items]

    @field_validator("source_url", mode="before")
    def _validate_url(cls, v):
        """
        Dealing with edge cases in URLs (not seen in the data right now)
        """
        if not v:
            return None
        try:
            resp = requests.get(v, timeout=5, allow_redirects=True)
        except RequestException as e:
            logger.warning("URL request error (continuing): %s", e)
            return v

        if resp.status_code == 404:
            raise ValueError("URL returned 404 Not Found")
        return v
