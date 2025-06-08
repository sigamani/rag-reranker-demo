import logging
import json
import requests
import shortuuid

from datetime import date, datetime
from typing import List
from pydantic.dataclasses import dataclass
from pydantic import Field, field_validator, HttpUrl
from requests.exceptions import RequestException, SSLError
from utils.helpers import non_empty_str, map_country_code, strip_html_tags

logger = logging.getLogger(__name__)

@dataclass(config={"populate_by_name": True})
class Policy:
    name: str
    geography: str
    published_date: date
    updated_date: datetime
    description: str
    topics: List[str]
    source_url: HttpUrl

    active: bool = Field(alias="status")
    sector: str = Field(alias="sectors")
    policy_id: str = Field(alias="id", default_factory=shortuuid.uuid)

    @field_validator("active", mode="before")
    def parse_status(cls, v):
        text = v.strip().lower()
        if text == "active":   return True
        if text == "inactive": return False
        raise ValueError(f"Unknown status: {v!r}")
    @field_validator("name", mode="before")
    def _validate_name(cls, v: str) -> str:
        try:
            return non_empty_str(v, "name")
        except Exception as e:
            logger.warning("Policy.name validation failed: %s", e)
            raise

    @field_validator("geography", mode="before")
    def _validate_geography(cls, v: str) -> str:
        try:
            return map_country_code(v)
        except Exception as e:
            logger.warning("Policy.geography mapping failed: %s", e)
            raise

    @field_validator("sectors", mode="before")
    def _validate_sector(cls, v: str) -> str:
        try:
            return non_empty_str(v, "sectors")
        except Exception as e:
            logger.warning("Policy.sectors validation failed: %s", e)
            raise

    @field_validator("published_date", mode="before")
    def parse_pub_date(cls, v: str) -> date:
        try:
            return datetime.strptime(v, "%d/%m/%Y").date()
        except Exception as e:
            logger.warning("Policy.published_date parse failed: %s", e)
            raise ValueError(f"Invalid published_date format: {v}")

    @field_validator("active", mode="before")
    def parse_status(cls, v):
        try:
            text = v.strip().lower()
            if text == "active":
                return True
            if text == "inactive":
                return False
            raise ValueError(f"Unexpected status value: {v!r}")
        except AttributeError:
            # If v wasn’t a string, let Pydantic’s own bool coercion handle it
            return bool(v)

    @field_validator("description", mode="before")
    def _strip_description(cls, v: str) -> str:
        try:
            return strip_html_tags(v)
        except Exception as e:
            logger.warning("Policy.description strip failed: %s", e)
            raise

    @field_validator("topics", mode="before")
    def _parse_topics(cls, v) -> List[str]:
        try:
            items = json.loads(v) if isinstance(v, str) else v
            if isinstance(items, list):
                return [item.strip() for item in items]
            raise ValueError("topics must be a JSON list or list of strings")
        except Exception as e:
            logger.warning("Policy.topics parse failed: %s", e)
            raise

    @field_validator("source_url", mode="before")
    def _validate_url(cls, v: HttpUrl) -> HttpUrl:
        try:
            resp = requests.get(str(v), timeout=5, allow_redirects=True)
            if resp.status_code == 404:
                logger.error("Resource missing (404) at URL: %s", v)
                raise ValueError("URL returned 404 Not Found")
            if resp.status_code != 200:
                logger.warning(
                    "Non-200 status %d at URL: %s (continuing)",
                    resp.status_code, v
                )
        except SSLError as e:
            logger.warning("SSL handshake failed for URL %s: %s", v, e)
        except RequestException as e:
            logger.warning("Request error for URL %s: %s (continuing)", v, e)
        return v