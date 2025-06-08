import logging
from datetime import datetime
from pydantic.dataclasses import dataclass
from pydantic import field_validator
from utils.helpers import map_country_code

logger = logging.getLogger(__name__)

@dataclass
class Company:
    company_id: str
    name: str
    operating_jurisdiction: str
    sector: str
    last_login: datetime

    @field_validator("company_id", mode="before")
    def ensure_non_empty_id(cls, v: str) -> str:
        if not v or not v.strip():
            logger.warning("Empty company_id encountered")
            raise ValueError("company_id cannot be empty")
        return v.strip()

    @field_validator("last_login", mode="before")
    def parse_last_login(cls, v: str) -> datetime:
        # Expecting ISO format or timestamp
        try:
            return datetime.fromisoformat(v)
        except Exception:
            # fallback to common datetime parse
            from dateutil import parser
            return parser.parse(v)

    @field_validator("operating_jurisdiction", mode="before")
    def ensure_non_empty_jurisdiction(cls, v: str) -> str:
        if not v or not v.strip():
            logger.warning("Empty operating_jurisdiction encountered")
            raise ValueError("operating_jurisdiction cannot be empty")
        return map_country_code(v.strip())