import logging
from datetime import datetime

from pydantic.dataclasses import dataclass
from pydantic import  field_validator
from utils.helpers import non_empty_str, map_country_code

logger = logging.getLogger(__name__)

@dataclass
class Company:
    company_id: int
    name: str
    operating_jurisdiction: str
    sector: str
    last_login: datetime  # ISO-8601 parsed natively by Pydantic

    @field_validator("name", mode="before")
    def _validate_name(cls, v: str) -> str:
        try:
            return non_empty_str(v, "name")
        except Exception as e:
            logger.warning("Company.name validation failed: %s", e)
            raise

    @field_validator("operating_jurisdiction", mode="before")
    def _validate_jurisdiction(cls, v: str) -> str:
        try:
            return map_country_code(v)
        except Exception as e:
            logger.warning("Company.operating_jurisdiction mapping failed: %s", e)
            raise

    @field_validator("sector", mode="before")
    def _validate_sector(cls, v: str) -> str:
        try:
            return non_empty_str(v, "sector")
        except Exception as e:
            logger.warning("Company.sector validation failed: %s", e)
            raise