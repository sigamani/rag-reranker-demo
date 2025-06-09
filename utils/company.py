import logging
from datetime import datetime
from utils.helpers import map_country_code
from pydantic import BaseModel, Field, field_validator

logger = logging.getLogger(__name__)

class Company(BaseModel):
    company_id: int = Field(description="Unique identifier for the company")
    name: str = Field(description="Name of the company")
    operating_jurisdiction: str = Field(description="Jurisdiction where the company operates")
    sector: str = Field(description="Sector in which the company operates")
    last_login: datetime = Field(description="Last login timestamp of the company")

    @field_validator("company_id", mode="before")
    def parse_int(cls, v):
        logger.debug("Parsing company_id: %s", v)
        return int(v)

    @field_validator("operating_jurisdiction", mode="before")
    def parse_country(cls, v):
        if not v:
            err = "Operating jurisdiction cannot be empty"
            logger.error(err)
            raise ValueError(err)
        logger.info("Mapping operating jurisdiction: %s", v)
        return map_country_code(v)