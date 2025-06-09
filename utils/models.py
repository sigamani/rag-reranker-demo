from datetime import datetime, date
from typing import List, Optional
from pydantic import BaseModel, Field, field_validator, HttpUrl
from utils.helpers import (
    validate_non_empty_int,
    validate_non_empty_str,
    map_country_code,
    extract_policy_id,
    validate_policy_type,
    validate_url_reachable,
    parse_status_value
)

class Company(BaseModel):
    company_id: int = Field(description="Unique identifier for the company")
    name: str = Field(description="Name of the company")
    operating_jurisdiction: str = Field(description="Jurisdiction where the company operates")
    sector: str = Field(description="Sector in which the company operates")
    last_login: datetime = Field(description="Last login timestamp of the company")

    @field_validator("company_id", mode="before")
    def parse_int(cls, v):
        return validate_non_empty_int(v, "company_id")

    @field_validator("operating_jurisdiction", mode="before")
    def parse_country(cls, v):
        validated = validate_non_empty_str(v, "operating_jurisdiction")
        return map_country_code(validated)

class Policy(BaseModel):
    policy_id: int = Field(description="Extracted numeric ID from policy document reference")
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
        validated = validate_non_empty_str(v, "policy_id")
        return extract_policy_id(validated)

    @field_validator("policy_type", mode="before")
    def detect_type(cls, v):
        validated = validate_non_empty_str(v, "policy_type")
        return validate_policy_type(validated)

    @field_validator("status", mode="before")
    def parse_status(cls, v):
        return parse_status_value(v)

    @field_validator("geography", mode="before")
    def validate_geography(cls, v):
        return map_country_code(v)

    @field_validator("source_url", mode="before")
    def validate_url(cls, v):
        return validate_url_reachable(v)