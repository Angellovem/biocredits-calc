"""Pydantic schemas for the public BioCredits API."""
from datetime import date, datetime
from typing import Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


RequestStatus = Literal["processing", "succeeded", "failed", "cancelled"]


class Location(BaseModel):
    lat: float = Field(ge=-90, le=90)
    lon: float = Field(ge=-180, le=180)


class AnimalObservation(BaseModel):
    animal_id: str = Field(min_length=1, max_length=128)
    species_code: str = Field(min_length=1, max_length=128)
    observed_at: datetime
    location: Location
    confidence_score: float = Field(ge=0, le=1)


class CalculationPeriod(BaseModel):
    start_date: date
    end_date: date

    @field_validator("end_date")
    @classmethod
    def validate_dates(cls, end_date: date, info):
        start_date = info.data.get("start_date")
        if start_date and end_date < start_date:
            raise ValueError("end_date must be greater than or equal to start_date")
        return end_date


class CreditCalculationRequestCreate(BaseModel):
    partner_id: str = Field(min_length=1, max_length=128)
    calculation_profile_id: str = Field(min_length=1, max_length=128)
    animal_observations: List[AnimalObservation] = Field(min_length=1, max_length=5000)
    period: CalculationPeriod
    metadata: Optional[Dict[str, str]] = None


class Links(BaseModel):
    self: str


class CreditCalculationRequestOut(BaseModel):
    id: str
    object: Literal["credit_calculation_request"] = "credit_calculation_request"
    status: RequestStatus
    created: datetime
    result_id: Optional[str] = None
    links: Links


class CreditBreakdownItem(BaseModel):
    credit_type: str
    credits: float


class CreditSummary(BaseModel):
    total_credits: float
    currency_unit: str = "bio_credit"
    period_start: date
    period_end: date


class CreditResultOut(BaseModel):
    id: str
    object: Literal["credit_result"] = "credit_result"
    request_id: str
    status: Literal["succeeded", "failed"]
    summary: CreditSummary
    breakdown: List[CreditBreakdownItem]
    metadata: Dict[str, str]


class CalculationProfileOut(BaseModel):
    id: str
    object: Literal["calculation_profile"] = "calculation_profile"
    name: str
    version: str
    accepted_fields: List[str]
    constraints: Dict[str, str]


class SpeciesCatalogItem(BaseModel):
    code: str
    common_name: str
    latin_name: str
    enabled: bool = True


class DashboardSeriesPoint(BaseModel):
    label: str
    value: float


class DashboardCreditTypePoint(BaseModel):
    credit_type: str
    credits: float


class DashboardRecentResult(BaseModel):
    request_id: str
    result_id: str
    total_credits: float
    created: datetime


class PartnerDashboardOut(BaseModel):
    partner_id: str
    totals: Dict[str, float]
    monthly_credits: List[DashboardSeriesPoint]
    credit_type_breakdown: List[DashboardCreditTypePoint]
    recent_results: List[DashboardRecentResult]


class DashboardSeedOut(BaseModel):
    partner_id: str
    created_requests: int
    created_results: int
    months_seeded: int


class ErrorBody(BaseModel):
    type: str
    code: str
    message: str
    param: Optional[str] = None
    request_id: str


class ErrorEnvelope(BaseModel):
    error: ErrorBody


class APIKeyPrincipal(BaseModel):
    model_config = ConfigDict(frozen=True)
    partner_id: str
    scopes: List[str]


