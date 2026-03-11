"""Versioned public API routes (/v1)."""
import secrets
from typing import List

from fastapi import APIRouter, Depends, Header, HTTPException, Request, status

from api.schemas import (
    APIKeyPrincipal,
    CalculationProfileOut,
    CreditCalculationRequestCreate,
    CreditCalculationRequestOut,
    CreditResultOut,
    DashboardSeedOut,
    ErrorEnvelope,
    PartnerDashboardOut,
    SpeciesCatalogItem,
)
from api.security import require_api_key, require_scope
from api.service import CalculationService


router = APIRouter(prefix="/v1", tags=["Public API"])
service = CalculationService()


@router.get("/health")
def healthcheck() -> dict:
    return {"status": "ok"}


@router.get("/calculation_profiles", response_model=List[CalculationProfileOut])
def list_profiles(principal: APIKeyPrincipal = Depends(require_api_key)):
    require_scope(principal, "catalog:read")
    return service.list_profiles()


@router.get("/species_catalog", response_model=List[SpeciesCatalogItem])
def list_species(principal: APIKeyPrincipal = Depends(require_api_key)):
    require_scope(principal, "catalog:read")
    return service.list_species()


@router.post(
    "/credit_calculation_requests",
    response_model=CreditCalculationRequestOut,
    status_code=status.HTTP_202_ACCEPTED,
    responses={400: {"model": ErrorEnvelope}, 401: {"model": ErrorEnvelope}},
)
def create_credit_calculation_request(
    payload: CreditCalculationRequestCreate,
    request: Request,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    principal: APIKeyPrincipal = Depends(require_api_key),
):
    require_scope(principal, "calc:write")
    if principal.partner_id != payload.partner_id:
        raise HTTPException(status_code=403, detail="partner_id does not match API key principal")
    if not idempotency_key:
        raise HTTPException(status_code=400, detail="Idempotency-Key header is required")

    request_id = f"req_{secrets.token_urlsafe(8)}"
    _ = request_id  # Reserved for future structured error payload usage.
    return service.create_request(payload=payload, idempotency_key=idempotency_key)


@router.get("/credit_calculation_requests/{request_id}", response_model=CreditCalculationRequestOut)
def get_credit_calculation_request(
    request_id: str,
    principal: APIKeyPrincipal = Depends(require_api_key),
):
    require_scope(principal, "calc:read")
    calc_request = service.get_request(request_id)
    if calc_request is None:
        raise HTTPException(status_code=404, detail="credit_calculation_request not found")
    return calc_request


@router.get("/credit_results/{result_id}", response_model=CreditResultOut)
def get_credit_result(
    result_id: str,
    principal: APIKeyPrincipal = Depends(require_api_key),
):
    require_scope(principal, "calc:read")
    result = service.get_result(result_id)
    if result is None:
        raise HTTPException(status_code=404, detail="credit_result not found")
    return result


@router.get("/dashboard/summary", response_model=PartnerDashboardOut)
def get_dashboard_summary(
    principal: APIKeyPrincipal = Depends(require_api_key),
):
    require_scope(principal, "calc:read")
    return service.get_partner_dashboard_summary(partner_id=principal.partner_id)


@router.post("/dashboard/seed_demo", response_model=DashboardSeedOut)
def seed_dashboard_demo_data(
    principal: APIKeyPrincipal = Depends(require_api_key),
):
    require_scope(principal, "calc:write")
    return service.seed_partner_demo_data(partner_id=principal.partner_id, months=6)

