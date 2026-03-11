"""Service layer for credit calculation request orchestration."""
import random
import secrets
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from threading import Lock
from typing import Dict, List, Optional, Tuple

from api.schemas import (
    AnimalObservation,
    CalculationProfileOut,
    CreditBreakdownItem,
    CreditCalculationRequestCreate,
    CreditCalculationRequestOut,
    CreditResultOut,
    CreditSummary,
    DashboardSeedOut,
    DashboardCreditTypePoint,
    DashboardRecentResult,
    DashboardSeriesPoint,
    Location,
    PartnerDashboardOut,
    SpeciesCatalogItem,
)


def _make_id(prefix: str) -> str:
    return f"{prefix}_{secrets.token_urlsafe(10).replace('-', '').replace('_', '')}"


@dataclass
class StoredRequest:
    id: str
    partner_id: str
    status: str
    created: datetime
    result_id: Optional[str]
    payload: CreditCalculationRequestCreate
    idempotency_key: str


class CalculationService:
    """
    In-memory service implementation.
    Replace this class internals with PostgreSQL + worker-backed processing for production.
    """

    def __init__(self):
        self._lock = Lock()
        self._requests: Dict[str, StoredRequest] = {}
        self._results: Dict[str, CreditResultOut] = {}
        self._idem_index: Dict[Tuple[str, str], str] = {}
        self._profiles = [
            CalculationProfileOut(
                id="cp_standard_v1",
                name="Standard Biodiversity Profile",
                version="v1",
                accepted_fields=[
                    "animal_id",
                    "species_code",
                    "observed_at",
                    "location.lat",
                    "location.lon",
                    "confidence_score",
                ],
                constraints={
                    "max_observations": "5000 per request",
                    "max_period_days": "365",
                    "allowed_confidence_range": "0.0-1.0",
                },
            )
        ]
        self._species = [
            SpeciesCatalogItem(code="panthera_onca", common_name="Jaguar", latin_name="Panthera onca"),
            SpeciesCatalogItem(code="tapirus_terrestris", common_name="Tapir", latin_name="Tapirus terrestris"),
            SpeciesCatalogItem(code="ateles_belzebuth", common_name="White-bellied spider monkey", latin_name="Ateles belzebuth"),
        ]

    def list_profiles(self) -> List[CalculationProfileOut]:
        return self._profiles

    def list_species(self) -> List[SpeciesCatalogItem]:
        return self._species

    def create_request(
        self,
        payload: CreditCalculationRequestCreate,
        idempotency_key: str,
    ) -> CreditCalculationRequestOut:
        with self._lock:
            idem_tuple = (payload.partner_id, idempotency_key)
            existing_id = self._idem_index.get(idem_tuple)
            if existing_id:
                existing = self._requests[existing_id]
                return self._to_request_out(existing)

            request_id = _make_id("ccr")
            result_id = _make_id("cr")
            created_at = datetime.now(timezone.utc)

            result = self._calculate_result(result_id=result_id, request_id=request_id, payload=payload)
            self._results[result_id] = result

            stored = StoredRequest(
                id=request_id,
                partner_id=payload.partner_id,
                status="succeeded",
                created=created_at,
                result_id=result_id,
                payload=payload,
                idempotency_key=idempotency_key,
            )
            self._requests[request_id] = stored
            self._idem_index[idem_tuple] = request_id
            return self._to_request_out(stored)

    def get_request(self, request_id: str) -> Optional[CreditCalculationRequestOut]:
        req = self._requests.get(request_id)
        if not req:
            return None
        return self._to_request_out(req)

    def get_result(self, result_id: str) -> Optional[CreditResultOut]:
        return self._results.get(result_id)

    def seed_partner_demo_data(self, partner_id: str, months: int = 6) -> DashboardSeedOut:
        """
        Create deterministic demo data so partner dashboards are immediately useful in demos.
        """
        rng = random.Random(partner_id)
        now = datetime.now(timezone.utc)
        created_requests = 0

        with self._lock:
            for month_index in range(months):
                month_date = now - timedelta(days=(months - month_index) * 30)
                for req_index in range(2):
                    obs_count = 2 + (req_index % 2)
                    observations: List[AnimalObservation] = []
                    for obs_idx in range(obs_count):
                        species = self._species[(month_index + obs_idx) % len(self._species)]
                        confidence = round(0.55 + rng.random() * 0.4, 2)
                        observations.append(
                            AnimalObservation(
                                animal_id=f"an_demo_{month_index}_{req_index}_{obs_idx}",
                                species_code=species.code,
                                observed_at=month_date - timedelta(days=obs_idx),
                                location=Location(
                                    lat=4.5 + rng.random() * 1.0,
                                    lon=-74.7 + rng.random() * 1.2,
                                ),
                                confidence_score=confidence,
                            )
                        )

                    period_end = month_date.date()
                    period_start = (month_date - timedelta(days=30)).date()
                    payload = CreditCalculationRequestCreate(
                        partner_id=partner_id,
                        calculation_profile_id="cp_standard_v1",
                        animal_observations=observations,
                        period={"start_date": period_start, "end_date": period_end},
                        metadata={"source": "demo_seed"},
                    )

                    request_id = _make_id("ccr")
                    result_id = _make_id("cr")
                    created_at = month_date + timedelta(days=req_index)
                    result = self._calculate_result(result_id=result_id, request_id=request_id, payload=payload)

                    self._results[result_id] = result
                    self._requests[request_id] = StoredRequest(
                        id=request_id,
                        partner_id=partner_id,
                        status="succeeded",
                        created=created_at,
                        result_id=result_id,
                        payload=payload,
                        idempotency_key=f"seed_{secrets.token_hex(6)}",
                    )
                    created_requests += 1

        return DashboardSeedOut(
            partner_id=partner_id,
            created_requests=created_requests,
            created_results=created_requests,
            months_seeded=months,
        )

    def get_partner_dashboard_summary(self, partner_id: str) -> PartnerDashboardOut:
        partner_requests = [r for r in self._requests.values() if r.partner_id == partner_id]
        partner_requests.sort(key=lambda r: r.created)

        total_requests = len(partner_requests)
        succeeded = len([r for r in partner_requests if r.status == "succeeded"])
        failed = len([r for r in partner_requests if r.status == "failed"])

        total_credits = 0.0
        by_month: Dict[str, float] = {}
        by_credit_type: Dict[str, float] = {}
        recent_rows: List[DashboardRecentResult] = []

        for req in partner_requests:
            if not req.result_id:
                continue

            result = self._results.get(req.result_id)
            if not result:
                continue

            total_credits += result.summary.total_credits

            month_key = req.created.strftime("%Y-%m")
            by_month[month_key] = by_month.get(month_key, 0.0) + result.summary.total_credits

            for item in result.breakdown:
                by_credit_type[item.credit_type] = by_credit_type.get(item.credit_type, 0.0) + item.credits

            recent_rows.append(
                DashboardRecentResult(
                    request_id=req.id,
                    result_id=result.id,
                    total_credits=result.summary.total_credits,
                    created=req.created,
                )
            )

        avg_credits = round(total_credits / succeeded, 2) if succeeded > 0 else 0.0
        success_rate = round((succeeded / total_requests) * 100, 2) if total_requests > 0 else 0.0

        monthly_credits = [
            DashboardSeriesPoint(label=month, value=round(value, 2))
            for month, value in sorted(by_month.items())
        ]
        credit_type_breakdown = [
            DashboardCreditTypePoint(credit_type=credit_type, credits=round(credits, 2))
            for credit_type, credits in sorted(by_credit_type.items())
        ]

        recent_results = sorted(recent_rows, key=lambda row: row.created, reverse=True)[:8]

        return PartnerDashboardOut(
            partner_id=partner_id,
            totals={
                "requests_total": float(total_requests),
                "requests_succeeded": float(succeeded),
                "requests_failed": float(failed),
                "success_rate_pct": success_rate,
                "credits_total": round(total_credits, 2),
                "credits_average": avg_credits,
            },
            monthly_credits=monthly_credits,
            credit_type_breakdown=credit_type_breakdown,
            recent_results=recent_results,
        )

    def _to_request_out(self, req: StoredRequest) -> CreditCalculationRequestOut:
        return CreditCalculationRequestOut(
            id=req.id,
            status=req.status,
            created=req.created,
            result_id=req.result_id,
            links={"self": f"/v1/credit_calculation_requests/{req.id}"},
        )

    def _calculate_result(
        self,
        result_id: str,
        request_id: str,
        payload: CreditCalculationRequestCreate,
    ) -> CreditResultOut:
        """
        Derived-only output: no raw ecological records are surfaced.
        """
        if not payload.animal_observations:
            total = 0.0
        else:
            weighted = sum(obs.confidence_score for obs in payload.animal_observations)
            total = round(weighted * 10.0, 2)

        biodiversity_integrity = round(total * 0.65, 2)
        habitat_stability = round(total - biodiversity_integrity, 2)

        return CreditResultOut(
            id=result_id,
            request_id=request_id,
            status="succeeded",
            summary=CreditSummary(
                total_credits=total,
                period_start=payload.period.start_date,
                period_end=payload.period.end_date,
            ),
            breakdown=[
                CreditBreakdownItem(credit_type="biodiversity_integrity", credits=biodiversity_integrity),
                CreditBreakdownItem(credit_type="habitat_stability", credits=habitat_stability),
            ],
            metadata={
                "model_version": "2026.02.1",
                "calculation_profile_id": payload.calculation_profile_id,
            },
        )


