"""Pydantic schemas for API requests and responses."""
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field

from src.constants import AnomalyType, FixAction, Severity


class AnomalyDetail(BaseModel):
    """Details of a detected anomaly."""
    type: AnomalyType
    name: str
    depth: int
    entity_id: str | None = None
    entity_type: str | None = None  # Added to fix governance tagging bug
    description: str | None = None


class ImpactedAssets(BaseModel):
    """Assets impacted by an incident."""
    tables: list[dict[str, Any]] = Field(default_factory=list)
    dashboards: list[dict[str, Any]] = Field(default_factory=list)
    ml_models: list[dict[str, Any]] = Field(default_factory=list)
    total_impact_count: int = 0


class SuggestedFix(BaseModel):
    """A suggested fix for an incident."""
    action: FixAction
    target: str
    description: str
    sql_script: str | None = None
    markdown_details: str | None = None


class DiagnosisRequest(BaseModel):
    """Request to diagnose an asset."""
    target_fqn: str = Field(
        ...,
        description="Fully Qualified Name of the asset to diagnose",
        examples=["snowflake.analytics.dim_customer"]
    )
    upstream_depth: int = Field(
        default=5,
        ge=1,
        le=10,
        description="Depth for upstream lineage traversal"
    )
    downstream_depth: int = Field(
        default=5,
        ge=1,
        le=10,
        description="Depth for downstream impact analysis"
    )
    # Multi-tenant support: Optional OpenMetadata credentials
    openmetadata_host_port: str | None = Field(
        default=None,
        description="OpenMetadata API endpoint (e.g., https://your-om.com/api). If not provided, uses server default.",
        examples=["https://sandbox.open-metadata.org/api", "http://localhost:8585/api"]
    )
    openmetadata_jwt_token: str | None = Field(
        default=None,
        description="JWT token for OpenMetadata authentication. If not provided, uses server default.",
    )


class DiagnosisResponse(BaseModel):
    """Response containing diagnosis results."""
    incident_id: str
    target_asset: str
    severity: Severity
    confidence_score: float = Field(ge=0.0, le=1.0)
    primary_root_cause: AnomalyDetail | None = None
    contributing_factors: list[AnomalyDetail] = Field(default_factory=list)
    impacted_assets: ImpactedAssets
    suggested_fixes: list[SuggestedFix] = Field(default_factory=list)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    execution_time_ms: float | None = None


class HealthCheckResponse(BaseModel):
    """Health check response."""
    status: str
    app_name: str
    version: str
    openmetadata_connected: bool
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class DemoScenarioResponse(BaseModel):
    """Response for demo scenario."""
    message: str
    demo_fqn: str
    diagnosis: DiagnosisResponse
