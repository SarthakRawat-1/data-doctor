"""Diagnosis endpoints for root cause analysis."""
import time
from datetime import datetime
from uuid import uuid4

from fastapi import APIRouter, HTTPException, status

from src.api.dependencies import MetadataClientDep
from src.config import settings
from src.constants import AnomalyType, Severity
from src.core.confidence import calculate_confidence_score
from src.core.detection import evaluate_asset_anomalies
from src.core.impact import compute_blast_radius_by_fqn
from src.core.root_cause import find_root_cause_by_fqn
from src.core.suggestions import generate_suggested_fixes
from src.schemas import (
    DemoScenarioResponse,
    DiagnosisRequest,
    DiagnosisResponse,
    ImpactedAssets,
)

router = APIRouter()


@router.post("/diagnose", response_model=DiagnosisResponse)
async def diagnose_asset(
    request: DiagnosisRequest,
    metadata_client: MetadataClientDep
):
    """
    Diagnose an asset for anomalies and root causes.
    
    This endpoint orchestrates the complete diagnosis pipeline:
    1. **Phase 1 (Detection)**: Evaluate target asset for anomalies
    2. **Phase 2 (Root Cause)**: Perform upstream BFS to find root causes
    3. **Phase 3 (Confidence)**: Calculate confidence score
    4. **Phase 3 (Impact)**: Perform downstream impact analysis
    5. **Phase 4 (Suggestions)**: Generate fix recommendations
    
    **Algorithm (from data_doctor.md Section 10):**
    - Fetch target entity from OpenMetadata
    - Run detection rules on target
    - If anomalies found, traverse upstream lineage
    - Calculate confidence based on depth and noise
    - Calculate downstream blast radius
    - Map anomalies to fix actions
    - Calculate severity based on impact
    
    **Phase 4 Implementation**
    """
    start_time = time.time()
    
    try:
        # Step 1: Fetch target entity and detect anomalies
        # Determine entity type from FQN pattern
        entity_type = _infer_entity_type(request.target_fqn)
        
        # Fetch entity
        if entity_type == "table":
            target_entity = metadata_client.get_table_by_fqn(request.target_fqn)
        elif entity_type == "pipeline":
            target_entity = metadata_client.get_pipeline_by_fqn(request.target_fqn)
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unsupported entity type inferred from FQN: {entity_type}"
            )
        
        # Get entity ID
        entity_id = target_entity.get("id")
        if not entity_id:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Entity not found: {request.target_fqn}"
            )
        
        # Detect anomalies in target asset
        target_anomalies = evaluate_asset_anomalies(
            metadata_client=metadata_client,
            entity=target_entity,
            entity_type=entity_type
        )
        
        # Step 2: Perform root cause analysis (upstream BFS)
        root_cause_result = find_root_cause_by_fqn(
            metadata_client=metadata_client,
            target_fqn=request.target_fqn,
            target_entity_type=entity_type,
            upstream_depth=request.upstream_depth
        )
        
        primary_cause = root_cause_result.get("primary_root_cause")
        contributing_factors = root_cause_result.get("contributing_factors", [])
        
        # Step 3: Calculate confidence score
        confidence_score = calculate_confidence_score(
            primary_cause=primary_cause,
            contributing_factors=contributing_factors
        )
        
        # Step 4: Perform impact analysis (downstream BFS)
        impacted_assets = compute_blast_radius_by_fqn(
            metadata_client=metadata_client,
            root_fqn=request.target_fqn,
            root_entity_type=entity_type,
            downstream_depth=request.downstream_depth
        )
        
        # Step 5: Calculate severity based on blast radius
        severity = _calculate_severity(impacted_assets)
        
        # Step 6: Generate fix suggestions
        suggested_fixes = generate_suggested_fixes(
            primary_cause=primary_cause,
            contributing_factors=contributing_factors
        )
        
        # Calculate execution time
        execution_time_ms = (time.time() - start_time) * 1000
        
        # Build response
        return DiagnosisResponse(
            incident_id=str(uuid4()),
            target_asset=request.target_fqn,
            severity=severity,
            confidence_score=confidence_score,
            primary_root_cause=primary_cause,
            contributing_factors=contributing_factors,
            impacted_assets=impacted_assets,
            suggested_fixes=suggested_fixes,
            timestamp=datetime.utcnow(),
            execution_time_ms=execution_time_ms
        )
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Diagnosis failed: {str(e)}"
        )


@router.get("/demo", response_model=DemoScenarioResponse)
async def run_demo_scenario(metadata_client: MetadataClientDep):
    """
    Run a pre-staged demo scenario.
    
    This endpoint demonstrates Data Doctor's capabilities using a
    pre-configured broken asset in OpenMetadata.
    
    Perfect for hackathon demos and presentations!
    
    **Demo Flow (from data_doctor.md Section 13):**
    1. User clicks "Run Demo Scenario"
    2. System queries pre-staged FQN from config
    3. Runs full diagnosis pipeline
    4. Returns results in < 3 seconds
    """
    demo_fqn = settings.DEMO_SCENARIO_FQN
    
    # Run diagnosis on demo FQN
    diagnosis_request = DiagnosisRequest(
        target_fqn=demo_fqn,
        upstream_depth=5,
        downstream_depth=5
    )
    
    diagnosis = await diagnose_asset(diagnosis_request, metadata_client)
    
    return DemoScenarioResponse(
        message=f"Demo scenario executed successfully for {demo_fqn}",
        demo_fqn=demo_fqn,
        diagnosis=diagnosis
    )


def _infer_entity_type(fqn: str) -> str:
    """
    Infer entity type from FQN pattern.
    
    OpenMetadata FQN patterns:
    - Tables: service.database.schema.table
    - Pipelines: service.pipeline_name
    - Dashboards: service.dashboard_name
    
    Args:
        fqn: Fully qualified name
    
    Returns:
        Entity type string ("table", "pipeline", "dashboard")
    """
    parts = fqn.split(".")
    
    # Heuristic: Tables typically have 4+ parts (service.db.schema.table)
    # Pipelines/Dashboards typically have 2 parts (service.name)
    if len(parts) >= 3:
        return "table"
    else:
        # Default to pipeline for 2-part FQNs
        # In production, would query OpenMetadata search API to determine type
        return "pipeline"


def _calculate_severity(impacted_assets: ImpactedAssets) -> Severity:
    """
    Calculate severity based on blast radius.
    
    **Rules (from data_doctor.md Section 15):**
    - HIGH: Impacts consumption assets (dashboards, ML models) OR > 3 tables
    - MEDIUM: Impacts tables but not consumption layers
    - LOW: No downstream impact or isolated issue
    
    Args:
        impacted_assets: Downstream impact analysis results
    
    Returns:
        Severity level
    """
    # HIGH: Consumption assets impacted
    if len(impacted_assets.dashboards) > 0 or len(impacted_assets.ml_models) > 0:
        return Severity.HIGH
    
    # HIGH: More than 3 tables impacted
    if len(impacted_assets.tables) > 3:
        return Severity.HIGH
    
    # MEDIUM: Some tables impacted
    if len(impacted_assets.tables) > 0:
        return Severity.MEDIUM
    
    # LOW: No downstream impact
    return Severity.LOW
