"""Diagnosis endpoints for root cause analysis."""
import time
from datetime import datetime, timezone
from uuid import uuid4

from fastapi import APIRouter, HTTPException, status

from src.api.dependencies import MetadataClientDep
from src.config import settings
from src.constants import AnomalyType, Severity
from src.core.ai_layer import enhance_suggestions_with_ai
from src.core.confidence import calculate_confidence_score
from src.core.detection import evaluate_asset_anomalies
from src.core.impact import compute_blast_radius_by_fqn
from src.core.root_cause import find_root_cause_by_fqn
from src.core.suggestions import generate_suggested_fixes
from src.schemas import (
    AnomalyDetail,  # Added for target anomaly creation
    DemoScenarioResponse,
    DiagnosisRequest,
    DiagnosisResponse,
    ImpactedAssets,
)

router = APIRouter()


@router.post("/diagnose", response_model=DiagnosisResponse)
async def diagnose_asset(
    request: DiagnosisRequest,
    metadata_client: MetadataClientDep,
    enhance_with_ai: bool = False,  # Optional AI enhancement
    apply_governance_tags: bool = False  # ← NEW: Optional governance tagging
):
    """
    Diagnose an asset for anomalies and root causes.
    
    This endpoint orchestrates the complete diagnosis pipeline:
    1. **Phase 1 (Detection)**: Evaluate target asset for anomalies
    2. **Phase 2 (Root Cause)**: Perform upstream BFS to find root causes
    3. **Phase 3 (Confidence)**: Calculate confidence score
    4. **Phase 3 (Impact)**: Perform downstream impact analysis
    5. **Phase 4 (Suggestions)**: Generate fix recommendations
    6. **Phase 5 (Optional)**: AI-enhance suggestions with context
    7. **Phase 5+ (Optional)**: Apply governance tags to mark unreliable assets
    
    **Algorithm (from data_doctor.md Section 10):**
    - Fetch target entity from OpenMetadata
    - Run detection rules on target
    - If anomalies found, traverse upstream lineage
    - Calculate confidence based on depth and noise
    - Calculate downstream blast radius
    - Map anomalies to fix actions
    - Calculate severity based on impact
    - Optionally enhance with AI (if enhance_with_ai=true)
    - Optionally tag assets (if apply_governance_tags=true)
    
    **Phase 4 + 5 + 5+ Implementation**
    
    Args:
        request: Diagnosis request with target FQN
        metadata_client: OpenMetadata client (injected)
        enhance_with_ai: If True, use AI to add context-aware suggestions
        apply_governance_tags: If True, automatically tag assets with governance labels
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
        # Fetch historical versions and test cases for complete detection
        historical_versions = None
        test_cases = None
        
        if entity_type == "table":
            try:
                historical_versions = metadata_client.get_table_versions(entity_id, limit=30)
            except Exception as e:
                print(f"Warning: Could not fetch historical versions: {e}")
            
            try:
                test_cases = metadata_client.get_test_case_results(request.target_fqn)
            except Exception as e:
                print(f"Warning: Could not fetch test cases: {e}")
        
        target_anomalies = evaluate_asset_anomalies(
            asset_entity=target_entity,
            asset_type=entity_type,
            historical_versions=historical_versions,
            test_cases=test_cases
        )
        
        # Convert target anomalies to AnomalyDetail objects for inclusion in response
        target_anomaly_details = []
        for anomaly_type in target_anomalies:
            target_anomaly_details.append(AnomalyDetail(
                type=anomaly_type,
                name=request.target_fqn.split(".")[-1],  # Simple name
                depth=0,  # Target is at depth 0
                entity_id=entity_id,
                entity_type=entity_type,
                description=f"{anomaly_type.value} detected in target {entity_type}"
            ))
        
        # Step 2: Perform root cause analysis (upstream BFS)
        root_cause_result = find_root_cause_by_fqn(
            metadata_client=metadata_client,
            target_fqn=request.target_fqn,
            target_entity_type=entity_type,
            upstream_depth=request.upstream_depth
        )
        
        primary_cause = root_cause_result.get("primary_root_cause")
        contributing_factors = root_cause_result.get("contributing_factors", [])
        
        # Step 2.5: Merge target anomalies with root cause results
        # If target has anomalies but no upstream root cause found, target IS the root cause
        if target_anomaly_details and not primary_cause:
            # Target entity is the root cause
            primary_cause = target_anomaly_details[0]
            contributing_factors = target_anomaly_details[1:] + contributing_factors
        elif target_anomaly_details:
            # Target has anomalies AND upstream root cause exists
            # Add target anomalies as contributing factors
            contributing_factors = target_anomaly_details + contributing_factors
        
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
        
        # Step 6: Generate fix suggestions (deterministic)
        suggested_fixes = generate_suggested_fixes(
            primary_cause=primary_cause,
            contributing_factors=contributing_factors
        )
        
        # Calculate execution time
        execution_time_ms = (time.time() - start_time) * 1000
        
        # Build response
        diagnosis = DiagnosisResponse(
            incident_id=str(uuid4()),
            target_asset=request.target_fqn,
            severity=severity,
            confidence_score=confidence_score,
            primary_root_cause=primary_cause,
            contributing_factors=contributing_factors,
            impacted_assets=impacted_assets,
            suggested_fixes=suggested_fixes,
            timestamp=datetime.now(timezone.utc),
            execution_time_ms=execution_time_ms
        )
        
        # Step 7 (Optional): AI-enhance suggestions with context
        if enhance_with_ai and settings.GROQ_API_KEY:
            try:
                enhanced_fixes = enhance_suggestions_with_ai(
                    base_fixes=suggested_fixes,
                    diagnosis=diagnosis
                )
                diagnosis.suggested_fixes = enhanced_fixes
            except Exception as e:
                # If AI enhancement fails, continue with base suggestions
                print(f"AI enhancement failed, using base suggestions: {e}")
        
        # Step 8 (Optional): Apply governance tags
        if apply_governance_tags or settings.ENABLE_GOVERNANCE_TAGGING:
            try:
                from src.core.governance import tag_unreliable_assets
                
                tagged_counts = tag_unreliable_assets(
                    metadata_client=metadata_client,
                    diagnosis=diagnosis,
                    tag_impacted_assets=settings.TAG_IMPACTED_ASSETS
                )
                
                # Log tagging results
                print(f"Governance tagging completed: "
                      f"{tagged_counts['target']} target, "
                      f"{tagged_counts['root_cause']} root cause, "
                      f"{tagged_counts['impacted']} impacted assets")
                
                if tagged_counts['errors']:
                    print(f"Tagging errors: {len(tagged_counts['errors'])}")
                    for error in tagged_counts['errors'][:3]:  # Show first 3 errors
                        print(f"  - {error}")
                        
            except Exception as e:
                # Don't fail diagnosis if tagging fails
                print(f"Governance tagging failed: {e}")
        
        return diagnosis
    
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
