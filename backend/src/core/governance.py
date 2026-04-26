"""Governance tagging for marking unreliable assets.

Automatically tags assets with governance labels when anomalies are detected,
marking them as "unreliable" or "under investigation" so data consumers
know not to trust them.

Phase 5+ Optional Enhancement - Section 9.

References:
- OpenMetadata Tagging: https://docs.open-metadata.org/latest/sdk/python/ingestion/tags
- Monte Carlo Data Incident Tagging
- Datadog Data Quality Badges
"""
from typing import Any

from src.constants import Severity
from src.core.api_client import OpenMetadataClient
from src.schemas import DiagnosisResponse


# Tag mapping based on severity
SEVERITY_TAG_MAPPING = {
    Severity.HIGH: "DataQuality.Critical",
    Severity.MEDIUM: "DataQuality.Warning",
    Severity.LOW: "DataQuality.UnderInvestigation"
}

# Root cause tag
ROOT_CAUSE_TAG = "DataQuality.RootCause"

# Affected asset tag
AFFECTED_TAG = "DataQuality.Affected"


def tag_unreliable_assets(
    metadata_client: OpenMetadataClient,
    diagnosis: DiagnosisResponse,
    tag_impacted_assets: bool = True
) -> dict[str, int]:
    """
    Tag assets with governance labels based on diagnosis.
    
    Tags applied:
    - Target asset: Tagged based on severity (Critical/Warning/UnderInvestigation)
    - Primary root cause: Tagged as "DataQuality.RootCause"
    - Impacted assets (optional): Tagged as "DataQuality.Affected"
    
    This creates visible warnings in OpenMetadata UI that prevent users
    from consuming unreliable data.
    
    Args:
        metadata_client: OpenMetadata client
        diagnosis: Complete diagnosis response
        tag_impacted_assets: Whether to tag downstream impacted assets
    
    Returns:
        Dictionary with counts of tagged assets:
        {
            "target": int,
            "root_cause": int,
            "impacted": int,
            "errors": list[str]
        }
    
    Reference: data_doctor.md Section 9 - Governance Tagging
    """
    tagged_count = {
        "target": 0,
        "root_cause": 0,
        "impacted": 0,
        "errors": []
    }
    
    # Tag target asset based on severity
    try:
        # Get target entity to extract ID
        target_fqn = diagnosis.target_asset
        entity_type = _infer_entity_type_from_fqn(target_fqn)
        
        # Fetch entity to get ID
        if entity_type == "table":
            target_entity = metadata_client.get_table_by_fqn(target_fqn)
        elif entity_type == "pipeline":
            target_entity = metadata_client.get_pipeline_by_fqn(target_fqn)
        else:
            tagged_count["errors"].append(f"Unsupported entity type for target: {entity_type}")
            return tagged_count
        
        target_id = target_entity.get("id")
        if target_id:
            severity_tag = SEVERITY_TAG_MAPPING.get(diagnosis.severity)
            if severity_tag:
                metadata_client.patch_entity_tag(
                    entity_type=entity_type,
                    entity_id=target_id,
                    tag_fqn=severity_tag
                )
                tagged_count["target"] = 1
    except Exception as e:
        error_msg = f"Failed to tag target asset: {e}"
        tagged_count["errors"].append(error_msg)
        print(error_msg)
    
    # Tag primary root cause
    if diagnosis.primary_root_cause:
        try:
            entity_id = diagnosis.primary_root_cause.entity_id
            entity_type = diagnosis.primary_root_cause.entity_type  # Use stored entity type
            
            if entity_id and entity_type:
                metadata_client.patch_entity_tag(
                    entity_type=entity_type,
                    entity_id=entity_id,
                    tag_fqn=ROOT_CAUSE_TAG
                )
                tagged_count["root_cause"] = 1
            else:
                error_msg = f"Missing entity_id or entity_type for root cause: {diagnosis.primary_root_cause.name}"
                tagged_count["errors"].append(error_msg)
                print(error_msg)
        except Exception as e:
            error_msg = f"Failed to tag root cause: {e}"
            tagged_count["errors"].append(error_msg)
            print(error_msg)
    
    # Tag impacted assets (optional)
    if tag_impacted_assets:
        # Tag impacted tables
        for table in diagnosis.impacted_assets.tables:
            try:
                table_id = table.get("id")
                if table_id:
                    metadata_client.patch_entity_tag(
                        entity_type="table",
                        entity_id=table_id,
                        tag_fqn=AFFECTED_TAG
                    )
                    tagged_count["impacted"] += 1
            except Exception as e:
                error_msg = f"Failed to tag impacted table {table.get('name', 'unknown')}: {e}"
                tagged_count["errors"].append(error_msg)
                print(error_msg)
        
        # Tag impacted dashboards
        for dashboard in diagnosis.impacted_assets.dashboards:
            try:
                dashboard_id = dashboard.get("id")
                if dashboard_id:
                    metadata_client.patch_entity_tag(
                        entity_type="dashboard",
                        entity_id=dashboard_id,
                        tag_fqn=AFFECTED_TAG
                    )
                    tagged_count["impacted"] += 1
            except Exception as e:
                error_msg = f"Failed to tag impacted dashboard {dashboard.get('name', 'unknown')}: {e}"
                tagged_count["errors"].append(error_msg)
                print(error_msg)
        
        # Tag impacted ML models
        for ml_model in diagnosis.impacted_assets.ml_models:
            try:
                ml_model_id = ml_model.get("id")
                if ml_model_id:
                    metadata_client.patch_entity_tag(
                        entity_type="mlmodel",
                        entity_id=ml_model_id,
                        tag_fqn=AFFECTED_TAG
                    )
                    tagged_count["impacted"] += 1
            except Exception as e:
                error_msg = f"Failed to tag impacted ML model {ml_model.get('name', 'unknown')}: {e}"
                tagged_count["errors"].append(error_msg)
                print(error_msg)
    
    return tagged_count


def _infer_entity_type_from_fqn(fqn: str) -> str:
    """
    Infer entity type from FQN pattern.
    
    OpenMetadata FQN patterns:
    - Tables: service.database.schema.table (4+ parts)
    - Pipelines: service.pipeline_name (2 parts)
    - Dashboards: service.dashboard_name (2 parts)
    
    Args:
        fqn: Fully qualified name
    
    Returns:
        Entity type string ("table", "pipeline", "dashboard")
    """
    parts = fqn.split(".")
    
    # Heuristic: Tables typically have 4+ parts
    if len(parts) >= 3:
        return "table"
    else:
        # Default to pipeline for 2-part FQNs
        # In production, would query OpenMetadata search API
        return "pipeline"
