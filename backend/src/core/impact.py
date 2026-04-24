"""Impact analysis engine for calculating downstream blast radius.

Implements downstream BFS traversal from data_doctor.md Section 7:
- Downstream lineage traversal
- Asset categorization (tables, dashboards, ML models)
- Total impact count

Phase 3 Implementation.
"""
from collections import deque
from typing import Any

from src.constants import DEFAULT_DOWNSTREAM_DEPTH
from src.schemas import ImpactedAssets


def compute_blast_radius(
    metadata_client: Any,
    root_entity_id: str,
    root_entity_type: str,
    downstream_depth: int = DEFAULT_DOWNSTREAM_DEPTH
) -> ImpactedAssets:
    """
    Calculate downstream blast radius using BFS.
    
    Algorithm:
    1. Fetch lineage graph with downstream edges
    2. Build downstream adjacency list
    3. BFS traversal collecting all downstream nodes
    4. Categorize by entity type (table, dashboard, mlmodel)
    5. Return structured impact data
    
    Args:
        metadata_client: OpenMetadata client
        root_entity_id: UUID of the root anomaly entity
        root_entity_type: Type of entity
        downstream_depth: How many hops to traverse downstream
    
    Returns:
        ImpactedAssets with categorized affected assets
    """
    # TODO: Phase 3 - Implement
    # 1. Fetch lineage graph
    # 2. Build downstream adjacency list
    # 3. BFS traversal
    # 4. Categorize nodes by type
    # 5. Return ImpactedAssets
    raise NotImplementedError("Phase 3")


def categorize_impacted_assets(
    nodes_map: dict[str, dict[str, Any]],
    visited_ids: set[str]
) -> ImpactedAssets:
    """
    Categorize visited nodes into asset types.
    
    Args:
        nodes_map: Map of node_id -> node_data
        visited_ids: Set of visited node IDs from BFS
    
    Returns:
        ImpactedAssets with categorized lists
    """
    # TODO: Phase 3 - Implement
    impacted = ImpactedAssets()
    
    for node_id in visited_ids:
        node = nodes_map.get(node_id)
        if not node:
            continue
        
        entity_type = node.get("type")
        
        if entity_type == "table":
            impacted.tables.append(node)
        elif entity_type == "dashboard":
            impacted.dashboards.append(node)
        elif entity_type == "mlmodel":
            impacted.ml_models.append(node)
    
    impacted.total_impact_count = (
        len(impacted.tables) +
        len(impacted.dashboards) +
        len(impacted.ml_models)
    )
    
    return impacted
