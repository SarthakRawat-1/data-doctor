"""Impact analysis engine for calculating downstream blast radius.

Implements downstream BFS traversal from data_doctor.md Section 7:
- Downstream lineage traversal
- Asset categorization (tables, dashboards, ML models)
- Total impact count

Phase 3 Implementation.

References:
- data_doctor.md Section 7 (impact_analysis.md)
- Industry research: Astronomer, Recce, DataGOL, Zenithdata
"""
from collections import deque
from typing import Any

from src.constants import DEFAULT_DOWNSTREAM_DEPTH
from src.core.api_client import OpenMetadataClient
from src.core.lineage import (
    fetch_lineage_graph,
    build_downstream_adjacency_list,
    build_nodes_map,
)
from src.schemas import ImpactedAssets


def compute_blast_radius(
    metadata_client: OpenMetadataClient,
    root_entity_id: str,
    root_entity_type: str,
    downstream_depth: int = DEFAULT_DOWNSTREAM_DEPTH
) -> ImpactedAssets:
    """
    Calculate downstream blast radius using BFS.
    
    Algorithm (from data_doctor.md Section 7):
    1. Fetch lineage graph with downstream edges only
    2. Build downstream adjacency list
    3. BFS traversal collecting all downstream nodes
    4. Categorize by entity type (table, dashboard, mlmodel)
    5. Return structured impact data
    
    This is the mirror of root cause analysis - instead of going upstream
    to find the cause, we go downstream to find the impact.
    
    Args:
        metadata_client: OpenMetadata client
        root_entity_id: UUID of the root anomaly entity
        root_entity_type: Type of entity (e.g., "table", "pipeline")
        downstream_depth: How many hops to traverse downstream (default: 5)
    
    Returns:
        ImpactedAssets with categorized affected assets
    
    Reference: data_doctor.md Section 7 - Pseudocode: compute_blast_radius
    """
    # Step 1: Fetch lineage graph (downstream only)
    lineage_graph = fetch_lineage_graph(
        metadata_client=metadata_client,
        entity_type=root_entity_type,
        entity_id=root_entity_id,
        upstream_depth=0,  # Don't need upstream for impact
        downstream_depth=downstream_depth
    )
    
    # Step 2: Build data structures for traversal
    nodes_map = build_nodes_map(lineage_graph.get("nodes", []))
    downstream_adj = build_downstream_adjacency_list(lineage_graph.get("downstreamEdges", []))
    
    # Step 3: Initialize BFS
    queue = deque([root_entity_id])
    visited = set([root_entity_id])
    
    # Step 4: BFS traversal
    while queue:
        current_id = queue.popleft()
        
        # Get children for this node
        children = downstream_adj.get(current_id, [])
        
        for child_id in children:
            if child_id not in visited:
                visited.add(child_id)
                queue.append(child_id)
    
    # Step 5: Categorize visited nodes (excluding root)
    visited.discard(root_entity_id)  # Don't include the root in impact
    impacted = categorize_impacted_assets(nodes_map, visited)
    
    return impacted


def categorize_impacted_assets(
    nodes_map: dict[str, dict[str, Any]],
    visited_ids: set[str]
) -> ImpactedAssets:
    """
    Categorize visited nodes into asset types.
    
    Groups downstream nodes by their entity type for structured reporting.
    This helps data stewards understand which dashboards, tables, and ML models
    are affected by an upstream failure.
    
    Args:
        nodes_map: Map of node_id -> node_data
        visited_ids: Set of visited node IDs from BFS
    
    Returns:
        ImpactedAssets with categorized lists
    
    Reference: data_doctor.md Section 7 - Asset categorization
    """
    impacted = ImpactedAssets()
    
    for node_id in visited_ids:
        node = nodes_map.get(node_id)
        if not node:
            continue
        
        entity_type = node.get("type")
        
        # Categorize by entity type
        if entity_type == "table":
            impacted.tables.append(node)
        elif entity_type == "dashboard":
            impacted.dashboards.append(node)
        elif entity_type == "mlmodel":
            impacted.ml_models.append(node)
        # Other types (pipeline, topic) are not included in impact
    
    # Calculate total impact
    impacted.total_impact_count = (
        len(impacted.tables) +
        len(impacted.dashboards) +
        len(impacted.ml_models)
    )
    
    return impacted


def compute_blast_radius_by_fqn(
    metadata_client: OpenMetadataClient,
    root_fqn: str,
    root_entity_type: str = "table",
    downstream_depth: int = DEFAULT_DOWNSTREAM_DEPTH
) -> ImpactedAssets:
    """
    Convenience method to compute blast radius using FQN instead of UUID.
    
    Args:
        metadata_client: OpenMetadata client
        root_fqn: Fully qualified name of the root entity
        root_entity_type: Type of entity (default: "table")
        downstream_depth: How many hops to traverse downstream
    
    Returns:
        ImpactedAssets with categorized affected assets
    """
    # Fetch entity to get its ID
    if root_entity_type == "table":
        entity = metadata_client.get_table_by_fqn(root_fqn)
    elif root_entity_type == "pipeline":
        entity = metadata_client.get_pipeline_by_fqn(root_fqn)
    else:
        raise ValueError(f"Unsupported entity type: {root_entity_type}")
    
    entity_id = entity.get("id")
    if not entity_id:
        raise ValueError(f"Could not extract ID from entity {root_fqn}")
    
    return compute_blast_radius(
        metadata_client=metadata_client,
        root_entity_id=entity_id,
        root_entity_type=root_entity_type,
        downstream_depth=downstream_depth
    )
