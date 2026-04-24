"""Root cause analysis engine using BFS upstream traversal.

Implements the algorithm from data_doctor.md Section 5:
- BFS upstream traversal
- Anomaly detection at each node and edge
- Primary root cause identification (minimum depth)
- Contributing factors collection

Phase 2 Implementation.

References:
- data_doctor.md Section 5 (root_cause_engine.md)
- BFS graph traversal best practices
- OpenMetadata lineage API structure
"""
from collections import deque
from typing import Any

from src.constants import DEFAULT_UPSTREAM_DEPTH, AnomalyType
from src.core.api_client import OpenMetadataClient
from src.core.lineage import (
    fetch_lineage_graph,
    build_upstream_adjacency_list,
    build_nodes_map,
    extract_pipeline_from_edge,
)
from src.core.detection import (
    evaluate_asset_anomalies,
    detect_pipeline_failure,
)
from src.schemas import AnomalyDetail


def find_root_cause(
    metadata_client: OpenMetadataClient,
    target_entity_id: str,
    target_entity_type: str,
    upstream_depth: int = DEFAULT_UPSTREAM_DEPTH
) -> dict[str, Any]:
    """
    Execute BFS upstream traversal to find root causes.
    
    Algorithm (from data_doctor.md Section 5):
    1. Fetch lineage graph from OpenMetadata
    2. Build adjacency list from upstreamEdges
    3. BFS traversal, checking each node and edge for anomalies
    4. Track all anomalies with their depth
    5. Sort by depth: closest = primary, rest = contributing
    
    The algorithm catalogs ALL upstream anomalies to build a comprehensive
    diagnosis, rather than stopping at the first anomaly found.
    
    Args:
        metadata_client: OpenMetadata client
        target_entity_id: UUID of the target entity
        target_entity_type: Type of entity (e.g., "table", "pipeline")
        upstream_depth: How many hops to traverse upstream (default: 5)
    
    Returns:
        {
            "primary_root_cause": AnomalyDetail | None,
            "contributing_factors": list[AnomalyDetail]
        }
    
    Reference: data_doctor.md Section 5 - Pseudocode: Root Cause Detection Engine
    """
    # Step 1: Fetch lineage graph
    lineage_graph = fetch_lineage_graph(
        metadata_client=metadata_client,
        entity_type=target_entity_type,
        entity_id=target_entity_id,
        upstream_depth=upstream_depth,
        downstream_depth=0  # Only need upstream for root cause
    )
    
    # Step 2: Build data structures for traversal
    nodes_map = build_nodes_map(lineage_graph.get("nodes", []))
    upstream_adj = build_upstream_adjacency_list(lineage_graph.get("upstreamEdges", []))
    
    # Step 3: Initialize BFS
    queue = deque([(target_entity_id, 0)])  # (node_id, current_depth)
    visited = set([target_entity_id])
    all_anomalies: list[AnomalyDetail] = []
    
    # Step 4: BFS traversal
    while queue:
        current_id, depth = queue.popleft()
        
        # Get parent edges for this node
        parent_edges = upstream_adj.get(current_id, [])
        
        for edge in parent_edges:
            parent_id = edge.get("fromEntity")
            next_depth = depth + 1
            
            # Check edge for pipeline failures
            edge_anomalies = check_edge_anomalies(metadata_client, edge, next_depth)
            all_anomalies.extend(edge_anomalies)
            
            # Process parent node if not visited
            if parent_id and parent_id not in visited:
                visited.add(parent_id)
                parent_node = nodes_map.get(parent_id)
                
                if parent_node:
                    # Check node for anomalies
                    node_anomalies = check_node_anomalies(
                        metadata_client,
                        parent_node,
                        next_depth
                    )
                    all_anomalies.extend(node_anomalies)
                    
                    # Add to queue for further traversal
                    queue.append((parent_id, next_depth))
    
    # Step 5: Sort anomalies by depth and categorize
    all_anomalies.sort(key=lambda x: x.depth)
    
    return {
        "primary_root_cause": all_anomalies[0] if all_anomalies else None,
        "contributing_factors": all_anomalies[1:] if len(all_anomalies) > 1 else []
    }


def check_node_anomalies(
    metadata_client: OpenMetadataClient,
    node: dict[str, Any],
    depth: int
) -> list[AnomalyDetail]:
    """
    Check a node for anomalies.
    
    Fetches the full entity details and runs all applicable detection rules.
    
    Args:
        metadata_client: OpenMetadata client
        node: Node from lineage graph (contains id, type, name, fullyQualifiedName)
        depth: Current traversal depth
    
    Returns:
        List of detected anomalies at this node
    
    Reference: data_doctor.md Section 5 - "Check for schema, data quality, or freshness issues"
    """
    anomalies: list[AnomalyDetail] = []
    
    node_id = node.get("id")
    node_type = node.get("type")
    node_name = node.get("name")
    node_fqn = node.get("fullyQualifiedName")
    
    if not node_id or not node_type:
        return anomalies
    
    try:
        # Fetch full entity details based on type
        if node_type == "table":
            # Fetch table with all observability fields
            entity = metadata_client.get_table_by_fqn(
                fqn=node_fqn,
                fields=["profile", "testSuite", "changeDescription"]
            )
            
            # Get historical data for volume/distribution anomalies
            historical_versions = metadata_client.get_table_versions(node_id, limit=30)
            
            # Get test cases for data quality checks
            test_cases = metadata_client.get_test_case_results(node_fqn)
            
            # Run all detection rules
            detected_types = evaluate_asset_anomalies(
                asset_entity=entity,
                asset_type="table",
                historical_versions=historical_versions,
                test_cases=test_cases
            )
            
        elif node_type == "pipeline":
            # Fetch pipeline entity
            entity = metadata_client.get_pipeline_by_fqn(node_fqn)
            
            # Run pipeline detection rules
            detected_types = evaluate_asset_anomalies(
                asset_entity=entity,
                asset_type="pipeline"
            )
        else:
            # Other entity types not yet supported
            return anomalies
        
        # Convert detected anomaly types to AnomalyDetail objects
        for anomaly_type in detected_types:
            anomalies.append(AnomalyDetail(
                type=anomaly_type,
                name=node_name or node_fqn,
                depth=depth,
                entity_id=node_id,
                description=f"{anomaly_type.value} detected in {node_type} {node_name}"
            ))
    
    except Exception as e:
        # Log error but continue traversal
        # In production, would use proper logging
        pass
    
    return anomalies


def check_edge_anomalies(
    metadata_client: OpenMetadataClient,
    edge: dict[str, Any],
    depth: int
) -> list[AnomalyDetail]:
    """
    Check an edge for anomalies (e.g., pipeline failures).
    
    Edges can contain lineageDetails with pipeline references that indicate
    which transformation job connects the source and target entities.
    
    Args:
        metadata_client: OpenMetadataClient
        edge: Edge from lineage graph
        depth: Current traversal depth
    
    Returns:
        List of detected anomalies on this edge
    
    Reference: data_doctor.md Section 5 - "Evaluate Pipeline Execution State on the edge"
    """
    anomalies: list[AnomalyDetail] = []
    
    # Extract pipeline from edge
    pipeline_ref = extract_pipeline_from_edge(edge)
    if not pipeline_ref:
        return anomalies
    
    pipeline_id = pipeline_ref.get("id")
    pipeline_name = pipeline_ref.get("name")
    pipeline_fqn = pipeline_ref.get("fullyQualifiedName")
    
    if not pipeline_fqn:
        return anomalies
    
    try:
        # Fetch pipeline entity
        pipeline_entity = metadata_client.get_pipeline_by_fqn(pipeline_fqn)
        
        # Check for pipeline failure
        failure = detect_pipeline_failure(pipeline_entity)
        if failure:
            anomalies.append(AnomalyDetail(
                type=AnomalyType.PIPELINE_FAILURE,
                name=pipeline_name or pipeline_fqn,
                depth=depth,
                entity_id=pipeline_id,
                description=f"Pipeline {pipeline_name} failed on transformation edge"
            ))
    
    except Exception as e:
        # Log error but continue traversal
        pass
    
    return anomalies


def find_root_cause_by_fqn(
    metadata_client: OpenMetadataClient,
    target_fqn: str,
    target_entity_type: str = "table",
    upstream_depth: int = DEFAULT_UPSTREAM_DEPTH
) -> dict[str, Any]:
    """
    Convenience method to find root cause using FQN instead of UUID.
    
    Args:
        metadata_client: OpenMetadata client
        target_fqn: Fully qualified name of the target entity
        target_entity_type: Type of entity (default: "table")
        upstream_depth: How many hops to traverse upstream
    
    Returns:
        Same as find_root_cause()
    """
    # Fetch entity to get its ID
    if target_entity_type == "table":
        entity = metadata_client.get_table_by_fqn(target_fqn)
    elif target_entity_type == "pipeline":
        entity = metadata_client.get_pipeline_by_fqn(target_fqn)
    else:
        raise ValueError(f"Unsupported entity type: {target_entity_type}")
    
    entity_id = entity.get("id")
    if not entity_id:
        raise ValueError(f"Could not extract ID from entity {target_fqn}")
    
    return find_root_cause(
        metadata_client=metadata_client,
        target_entity_id=entity_id,
        target_entity_type=target_entity_type,
        upstream_depth=upstream_depth
    )
