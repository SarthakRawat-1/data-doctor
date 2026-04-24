"""Lineage engine for fetching and processing lineage graphs.

Handles:
- Fetching lineage from OpenMetadata API
- Building adjacency lists for graph traversal
- Parsing nodes and edges

Phase 2 Implementation.

References:
- data_doctor.md Section 3 (lineage_system.md)
- OpenMetadata Lineage API documentation
- BFS graph traversal best practices
"""
from typing import Any

from src.core.api_client import OpenMetadataClient


def fetch_lineage_graph(
    metadata_client: OpenMetadataClient,
    entity_type: str,
    entity_id: str,
    upstream_depth: int = 5,
    downstream_depth: int = 5
) -> dict[str, Any]:
    """
    Fetch lineage graph from OpenMetadata.
    
    Uses the GET /v1/lineage/{entityType}/{id} endpoint to retrieve
    the complete lineage subgraph with specified traversal depths.
    
    Args:
        metadata_client: OpenMetadata client instance
        entity_type: Type of entity (e.g., "table", "pipeline")
        entity_id: UUID of the entity
        upstream_depth: Upstream traversal depth (default: 5, max: 3 per API call)
        downstream_depth: Downstream traversal depth (default: 5, max: 3 per API call)
    
    Returns:
        Lineage graph with structure:
        {
            "entity": {
                "id": "...",
                "type": "table",
                "name": "...",
                "fullyQualifiedName": "...",
                "deleted": false
            },
            "nodes": [
                {"id": "...", "type": "...", "name": "...", "fullyQualifiedName": "..."},
                ...
            ],
            "upstreamEdges": [
                {"fromEntity": "source_id", "toEntity": "target_id", "lineageDetails": {...}},
                ...
            ],
            "downstreamEdges": [
                {"fromEntity": "source_id", "toEntity": "target_id"},
                ...
            ]
        }
    
    Reference: data_doctor.md Section 3 - Lineage JSON Structure Analysis
    """
    return metadata_client.get_lineage(
        entity_type=entity_type,
        entity_id=entity_id,
        upstream_depth=upstream_depth,
        downstream_depth=downstream_depth
    )


def fetch_lineage_graph_by_fqn(
    metadata_client: OpenMetadataClient,
    entity_type: str,
    fqn: str,
    upstream_depth: int = 5,
    downstream_depth: int = 5
) -> dict[str, Any]:
    """
    Fetch lineage graph by Fully Qualified Name.
    
    Convenience method for fetching lineage when you have FQN instead of UUID.
    
    Args:
        metadata_client: OpenMetadata client instance
        entity_type: Type of entity (e.g., "table")
        fqn: Fully qualified name (e.g., "snowflake.db.schema.table")
        upstream_depth: Upstream traversal depth
        downstream_depth: Downstream traversal depth
    
    Returns:
        Lineage graph (same structure as fetch_lineage_graph)
    """
    return metadata_client.get_lineage_by_fqn(
        entity_type=entity_type,
        fqn=fqn,
        upstream_depth=upstream_depth,
        downstream_depth=downstream_depth
    )


def build_upstream_adjacency_list(upstream_edges: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """
    Build adjacency list for upstream traversal.
    
    Converts the upstreamEdges array into a dictionary mapping each node
    to its parent edges, enabling efficient BFS traversal.
    
    Args:
        upstream_edges: List of upstream edges from lineage API
            Format: [{"fromEntity": "parent_id", "toEntity": "child_id", ...}, ...]
    
    Returns:
        Dictionary mapping node_id -> list of parent edges
        Format: {
            "child_id": [
                {"fromEntity": "parent_id", "toEntity": "child_id", "lineageDetails": {...}},
                ...
            ]
        }
    
    Example:
        Input: [
            {"fromEntity": "A", "toEntity": "B"},
            {"fromEntity": "C", "toEntity": "B"},
            {"fromEntity": "B", "toEntity": "D"}
        ]
        Output: {
            "B": [
                {"fromEntity": "A", "toEntity": "B"},
                {"fromEntity": "C", "toEntity": "B"}
            ],
            "D": [
                {"fromEntity": "B", "toEntity": "D"}
            ]
        }
    
    Reference: data_doctor.md Section 5 - Upstream Traversal Algorithm
    """
    adjacency_list: dict[str, list[dict[str, Any]]] = {}
    
    for edge in upstream_edges:
        to_entity = edge.get("toEntity")
        if to_entity:
            if to_entity not in adjacency_list:
                adjacency_list[to_entity] = []
            adjacency_list[to_entity].append(edge)
    
    return adjacency_list


def build_downstream_adjacency_list(downstream_edges: list[dict[str, Any]]) -> dict[str, list[str]]:
    """
    Build adjacency list for downstream traversal.
    
    Converts the downstreamEdges array into a dictionary mapping each node
    to its child node IDs, enabling efficient BFS traversal for impact analysis.
    
    Args:
        downstream_edges: List of downstream edges from lineage API
            Format: [{"fromEntity": "parent_id", "toEntity": "child_id"}, ...]
    
    Returns:
        Dictionary mapping node_id -> list of child node IDs
        Format: {
            "parent_id": ["child_id_1", "child_id_2", ...]
        }
    
    Example:
        Input: [
            {"fromEntity": "A", "toEntity": "B"},
            {"fromEntity": "A", "toEntity": "C"},
            {"fromEntity": "B", "toEntity": "D"}
        ]
        Output: {
            "A": ["B", "C"],
            "B": ["D"]
        }
    
    Reference: data_doctor.md Section 7 - Downstream Lineage Traversal
    """
    adjacency_list: dict[str, list[str]] = {}
    
    for edge in downstream_edges:
        from_entity = edge.get("fromEntity")
        to_entity = edge.get("toEntity")
        
        if from_entity and to_entity:
            if from_entity not in adjacency_list:
                adjacency_list[from_entity] = []
            adjacency_list[from_entity].append(to_entity)
    
    return adjacency_list


def build_nodes_map(nodes: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """
    Build a lookup map of nodes by ID.
    
    Creates a dictionary for O(1) node lookup during graph traversal.
    The nodes array from the lineage API contains all entities in the subgraph.
    
    Args:
        nodes: List of nodes from lineage API
            Format: [
                {"id": "...", "type": "table", "name": "...", "fullyQualifiedName": "..."},
                ...
            ]
    
    Returns:
        Dictionary mapping node_id -> node_data
        Format: {
            "node_id": {"id": "...", "type": "...", "name": "...", ...}
        }
    
    Reference: data_doctor.md Section 3 - Lineage JSON Structure Analysis
    """
    return {node["id"]: node for node in nodes if "id" in node}


def extract_pipeline_from_edge(edge: dict[str, Any]) -> dict[str, Any] | None:
    """
    Extract pipeline information from a lineage edge.
    
    Edges can contain lineageDetails with pipeline references that indicate
    which transformation job connects the source and target entities.
    
    Args:
        edge: Edge from upstreamEdges or downstreamEdges
    
    Returns:
        Pipeline reference dict if present, None otherwise
        Format: {"id": "...", "type": "pipeline", "name": "...", "fullyQualifiedName": "..."}
    
    Reference: data_doctor.md Section 3 - "edges can contain nested lineageDetails objects"
    """
    lineage_details = edge.get("lineageDetails")
    if lineage_details:
        pipeline = lineage_details.get("pipeline")
        if pipeline:
            return pipeline
    
    return None
