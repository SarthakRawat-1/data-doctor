"""Interactive demo endpoints for dataset and scenario management."""
from typing import List, Dict, Any
from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field

from src.api.dependencies import MetadataClientDep

router = APIRouter()


# Response models
class DatasetInfo(BaseModel):
    """Information about a dataset."""
    id: str = Field(..., description="Dataset identifier (ecommerce, healthcare, finance)")
    name: str = Field(..., description="Display name")
    description: str = Field(..., description="Dataset description")
    service_name: str = Field(..., description="OpenMetadata service name")
    database_name: str = Field(..., description="Database name")
    table_count: int = Field(..., description="Number of tables")
    icon: str = Field(..., description="Icon emoji")


class FQNInfo(BaseModel):
    """Information about an FQN in a dataset."""
    fqn: str = Field(..., description="Fully qualified name")
    table_name: str = Field(..., description="Table name")
    description: str = Field(..., description="Table description")
    row_count: int | None = Field(None, description="Approximate row count")


class ScenarioInfo(BaseModel):
    """Information about an anomaly scenario."""
    id: str = Field(..., description="Scenario identifier")
    name: str = Field(..., description="Display name")
    description: str = Field(..., description="Scenario description")
    anomaly_types: List[str] = Field(..., description="Types of anomalies created")
    severity: str = Field(..., description="Expected severity (LOW, MEDIUM, HIGH)")


class DatasetsResponse(BaseModel):
    """Response containing available datasets."""
    datasets: List[DatasetInfo]


class FQNsResponse(BaseModel):
    """Response containing FQNs for a dataset."""
    dataset_id: str
    fqns: List[FQNInfo]


class ScenariosResponse(BaseModel):
    """Response containing available scenarios."""
    scenarios: List[ScenarioInfo]


# Dataset configurations
DATASETS_CONFIG = {
    "ecommerce": {
        "name": "E-commerce",
        "description": "Customer orders and revenue data",
        "service_name": "sample_mysql_ecommerce",  # Base name, will be suffixed with scenario
        "database_name": "ecommerce",  # Base name, will be suffixed with scenario
        "tables": [
            {
                "name": "dim_customer",
                "description": "Customer dimension table with 500 customers",
                "row_count": 500
            },
            {
                "name": "fact_orders",
                "description": "Order transactions with 2,500 orders",
                "row_count": 2500
            },
            {
                "name": "fact_revenue",
                "description": "Daily revenue aggregations",
                "row_count": 365
            }
        ],
        "icon": "🛒"
    },
    "healthcare": {
        "name": "Healthcare",
        "description": "Patient visits and prescriptions",
        "service_name": "sample_mysql_healthcare",  # Base name
        "database_name": "healthcare",  # Base name
        "tables": [
            {
                "name": "dim_patient",
                "description": "Patient dimension table with 750 patients",
                "row_count": 750
            },
            {
                "name": "fact_visit",
                "description": "Hospital visits with 4,000 visits",
                "row_count": 4000
            },
            {
                "name": "fact_prescription",
                "description": "Prescription records with 6,000 prescriptions",
                "row_count": 6000
            },
            {
                "name": "fact_daily_metrics",
                "description": "Daily healthcare metrics",
                "row_count": 730
            }
        ],
        "icon": "🏥"
    },
    "finance": {
        "name": "Finance",
        "description": "Banking accounts and transactions",
        "service_name": "sample_mysql_finance",  # Base name
        "database_name": "finance",  # Base name
        "tables": [
            {
                "name": "dim_account",
                "description": "Account dimension table with 1,000 accounts",
                "row_count": 1000
            },
            {
                "name": "fact_transaction",
                "description": "Transaction records with 25,000 transactions",
                "row_count": 25000
            },
            {
                "name": "fact_balance_snapshot",
                "description": "Daily balance snapshots with 91,250 records",
                "row_count": 91250
            },
            {
                "name": "fact_daily_summary",
                "description": "Daily financial summaries",
                "row_count": 365
            }
        ],
        "icon": "💰"
    }
}


def get_service_name(dataset_id: str, scenario_id: str) -> str:
    """Get the OpenMetadata service name for a dataset-scenario combination."""
    return f"sample_mysql_{dataset_id}_{scenario_id}"


def get_database_name(dataset_id: str, scenario_id: str) -> str:
    """Get the database name for a dataset-scenario combination."""
    return f"{dataset_id}_{scenario_id}"


def get_fqn(dataset_id: str, scenario_id: str, table_name: str) -> str:
    """Get the FQN for a specific table in a dataset-scenario combination.
    
    MySQL uses 4-part FQN: service.database.schema.table
    where schema = database name (MySQL doesn't have separate schemas)
    """
    service_name = get_service_name(dataset_id, scenario_id)
    database_name = get_database_name(dataset_id, scenario_id)
    # MySQL FQN format: service.database.schema.table (schema = database)
    return f"{service_name}.{database_name}.{database_name}.{table_name}"

SCENARIOS_CONFIG = [
    {
        "id": "clean",
        "name": "No Anomalies",
        "description": "Baseline state with no data quality issues",
        "anomaly_types": [],
        "severity": "LOW"
    },
    {
        "id": "schema_change",
        "name": "Schema Change",
        "description": "A column has been deleted from the table",
        "anomaly_types": ["SCHEMA_CHANGE"],
        "severity": "HIGH"
    },
    {
        "id": "data_quality",
        "name": "Data Quality Failure",
        "description": "Uniqueness constraint violations detected",
        "anomaly_types": ["DATA_QUALITY_FAILURE"],
        "severity": "MEDIUM"
    },
    {
        "id": "volume_anomaly",
        "name": "Volume Anomaly",
        "description": "50% drop in row count detected",
        "anomaly_types": ["VOLUME_ANOMALY"],
        "severity": "HIGH"
    },
    {
        "id": "distribution_drift",
        "name": "Distribution Drift",
        "description": "Null proportion has changed significantly",
        "anomaly_types": ["DISTRIBUTION_DRIFT"],
        "severity": "MEDIUM"
    },
    {
        "id": "stale_data",
        "name": "Stale Data",
        "description": "No profile updates in 48+ hours (requires waiting or SLA modification)",
        "anomaly_types": ["STALE_DATA"],
        "severity": "MEDIUM"
    },
    {
        "id": "pipeline_failure",
        "name": "Pipeline Failure",
        "description": "Failed pipeline in lineage (requires OpenMetadata pipeline setup)",
        "anomaly_types": ["PIPELINE_FAILURE"],
        "severity": "HIGH"
    },
    {
        "id": "multiple",
        "name": "Multiple Anomalies",
        "description": "Schema change + volume drop + distribution drift",
        "anomaly_types": ["SCHEMA_CHANGE", "VOLUME_ANOMALY", "DISTRIBUTION_DRIFT"],
        "severity": "HIGH"
    }
]


@router.get("/datasets", response_model=DatasetsResponse)
async def list_datasets():
    """
    List all available datasets for interactive demo.
    
    Returns information about the 3 pre-configured datasets:
    - E-commerce (customers, orders, revenue)
    - Healthcare (patients, visits, prescriptions)
    - Finance (accounts, transactions, balances)
    """
    datasets = []
    
    for dataset_id, config in DATASETS_CONFIG.items():
        datasets.append(DatasetInfo(
            id=dataset_id,
            name=config["name"],
            description=config["description"],
            service_name=config["service_name"],
            database_name=config["database_name"],
            table_count=len(config["tables"]),
            icon=config["icon"]
        ))
    
    return DatasetsResponse(datasets=datasets)


@router.get("/datasets/{dataset_id}/fqns", response_model=FQNsResponse)
async def list_dataset_fqns(
    dataset_id: str,
    scenario_id: str = "clean",  # Default to clean scenario
    metadata_client: MetadataClientDep = None
):
    """
    List all FQNs (tables) available in a dataset-scenario combination.
    
    Args:
        dataset_id: Dataset identifier (ecommerce, healthcare, finance)
        scenario_id: Scenario identifier (clean, schema_change, etc.)
        metadata_client: OpenMetadata client
    
    Returns:
        List of FQNs with metadata
    """
    if dataset_id not in DATASETS_CONFIG:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dataset not found: {dataset_id}"
        )
    
    config = DATASETS_CONFIG[dataset_id]
    fqns = []
    
    for table in config["tables"]:
        fqn = get_fqn(dataset_id, scenario_id, table["name"])
        
        # Try to get actual row count from OpenMetadata
        actual_row_count = None
        if metadata_client:
            try:
                table_entity = metadata_client.get_table_by_fqn(fqn)
                profile = table_entity.get("profile")
                actual_row_count = profile.get("rowCount") if profile else None
            except:
                pass
        
        fqns.append(FQNInfo(
            fqn=fqn,
            table_name=table["name"],
            description=table["description"],
            row_count=actual_row_count or table["row_count"]
        ))
    
    return FQNsResponse(
        dataset_id=dataset_id,
        fqns=fqns
    )


@router.get("/scenarios", response_model=ScenariosResponse)
async def list_scenarios():
    """
    List all available anomaly scenarios.
    
    Returns information about the 8 pre-configured scenarios:
    - clean: No anomalies
    - schema_change: Column deletion
    - data_quality: Constraint violations
    - volume_anomaly: Row count drop
    - distribution_drift: Null proportion change
    - stale_data: No recent profile updates
    - pipeline_failure: Failed pipeline in lineage
    - multiple: Multiple concurrent anomalies
    """
    scenarios = [
        ScenarioInfo(**scenario)
        for scenario in SCENARIOS_CONFIG
    ]
    
    return ScenariosResponse(scenarios=scenarios)


@router.post("/scenarios/apply")
async def apply_scenario(
    dataset_id: str,
    scenario_id: str
):
    """
    Apply an anomaly scenario to a dataset.
    
    **Note:** This endpoint returns instructions for manual scenario application.
    In production, this would trigger automated scenario management.
    
    Args:
        dataset_id: Dataset identifier (ecommerce, healthcare, finance)
        scenario_id: Scenario identifier (clean, schema_change, etc.)
    
    Returns:
        Instructions for applying the scenario
    """
    if dataset_id not in DATASETS_CONFIG:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dataset not found: {dataset_id}"
        )
    
    scenario = next((s for s in SCENARIOS_CONFIG if s["id"] == scenario_id), None)
    if not scenario:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Scenario not found: {scenario_id}"
        )
    
    # Return instructions for manual application
    # In production, this would trigger the manage_anomaly_scenarios.py script
    return {
        "status": "instructions",
        "message": f"To apply scenario '{scenario_id}' to dataset '{dataset_id}', run:",
        "command": f"uv run python scripts/manage_anomaly_scenarios.py --dataset {dataset_id} --scenario {scenario_id}",
        "note": "After applying the scenario, re-run the profiler to detect changes",
        "profiler_command": f"uv run python scripts/setup_multi_dataset_demo.py --dataset {dataset_id}"
    }
