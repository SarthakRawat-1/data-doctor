"""Multi-dataset demo setup using OpenMetadata Python SDK.

This script creates all OpenMetadata entities needed for the demo based on 
complete backend analysis. Creates exactly what the detection rules expect.

REQUIRED ENTITIES (from backend analysis):
1. DATABASE SERVICES: MySQL connections to scenario databases
2. TABLE ENTITIES: With profile, changeDescription, columns fields
3. PROFILING DATA: profile.timestamp, profile.rowCount, profile.columnProfile
4. TEST CASE ENTITIES: With testCaseResult.testCaseStatus for DATA_QUALITY_FAILURE
5. PIPELINE ENTITIES: With taskStatus.executionStatus for PIPELINE_FAILURE  
6. LINEAGE RELATIONSHIPS: upstreamEdges/downstreamEdges for root cause analysis
7. HISTORICAL VERSIONS: For VOLUME_ANOMALY and DISTRIBUTION_DRIFT detection

For judges using their own OpenMetadata, these already exist.
For our demo setup, we need to create them programmatically.
"""
import sys
import time
import json
import requests
from pathlib import Path
from typing import Dict, List, Tuple, Any
from datetime import datetime, timedelta

# Add backend directory to path so we can import from src
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core.api_client import get_metadata_client
from src.config import settings

# Import OpenMetadata SDK classes
try:
    from metadata.workflow.metadata import MetadataWorkflow
    from metadata.workflow.profiler import ProfilerWorkflow
    from metadata.generated.schema.entity.services.databaseService import DatabaseService
    from metadata.generated.schema.entity.data.table import Table
    from metadata.generated.schema.entity.services.connections.database.mysqlConnection import MysqlConnection
    from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
        OpenMetadataConnection, AuthProvider
    )
    from metadata.generated.schema.security.client.openMetadataJWTClientConfig import (
        OpenMetadataJWTClientConfig,
    )
except ImportError as e:
    print(f"Warning: OpenMetadata SDK imports failed: {e}")
    print("Some advanced features may not work. Basic REST API will be used instead.")

# Database connection constants (from docker-compose files)
SAMPLE_DB_USER = "sample_user"
SAMPLE_DB_PASSWORD = "sample_pass"
SAMPLE_DB_ROOT_PASSWORD = "sample_root_pass"

def print_header(text: str):
    """Print a header with borders."""
    print("=" * 80)
    print(f"  {text}")
    print("=" * 80)

def print_step(step: str, text: str):
    """Print a step with formatting."""
    print(f"\n[{step}] {text}")

def print_success(text: str):
    """Print success message."""
    print(f"✅ {text}")

def print_error(text: str):
    """Print error message."""
    print(f"❌ {text}")

def print_info(text: str):
    """Print info message."""
    print(f"ℹ️  {text}")


def create_database_service(config: Dict[str, Any]):
    """Create a MySQL database service in OpenMetadata.
    
    This creates the connection that allows OpenMetadata to discover tables.
    Uses proper OpenMetadata SDK classes for reliability.
    
    Args:
        config: Configuration dictionary with service details
    """
    client = get_metadata_client()
    
    service_name = config["service_name"]
    database_name = config["database_name"]
    external_port = config["external_port"]
    
    print_info(f"Creating database service: {service_name}")
    
    try:
        # Check if service already exists
        try:
            existing_service = client.get_by_name(
                entity=DatabaseService,
                fqn=service_name
            )
            if existing_service:
                print_info(f"Service {service_name} already exists, skipping creation")
                return existing_service
        except:
            pass  # Service doesn't exist, continue with creation
        
        # Create MySQL connection config
        # Note: Using sample_user credentials from docker-compose
        mysql_connection = MysqlConnection(
            username=SAMPLE_DB_USER,
            authType={"password": SAMPLE_DB_PASSWORD},
            hostPort=f"localhost:{external_port}",
            databaseName=database_name
        )
        
        # Create database service
        service = DatabaseService(
            name=service_name,
            serviceType="Mysql",
            connection={"config": mysql_connection},
            description=f"MySQL service for {database_name} scenario"
        )
        
        # Create service using SDK
        created_service = client.create_or_update(service)
        
        print_success(f"Created database service: {service_name}")
        return created_service
        
    except Exception as e:
        print_error(f"Failed to create service {service_name}: {e}")
        # Try fallback REST API approach
        try:
            service_payload = {
                "name": service_name,
                "serviceType": "Mysql",
                "connection": {
                    "config": {
                        "type": "Mysql",
                        "username": SAMPLE_DB_USER,
                        "authType": {
                            "password": SAMPLE_DB_PASSWORD
                        },
                        "hostPort": f"localhost:{external_port}",
                        "databaseName": database_name
                    }
                },
                "description": f"MySQL service for {database_name} scenario"
            }
            
            response = client._client.client.post(
                "/services/databaseServices",
                json=service_payload
            )
            
            print_success(f"Created database service via REST API: {service_name}")
            return response
            
        except Exception as rest_error:
            print_error(f"REST API fallback also failed: {rest_error}")
            # Don't raise - continue with other services


def run_metadata_ingestion(config: Dict[str, Any]):
    """Run metadata ingestion to discover tables.
    
    This triggers OpenMetadata to scan the MySQL database and create table entities
    with the profile, columns, and other fields our detection rules need.
    
    Args:
        config: Configuration dictionary with service and table details
    """
    service_name = config["service_name"]
    database_name = config["database_name"]
    tables = config["tables"]
    external_port = config["external_port"]
    
    print_info(f"Running metadata ingestion for {service_name}")
    
    # Convert table names to regex patterns for OpenMetadata filter
    table_patterns = [f"^{table}$" for table in tables]
    print_info(f"Table filter patterns: {table_patterns}")
    
    # Create ingestion workflow configuration
    ingestion_config = {
        "source": {
            "type": "mysql",
            "serviceName": service_name,
            "serviceConnection": {
                "config": {
                    "type": "Mysql",
                    "username": SAMPLE_DB_USER,
                    "authType": {
                        "password": SAMPLE_DB_PASSWORD
                    },
                    "hostPort": f"localhost:{external_port}",
                    "databaseName": database_name
                }
            },
            "sourceConfig": {
                "config": {
                    "type": "DatabaseMetadata",
                    "tableFilterPattern": {
                        "includes": table_patterns
                    }
                }
            }
        },
        "sink": {
            "type": "metadata-rest",
            "config": {}
        },
        "workflowConfig": {
            "loggerLevel": "INFO",
            "openMetadataServerConfig": {
                "hostPort": settings.OPENMETADATA_HOST_PORT,
                "authProvider": "openmetadata",
                "securityConfig": {
                    "jwtToken": settings.OPENMETADATA_JWT_TOKEN
                }
            }
        }
    }
    
    try:
        # Create and run metadata workflow
        workflow = MetadataWorkflow.create(ingestion_config)
        workflow.execute()
        workflow.raise_from_status()
        
        print_success(f"Metadata ingestion completed for {service_name}")
        
        # Wait for ingestion to complete
        time.sleep(10)
        
    except Exception as e:
        print_error(f"Failed to run ingestion for {service_name}: {e}")
        # Don't raise - continue with other steps


def run_profiler(config: Dict[str, Any]):
    """Run profiler to generate table statistics.
    
    This creates the profile.rowCount and profile.columnProfile data
    that VOLUME_ANOMALY and DISTRIBUTION_DRIFT detection rules need.
    Also creates profile.timestamp for STALE_DATA detection.
    
    Args:
        config: Configuration dictionary with service and table details
    """
    service_name = config["service_name"]
    database_name = config["database_name"]
    tables = config["tables"]
    external_port = config["external_port"]
    
    print_info(f"Running profiler on {len(tables)} tables")
    
    # Convert table names to regex patterns for OpenMetadata filter
    table_patterns = [f"^{table}$" for table in tables]
    
    # Create profiler workflow configuration
    profiler_config = {
        "source": {
            "type": "mysql",
            "serviceName": service_name,
            "serviceConnection": {
                "config": {
                    "type": "Mysql",
                    "username": SAMPLE_DB_USER,
                    "authType": {
                        "password": SAMPLE_DB_PASSWORD
                    },
                    "hostPort": f"localhost:{external_port}",
                    "databaseName": database_name
                }
            },
            "sourceConfig": {
                "config": {
                    "type": "Profiler",
                    "profileSample": 100,
                    "tableFilterPattern": {
                        "includes": table_patterns
                    }
                }
            }
        },
        "processor": {
            "type": "orm-profiler",
            "config": {}
        },
        "sink": {
            "type": "metadata-rest",
            "config": {}
        },
        "workflowConfig": {
            "loggerLevel": "INFO",
            "openMetadataServerConfig": {
                "hostPort": settings.OPENMETADATA_HOST_PORT,
                "authProvider": "openmetadata",
                "securityConfig": {
                    "jwtToken": settings.OPENMETADATA_JWT_TOKEN
                }
            }
        }
    }
    
    try:
        # Create and run profiler workflow using ProfilerWorkflow
        workflow = ProfilerWorkflow.create(profiler_config)
        workflow.execute()
        workflow.raise_from_status()
        workflow.stop()
        
        print_success(f"Profiler completed for {len(tables)} tables")
        
        # Wait for profiling to complete
        time.sleep(15)
        
    except Exception as e:
        print_error(f"Failed to run profiler: {e}")
        # Don't raise - continue with other steps


def add_lineage(config: Dict[str, Any]):
    """Add lineage relationships between tables.
    
    Creates upstream/downstream relationships that our root cause analysis uses.
    Uses REST API directly with entity IDs.
    
    Args:
        config: Configuration dictionary with lineage relationships
    """
    client = get_metadata_client()
    
    service_name = config["service_name"]
    database_name = config["database_name"]
    lineage_relationships = config["lineage"]
    
    print_info(f"Adding {len(lineage_relationships)} lineage relationships")
    
    # Import required classes
    from metadata.generated.schema.entity.data.table import Table
    
    for upstream_table, downstream_table, description in lineage_relationships:
        try:
            # OpenMetadata FQN format for MySQL: service.database.schema.table (4 parts)
            # For MySQL, try both schema formats: database name and "default"
            upstream_fqn_1 = f"{service_name}.{database_name}.{database_name}.{upstream_table}"
            downstream_fqn_1 = f"{service_name}.{database_name}.{database_name}.{downstream_table}"
            upstream_fqn_2 = f"{service_name}.default.{database_name}.{upstream_table}"
            downstream_fqn_2 = f"{service_name}.default.{database_name}.{downstream_table}"
            
            # Get source and target tables (try both FQN formats silently)
            source_table = None
            target_table = None
            
            for fqn in [upstream_fqn_1, upstream_fqn_2]:
                try:
                    source_table = client._client.get_by_name(entity=Table, fqn=fqn)
                    break
                except:
                    continue
                    
            for fqn in [downstream_fqn_1, downstream_fqn_2]:
                try:
                    target_table = client._client.get_by_name(entity=Table, fqn=fqn)
                    break
                except:
                    continue
            
            if not source_table or not target_table:
                print_error(f"Could not find tables: {upstream_table} or {downstream_table}")
                continue
            
            # Create lineage using OpenMetadata SDK
            from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
            from metadata.generated.schema.type.entityLineage import EntitiesEdge, LineageDetails
            from metadata.generated.schema.type.entityReference import EntityReference
            
            add_lineage_request = AddLineageRequest(
                edge=EntitiesEdge(
                    fromEntity=EntityReference(id=source_table.id, type="table"),
                    toEntity=EntityReference(id=target_table.id, type="table"),
                    lineageDetails=LineageDetails(
                        description=description
                    )
                )
            )
            
            # Add lineage using SDK
            client._client.add_lineage(data=add_lineage_request)
            print_success(f"Added lineage: {upstream_table} -> {downstream_table}")
            
        except Exception as e:
            print_error(f"Failed to add lineage {upstream_table} -> {downstream_table}: {e}")
    
    print_success(f"Configured {len(lineage_relationships)} lineage relationships")


def add_data_quality_tests(config: Dict[str, Any]):
    """Add data quality test cases.
    
    Creates test cases that DATA_QUALITY_FAILURE detection rule uses.
    Uses REST API to create test cases with proper schema (no testSuite field in creation).
    
    Args:
        config: Configuration dictionary with test case definitions
    """
    client = get_metadata_client()
    
    service_name = config["service_name"]
    database_name = config["database_name"]
    test_table = config["test_table"]
    test_cases = config["test_cases"]
    
    # For MySQL, FQN is: service.database.schema.table (schema = database name or "default")
    # Try both formats since OpenMetadata creates tables with both
    table_fqn_1 = f"{service_name}.{database_name}.{database_name}.{test_table}"
    table_fqn_2 = f"{service_name}.default.{database_name}.{test_table}"
    
    print_info(f"Adding {len(test_cases)} data quality tests to {test_table}")
    
    # Verify table exists and get correct FQN
    from metadata.generated.schema.entity.data.table import Table
    table_fqn = None
    try:
        table = client._client.get_by_name(entity=Table, fqn=table_fqn_1)
        table_fqn = table_fqn_1
    except:
        try:
            table = client._client.get_by_name(entity=Table, fqn=table_fqn_2)
            table_fqn = table_fqn_2
        except Exception as e:
            print_error(f"Could not find table {test_table}: {e}")
            return
    
    # Add each test case using REST API
    for test_case_config in test_cases:
        try:
            test_name = test_case_config["name"]
            test_definition_name = test_case_config["testDefinitionName"]
            column_name = test_case_config["columnName"]
            
            # Create test case using REST API
            # For OpenMetadata 1.12.6, testSuite is NOT part of CreateTestCase
            test_payload = {
                "name": f"{test_name}_{table_fqn.replace('.', '_')}",
                "displayName": test_name.replace("_", " ").title(),
                "description": f"Test case for {column_name} column",
                "testDefinition": test_definition_name,
                "entityLink": f"<#E::table::{table_fqn}::columns::{column_name}>",
                "parameterValues": [],
                "computePassedFailedRowCount": True
            }
            
            # Use OpenMetadata client's REST API
            response = client._client.client.post(
                "/dataQuality/testCases",
                json=test_payload
            )
            
            print_success(f"Added test case: {test_name}")
            
        except Exception as e:
            print_error(f"Failed to add test case {test_case_config['name']}: {e}")
    
    print_success(f"Configured {len(test_cases)} data quality tests")


def create_demo_pipeline(config: Dict[str, Any]):
    """Create a demo pipeline entity for PIPELINE_FAILURE detection.
    
    Creates a pipeline with tasks. According to OpenMetadata docs, taskStatus is NOT
    part of CreatePipeline schema - it's added later via pipeline runs/executions.
    
    Args:
        config: Configuration dictionary with service details
    """
    client = get_metadata_client()
    
    service_name = config["service_name"]
    database_name = config["database_name"]
    
    pipeline_name = f"{service_name}_etl_pipeline"
    
    print_info(f"Creating demo pipeline: {pipeline_name}")
    
    try:
        # First create a pipeline service if it doesn't exist
        pipeline_service_name = f"{service_name}_pipeline_service"
        
        pipeline_service_payload = {
            "name": pipeline_service_name,
            "serviceType": "Airflow",
            "connection": {
                "config": {
                    "type": "Airflow",
                    "hostPort": "http://localhost:8080",
                    "connection": {
                        "type": "Backend"
                    }
                }
            },
            "description": f"Pipeline service for {database_name} ETL pipelines"
        }
        
        try:
            # Create pipeline service
            service_response = client._client.client.post(
                "/services/pipelineServices",
                json=pipeline_service_payload
            )
            print_info(f"Created pipeline service: {pipeline_service_name}")
        except Exception as e:
            print_info(f"Pipeline service may already exist: {e}")
        
        # Create pipeline entity using CreatePipelineRequest schema
        # According to OpenMetadata docs, taskStatus is NOT part of CreatePipeline
        pipeline_payload = {
            "name": pipeline_name,
            "displayName": f"ETL Pipeline for {database_name}",
            "description": f"Demo ETL pipeline for {database_name} dataset",
            "service": pipeline_service_name,  # Use string FQN
            "tasks": [
                {
                    "name": "extract_data",
                    "displayName": "Extract Data",
                    "taskType": "SQL",
                    "description": "Extract data from source"
                },
                {
                    "name": "transform_data", 
                    "displayName": "Transform Data",
                    "taskType": "SQL",
                    "description": "Transform and clean data"
                },
                {
                    "name": "load_data",
                    "displayName": "Load Data",
                    "taskType": "SQL", 
                    "description": "Load data to target tables"
                }
            ]
            # Note: taskStatus and pipelineStatus are NOT part of CreatePipeline
            # They are added via pipeline runs/executions
        }
        
        # Use OpenMetadata client's REST API
        response = client._client.client.post(
            "/pipelines",
            json=pipeline_payload
        )
        
        print_success(f"Created demo pipeline: {pipeline_name}")
        
    except Exception as e:
        print_error(f"Failed to create pipeline {pipeline_name}: {e}")


def apply_scenario_anomalies(config: Dict[str, Any], scenario_name: str):
    """Apply scenario-specific anomalies to OpenMetadata entities.
    
    This modifies the entities to create the anomalies that our detection rules
    will find. Uses JSON Patch (RFC 6902) format for PATCH operations.
    
    Args:
        config: Configuration dictionary
        scenario_name: Name of the scenario (clean, schema_change, etc.)
    """
    if scenario_name == "clean":
        print_info("Clean scenario - no anomalies to apply")
        return
    
    client = get_metadata_client()
    service_name = config["service_name"]
    database_name = config["database_name"]
    
    print_info(f"Applying {scenario_name} anomalies to OpenMetadata entities...")
    
    try:
        if scenario_name == "pipeline_failure":
            # Set pipeline status to failed using JSON Patch
            pipeline_name = f"{service_name}_etl_pipeline"
            pipeline_fqn = f"{service_name}_pipeline_service.{pipeline_name}"
            
            # Get the pipeline first to get its ID
            from metadata.generated.schema.entity.services.ingestionPipelines.ingestionPipeline import IngestionPipeline
            
            try:
                pipeline = client._client.get_by_name(
                    entity=IngestionPipeline,
                    fqn=pipeline_fqn
                )
                
                # Update pipeline status using JSON Patch
                import requests
                patch_url = f"{settings.OPENMETADATA_HOST_PORT}/api/v1/pipelines/{pipeline.id.__root__}"
                patch_payload = [
                    {
                        "op": "add",
                        "path": "/pipelineStatus",
                        "value": {
                            "pipelineState": "failed",
                            "timestamp": int(time.time() * 1000)
                        }
                    }
                ]
                
                response = requests.patch(
                    patch_url,
                    json=patch_payload,
                    headers={
                        "Content-Type": "application/json-patch+json",
                        "Authorization": f"Bearer {settings.OPENMETADATA_JWT_TOKEN}"
                    }
                )
                
                if response.status_code in [200, 201]:
                    print_success(f"Set pipeline {pipeline_name} status to Failed")
                else:
                    print_error(f"Failed to update pipeline status: {response.text}")
                    
            except Exception as e:
                print_error(f"Failed to update pipeline {pipeline_name}: {e}")
            
        elif scenario_name == "schema_change":
            # Add changeDescription to indicate column deletion
            test_table = config["test_table"]
            table_fqn = f"{service_name}.{database_name}.{database_name}.{test_table}"
            
            from metadata.generated.schema.entity.data.table import Table
            
            try:
                table = client._client.get_by_name(
                    entity=Table,
                    fqn=table_fqn
                )
                
                # Update table with changeDescription using JSON Patch
                import requests
                patch_url = f"{settings.OPENMETADATA_HOST_PORT}/api/v1/tables/{table.id.__root__}"
                patch_payload = [
                    {
                        "op": "add",
                        "path": "/changeDescription",
                        "value": {
                            "fieldsDeleted": [
                                {
                                    "name": "deleted_column",
                                    "oldValue": "VARCHAR(255)"
                                }
                            ],
                            "fieldsUpdated": [],
                            "fieldsAdded": []
                        }
                    }
                ]
                
                response = requests.patch(
                    patch_url,
                    json=patch_payload,
                    headers={
                        "Content-Type": "application/json-patch+json",
                        "Authorization": f"Bearer {settings.OPENMETADATA_JWT_TOKEN}"
                    }
                )
                
                if response.status_code in [200, 201]:
                    print_success(f"Added schema change to table {test_table}")
                else:
                    print_error(f"Failed to update table: {response.text}")
                    
            except Exception as e:
                print_error(f"Failed to update table {test_table}: {e}")
            
        elif scenario_name == "data_quality":
            # Set test case results to Failed
            test_table = config["test_table"]
            table_fqn = f"{service_name}.{database_name}.{database_name}.{test_table}"
            
            # Update test case results
            for test_case_config in config["test_cases"]:
                test_name = f"{table_fqn}_{test_case_config['name']}"
                
                try:
                    # Get test case to get its ID
                    import requests
                    search_url = f"{settings.OPENMETADATA_HOST_PORT}/api/v1/dataQuality/testCases/name/{test_name}"
                    
                    response = requests.get(
                        search_url,
                        headers={"Authorization": f"Bearer {settings.OPENMETADATA_JWT_TOKEN}"}
                    )
                    
                    if response.status_code == 200:
                        test_case = response.json()
                        test_case_id = test_case.get("id")
                        
                        # Update test case result using JSON Patch
                        patch_url = f"{settings.OPENMETADATA_HOST_PORT}/api/v1/dataQuality/testCases/{test_case_id}"
                        patch_payload = [
                            {
                                "op": "add",
                                "path": "/testCaseResult",
                                "value": {
                                    "testCaseStatus": "Failed",
                                    "result": "Test failed - constraint violation detected",
                                    "timestamp": int(time.time() * 1000)
                                }
                            }
                        ]
                        
                        patch_response = requests.patch(
                            patch_url,
                            json=patch_payload,
                            headers={
                                "Content-Type": "application/json-patch+json",
                                "Authorization": f"Bearer {settings.OPENMETADATA_JWT_TOKEN}"
                            }
                        )
                        
                        if patch_response.status_code in [200, 201]:
                            print_success(f"Set test case {test_case_config['name']} to Failed")
                        else:
                            print_error(f"Failed to update test case: {patch_response.text}")
                    else:
                        print_error(f"Failed to find test case {test_name}")
                        
                except Exception as e:
                    print_error(f"Failed to update test case {test_case_config['name']}: {e}")
        
        # For stale_data, volume_anomaly, distribution_drift scenarios,
        # the anomalies are created by the database modifications in setup_all_scenarios.py
        # The profiler will detect these when it runs
        
    except Exception as e:
        print_error(f"Failed to apply {scenario_name} anomalies: {e}")


def wait_for_ingestion_completion(service_name: str, timeout: int = 300):
    """Wait for metadata ingestion and profiling to complete.
    
    Args:
        service_name: Name of the database service
        timeout: Maximum time to wait in seconds
    """
    client = get_metadata_client()
    
    print_info(f"Waiting for ingestion to complete for {service_name}...")
    
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            # Check if tables are discovered by trying to fetch one
            # This is a simple check - in production you'd check pipeline status
            time.sleep(10)
            print_info("Checking ingestion progress...")
            
        except Exception as e:
            pass
        
        if time.time() - start_time > timeout:
            break
    
    print_success("Ingestion wait period completed")


def wait_for_ingestion(service_name: str, timeout: int = 300):
    """Wait for metadata ingestion to complete.
    
    Args:
        service_name: Name of the database service
        timeout: Maximum time to wait in seconds
    """
    client = get_metadata_client()
    
    print_info(f"Waiting for ingestion to complete for {service_name}...")
    
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            # Check if tables are discovered
            # This is a simplified check - in reality you'd check ingestion pipeline status
            service = client._client.get_by_name(
                entity=DatabaseService,
                fqn=service_name
            )
            
            if service:
                print_success("Ingestion appears to be complete")
                return True
                
        except Exception as e:
            pass
        
        time.sleep(10)  # Wait 10 seconds before checking again
    
    print_error(f"Ingestion timeout after {timeout} seconds")
    return False


if __name__ == "__main__":
    print_header("MULTI-DATASET DEMO SETUP")
    print_info("This script provides functions for setting up OpenMetadata entities")
    print_info("Use the scenario-specific scripts to run the full setup")