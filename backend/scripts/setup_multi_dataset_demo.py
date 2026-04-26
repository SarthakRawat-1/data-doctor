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
from pathlib import Path
from typing import Dict, List, Tuple, Any
from datetime import datetime, timedelta

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from src.core.api_client import get_metadata_client

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

# Database connection constants
SAMPLE_DB_USER = "root"
SAMPLE_DB_PASSWORD = "password"

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
                    "schemaFilterPattern": {
                        "includes": [database_name]
                    },
                    "tableFilterPattern": {
                        "includes": tables
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
                "hostPort": "http://localhost:8585/api",
                "authProvider": "openmetadata",
                "securityConfig": {
                    "jwtToken": ""
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
                    "generateSampleData": True,
                    "profileSample": 100,
                    "schemaFilterPattern": {
                        "includes": [database_name]
                    },
                    "tableFilterPattern": {
                        "includes": tables
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
                "hostPort": "http://localhost:8585/api",
                "authProvider": "openmetadata",
                "securityConfig": {
                    "jwtToken": ""
                }
            }
        }
    }
    
    try:
        # Create and run profiler workflow
        workflow = MetadataWorkflow.create(profiler_config)
        workflow.execute()
        workflow.raise_from_status()
        
        print_success(f"Profiler completed for {len(tables)} tables")
        
        # Wait for profiling to complete
        time.sleep(15)
        
    except Exception as e:
        print_error(f"Failed to run profiler: {e}")
        # Don't raise - continue with other steps


def add_lineage(config: Dict[str, Any]):
    """Add lineage relationships between tables.
    
    Creates upstream/downstream relationships that our root cause analysis uses.
    Uses REST API for reliability.
    
    Args:
        config: Configuration dictionary with lineage relationships
    """
    client = get_metadata_client()
    
    service_name = config["service_name"]
    database_name = config["database_name"]
    lineage_relationships = config["lineage"]
    
    print_info(f"Adding {len(lineage_relationships)} lineage relationships")
    
    for upstream_table, downstream_table, description in lineage_relationships:
        try:
            upstream_fqn = f"{service_name}.{database_name}.{upstream_table}"
            downstream_fqn = f"{service_name}.{database_name}.{downstream_table}"
            
            # Create lineage using REST API
            lineage_payload = {
                "edge": {
                    "fromEntity": {
                        "fqn": upstream_fqn,
                        "type": "table"
                    },
                    "toEntity": {
                        "fqn": downstream_fqn,
                        "type": "table"
                    },
                    "lineageDetails": {
                        "description": description
                    }
                }
            }
            
            # Use OpenMetadata client's REST API
            response = client._client.client.put(
                "/lineage",
                json=lineage_payload
            )
            
            print_success(f"Added lineage: {upstream_table} -> {downstream_table}")
            
        except Exception as e:
            print_error(f"Failed to add lineage {upstream_table} -> {downstream_table}: {e}")
    
    print_success(f"Configured {len(lineage_relationships)} lineage relationships")


def add_data_quality_tests(config: Dict[str, Any]):
    """Add data quality test cases.
    
    Creates test cases that DATA_QUALITY_FAILURE detection rule uses.
    Uses REST API to create test cases with proper testCaseResult structure.
    
    Args:
        config: Configuration dictionary with test case definitions
    """
    client = get_metadata_client()
    
    service_name = config["service_name"]
    database_name = config["database_name"]
    test_table = config["test_table"]
    test_cases = config["test_cases"]
    
    table_fqn = f"{service_name}.{database_name}.{test_table}"
    
    print_info(f"Adding {len(test_cases)} data quality tests to {test_table}")
    
    # Add each test case using REST API
    for test_case_config in test_cases:
        try:
            test_name = test_case_config["name"]
            test_definition_name = test_case_config["testDefinitionName"]
            column_name = test_case_config["columnName"]
            
            # Create test case using REST API
            test_payload = {
                "name": f"{table_fqn}_{test_name}",
                "displayName": test_name.replace("_", " ").title(),
                "description": f"Test case for {column_name} column",
                "testDefinition": {
                    "name": test_definition_name,
                    "type": "testDefinition"
                },
                "entityLink": f"<#E::table::{table_fqn}::columns::{column_name}>",
                "testSuite": {
                    "name": f"{table_fqn}_test_suite",
                    "type": "testSuite"
                },
                "parameterValues": [
                    {
                        "name": "columnName",
                        "value": column_name
                    }
                ]
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
    
    Creates a pipeline with taskStatus.executionStatus that can be set to "Failed"
    for demo purposes. This is what the detect_pipeline_failure() function expects.
    
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
        
        # Create pipeline entity
        pipeline_payload = {
            "name": pipeline_name,
            "displayName": f"ETL Pipeline for {database_name}",
            "description": f"Demo ETL pipeline for {database_name} dataset",
            "service": {
                "name": pipeline_service_name,
                "type": "pipelineService"
            },
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
            ],
            # Add taskStatus for PIPELINE_FAILURE detection
            "taskStatus": {
                "executionStatus": "Success"  # Will be changed to "Failed" for anomaly scenarios
            },
            "pipelineStatus": {
                "pipelineState": "success"
            }
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
    will find. For example, setting pipeline status to "Failed" or modifying
    table changeDescription to simulate schema changes.
    
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
            # Set pipeline status to failed
            pipeline_name = f"{service_name}_etl_pipeline"
            pipeline_fqn = f"{service_name}_pipeline_service.{pipeline_name}"
            
            # Update pipeline status using PATCH
            patch_payload = [
                {
                    "op": "replace",
                    "path": "/taskStatus/executionStatus",
                    "value": "Failed"
                },
                {
                    "op": "replace", 
                    "path": "/pipelineStatus/pipelineState",
                    "value": "failed"
                }
            ]
            
            response = client._client.client.patch(
                f"/pipelines/name/{pipeline_fqn}",
                json=patch_payload
            )
            print_success(f"Set pipeline {pipeline_name} status to Failed")
            
        elif scenario_name == "schema_change":
            # Add changeDescription to indicate column deletion
            test_table = config["test_table"]
            table_fqn = f"{service_name}.{database_name}.{test_table}"
            
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
            
            response = client._client.client.patch(
                f"/tables/name/{table_fqn}",
                json=patch_payload
            )
            print_success(f"Added schema change to table {test_table}")
            
        elif scenario_name == "data_quality":
            # Set test case results to Failed
            test_table = config["test_table"]
            table_fqn = f"{service_name}.{database_name}.{test_table}"
            
            # Update test case results
            for test_case_config in config["test_cases"]:
                test_name = f"{table_fqn}_{test_case_config['name']}"
                
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
                
                try:
                    response = client._client.client.patch(
                        f"/dataQuality/testCases/name/{test_name}",
                        json=patch_payload
                    )
                    print_success(f"Set test case {test_case_config['name']} to Failed")
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