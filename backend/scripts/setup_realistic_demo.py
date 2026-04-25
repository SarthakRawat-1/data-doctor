"""Setup realistic demo data for Data Doctor testing.

This script automates the complete setup of sample data in OpenMetadata using
the Python SDK. It supports 3 modes:

1. QUICK (--quick): 2 minutes - Tests 3 detection rules
2. FULL (--full): 5 minutes - Tests all 6 detection rules with compressed history
3. PRODUCTION (--production): 1 hour - Tests all 6 rules with realistic 7-day history

Usage:
    uv run python scripts/setup_realistic_demo.py --quick
    uv run python scripts/setup_realistic_demo.py --full
    uv run python scripts/setup_realistic_demo.py --production
"""
import sys
import time
import yaml
import argparse
from pathlib import Path
from datetime import datetime, timedelta

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from metadata.generated.schema.api.services.createDatabaseService import (
    CreateDatabaseServiceRequest,
)
from metadata.generated.schema.entity.services.connections.database.mysqlConnection import (
    MysqlConnection,
)
from metadata.generated.schema.entity.services.connections.database.common.basicAuth import (
    BasicAuth,
)
from metadata.generated.schema.entity.services.databaseService import (
    DatabaseConnection,
    DatabaseServiceType,
)
from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
from metadata.generated.schema.type.entityLineage import EntEdge, LineageDetails
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.workflow.metadata import MetadataWorkflow
from metadata.workflow.profiler import ProfilerWorkflow
from metadata.workflow.data_quality import TestSuiteWorkflow

from src.core.api_client import get_metadata_client
from src.config import settings

# Configuration
SERVICE_NAME = "sample_mysql_service"
DATABASE_NAME = "ecommerce_sample"
SAMPLE_DB_HOST = "sample_mysql_db"  # Docker container name
SAMPLE_DB_PORT = 3306
SAMPLE_DB_USER = "sample_user"
SAMPLE_DB_PASSWORD = "sample_pass"


def print_header(message: str):
    """Print formatted header."""
    print(f"\n{'='*60}")
    print(f"  {message}")
    print(f"{'='*60}\n")


def print_step(step: int, message: str):
    """Print formatted step."""
    print(f"[Step {step}] {message}")


def print_success(message: str):
    """Print success message."""
    print(f"✅ {message}")


def print_error(message: str):
    """Print error message."""
    print(f"❌ {message}")


def print_info(message: str):
    """Print info message."""
    print(f"ℹ️  {message}")


def step1_create_database_service():
    """Step 1: Create MySQL database service in OpenMetadata."""
    print_step(1, "Creating MySQL database service...")
    
    try:
        client = get_metadata_client()
        
        # Create service request
        service_request = CreateDatabaseServiceRequest(
            name=SERVICE_NAME,
            serviceType=DatabaseServiceType.Mysql,
            connection=DatabaseConnection(
                config=MysqlConnection(
                    username=SAMPLE_DB_USER,
                    authType=BasicAuth(password=SAMPLE_DB_PASSWORD),
                    hostPort=f"{SAMPLE_DB_HOST}:{SAMPLE_DB_PORT}",
                )
            ),
            description="Sample MySQL database for Data Doctor testing"
        )
        
        # Create or update service
        service = client._client.create_or_update(data=service_request)
        print_success(f"Service created: {service.fullyQualifiedName}")
        
        return service
        
    except Exception as e:
        print_error(f"Failed to create service: {e}")
        raise


def step2_run_metadata_ingestion():
    """Step 2: Run metadata ingestion workflow."""
    print_step(2, "Running metadata ingestion workflow...")
    
    try:
        # Create workflow configuration
        config = f"""
source:
  type: mysql
  serviceName: {SERVICE_NAME}
  serviceConnection:
    config:
      type: Mysql
      username: {SAMPLE_DB_USER}
      authType:
        password: {SAMPLE_DB_PASSWORD}
      hostPort: {SAMPLE_DB_HOST}:{SAMPLE_DB_PORT}
  sourceConfig:
    config:
      type: DatabaseMetadata
      databaseFilterPattern:
        includes:
          - {DATABASE_NAME}

sink:
  type: metadata-rest
  config: {{}}

workflowConfig:
  loggerLevel: INFO
  openMetadataServerConfig:
    hostPort: {settings.OPENMETADATA_HOST_PORT}
    authProvider: openmetadata
    securityConfig:
      jwtToken: "{settings.OPENMETADATA_JWT_TOKEN}"
"""
        
        # Parse and run workflow
        workflow_config = yaml.safe_load(config)
        workflow = MetadataWorkflow.create(workflow_config)
        
        print_info("Extracting metadata from sample database...")
        workflow.execute()
        workflow.raise_from_status()
        workflow.print_status()
        workflow.stop()
        
        print_success("Metadata ingestion completed")
        print_info("Ingested tables: dim_customer, fact_orders, fact_revenue")
        
    except Exception as e:
        print_error(f"Metadata ingestion failed: {e}")
        raise


def step3_run_profiler(iteration: int = 1):
    """Step 3: Run profiler workflow to generate table profiles."""
    print_step(3, f"Running profiler workflow (iteration {iteration})...")
    
    try:
        # Create profiler configuration
        config = f"""
source:
  type: mysql
  serviceName: {SERVICE_NAME}
  serviceConnection:
    config:
      type: Mysql
      username: {SAMPLE_DB_USER}
      authType:
        password: {SAMPLE_DB_PASSWORD}
      hostPort: {SAMPLE_DB_HOST}:{SAMPLE_DB_PORT}
  sourceConfig:
    config:
      type: Profiler
      generateSampleData: true
      profileSample: 100
      threadCount: 2
      timeoutSeconds: 300
      databaseFilterPattern:
        includes:
          - {DATABASE_NAME}

processor:
  type: orm-profiler
  config:
    profiler:
      name: default_profiler
      metrics:
        - rowCount
        - columnCount
        - nullCount
        - nullProportion
        - uniqueCount
        - uniqueProportion
        - distinctCount
        - min
        - max
        - mean
        - sum
        - stddev

sink:
  type: metadata-rest
  config: {{}}

workflowConfig:
  loggerLevel: INFO
  openMetadataServerConfig:
    hostPort: {settings.OPENMETADATA_HOST_PORT}
    authProvider: openmetadata
    securityConfig:
      jwtToken: "{settings.OPENMETADATA_JWT_TOKEN}"
"""
        
        # Parse and run workflow
        workflow_config = yaml.safe_load(config)
        workflow = ProfilerWorkflow.create(workflow_config)
        
        print_info("Generating table profiles and column statistics...")
        workflow.execute()
        workflow.raise_from_status()
        workflow.print_status()
        workflow.stop()
        
        print_success(f"Profiler completed (iteration {iteration})")
        
    except Exception as e:
        print_error(f"Profiler failed: {e}")
        raise


def step4_add_lineage():
    """Step 4: Add lineage relationships between tables."""
    print_step(4, "Adding lineage relationships...")
    
    try:
        client = get_metadata_client()
        
        # Get table entities
        dim_customer = client.get_table_by_fqn(f"{SERVICE_NAME}.{DATABASE_NAME}.dim_customer")
        fact_orders = client.get_table_by_fqn(f"{SERVICE_NAME}.{DATABASE_NAME}.fact_orders")
        fact_revenue = client.get_table_by_fqn(f"{SERVICE_NAME}.{DATABASE_NAME}.fact_revenue")
        
        # Add lineage: dim_customer → fact_orders
        print_info("Adding lineage: dim_customer → fact_orders")
        lineage1 = AddLineageRequest(
            edge=EntEdge(
                fromEntity=EntityReference(
                    id=dim_customer['id'],
                    type="table"
                ),
                toEntity=EntityReference(
                    id=fact_orders['id'],
                    type="table"
                ),
                lineageDetails=LineageDetails(
                    description="Customer data flows into orders"
                )
            )
        )
        client._client.add_lineage(lineage1)
        
        # Add lineage: fact_orders → fact_revenue
        print_info("Adding lineage: fact_orders → fact_revenue")
        lineage2 = AddLineageRequest(
            edge=EntEdge(
                fromEntity=EntityReference(
                    id=fact_orders['id'],
                    type="table"
                ),
                toEntity=EntityReference(
                    id=fact_revenue['id'],
                    type="table"
                ),
                lineageDetails=LineageDetails(
                    description="Orders aggregated into revenue"
                )
            )
        )
        client._client.add_lineage(lineage2)
        
        print_success("Lineage relationships added")
        print_info("Data flow: dim_customer → fact_orders → fact_revenue")
        
    except Exception as e:
        print_error(f"Failed to add lineage: {e}")
        raise


def step5_add_data_quality_tests():
    """Step 5: Add data quality tests."""
    print_step(5, "Adding data quality tests...")
    
    try:
        # Create test suite configuration
        config = f"""
source:
  type: TestSuite
  serviceName: {SERVICE_NAME}
  sourceConfig:
    config:
      type: TestSuite
      entityFullyQualifiedName: {SERVICE_NAME}.{DATABASE_NAME}.dim_customer

processor:
  type: orm-test-runner
  config:
    testCases:
      - name: customer_id_not_null
        testDefinitionName: columnValuesToBeNotNull
        columnName: customer_id
        
      - name: email_unique
        testDefinitionName: columnValuesToBeUnique
        columnName: email
        
      - name: email_not_null
        testDefinitionName: columnValuesToBeNotNull
        columnName: email
        
      - name: row_count_check
        testDefinitionName: tableRowCountToBeBetween
        parameterValues:
          - name: minValue
            value: 100
          - name: maxValue
            value: 10000

sink:
  type: metadata-rest
  config: {{}}

workflowConfig:
  loggerLevel: INFO
  openMetadataServerConfig:
    hostPort: {settings.OPENMETADATA_HOST_PORT}
    authProvider: openmetadata
    securityConfig:
      jwtToken: "{settings.OPENMETADATA_JWT_TOKEN}"
"""
        
        # Parse and run workflow
        workflow_config = yaml.safe_load(config)
        workflow = TestSuiteWorkflow.create(workflow_config)
        
        print_info("Creating and running data quality tests...")
        workflow.execute()
        workflow.raise_from_status()
        workflow.print_status()
        workflow.stop()
        
        print_success("Data quality tests added and executed")
        print_info("Tests: customer_id_not_null, email_unique, email_not_null, row_count_check")
        
    except Exception as e:
        print_error(f"Failed to add tests: {e}")
        raise


def quick_mode():
    """Quick mode: 2 minutes - Basic setup with 3 detection rules."""
    print_header("QUICK MODE: Basic Demo Setup (2 minutes)")
    
    print_info("This mode creates:")
    print_info("  ✅ Database service and metadata")
    print_info("  ✅ Table profiles (1 iteration)")
    print_info("  ✅ Lineage relationships")
    print_info("  ✅ Data quality tests")
    print_info("  ✅ Detectable: Schema Change, Data Quality Failure, Pipeline Failure")
    print()
    
    start_time = time.time()
    
    # Execute steps
    step1_create_database_service()
    time.sleep(2)  # Wait for service to be ready
    
    step2_run_metadata_ingestion()
    time.sleep(2)
    
    step3_run_profiler(iteration=1)
    time.sleep(2)
    
    step4_add_lineage()
    time.sleep(1)
    
    step5_add_data_quality_tests()
    
    elapsed = time.time() - start_time
    
    print_header("QUICK MODE COMPLETE")
    print_success(f"Setup completed in {elapsed:.1f} seconds")
    print_info("Ready to test Data Doctor!")
    print()
    print_info("Next steps:")
    print_info("  1. Start backend: uv run python -m uvicorn src.main:app --reload")
    print_info("  2. Test diagnosis: curl -X POST http://localhost:8000/api/v1/diagnose \\")
    print_info(f'       -H "Content-Type: application/json" \\')
    print_info(f'       -d \'{{"target_fqn": "{SERVICE_NAME}.{DATABASE_NAME}.dim_customer"}}\'')
    print()
    print_info("To create anomalies, run:")
    print_info("  uv run python scripts/create_what_if_scenarios.py")


def full_mode():
    """Full mode: 5 minutes - Complete setup with all 6 detection rules."""
    print_header("FULL MODE: Complete Demo Setup (5 minutes)")
    
    print_info("This mode creates:")
    print_info("  ✅ Database service and metadata")
    print_info("  ✅ Table profiles (3 iterations for history)")
    print_info("  ✅ Lineage relationships")
    print_info("  ✅ Data quality tests")
    print_info("  ✅ Detectable: All 6 detection rules")
    print()
    
    start_time = time.time()
    
    # Execute steps
    step1_create_database_service()
    time.sleep(2)
    
    step2_run_metadata_ingestion()
    time.sleep(2)
    
    # Run profiler 3 times to create history
    print_info("Creating historical profiles (3 iterations)...")
    for i in range(1, 4):
        step3_run_profiler(iteration=i)
        if i < 3:
            time.sleep(5)  # Wait between iterations
    
    step4_add_lineage()
    time.sleep(1)
    
    step5_add_data_quality_tests()
    
    elapsed = time.time() - start_time
    
    print_header("FULL MODE COMPLETE")
    print_success(f"Setup completed in {elapsed:.1f} seconds")
    print_info("Ready to test Data Doctor with all 6 detection rules!")
    print()
    print_info("Next steps:")
    print_info("  1. Start backend: uv run python -m uvicorn src.main:app --reload")
    print_info("  2. Create anomalies: uv run python scripts/create_what_if_scenarios.py --all")
    print_info("  3. Test diagnosis: curl -X POST http://localhost:8000/api/v1/diagnose \\")
    print_info(f'       -H "Content-Type: application/json" \\')
    print_info(f'       -d \'{{"target_fqn": "{SERVICE_NAME}.{DATABASE_NAME}.dim_customer"}}\'')


def production_mode():
    """Production mode: 1 hour - Realistic 7-day history."""
    print_header("PRODUCTION MODE: Realistic Setup (1 hour)")
    
    print_info("This mode creates:")
    print_info("  ✅ Database service and metadata")
    print_info("  ✅ Table profiles (7 iterations with delays)")
    print_info("  ✅ Lineage relationships")
    print_info("  ✅ Data quality tests")
    print_info("  ✅ Realistic 7-day history for trend analysis")
    print()
    
    start_time = time.time()
    
    # Execute steps
    step1_create_database_service()
    time.sleep(2)
    
    step2_run_metadata_ingestion()
    time.sleep(2)
    
    # Run profiler 7 times with delays to simulate days
    print_info("Creating realistic 7-day history (this will take ~1 hour)...")
    for i in range(1, 8):
        step3_run_profiler(iteration=i)
        if i < 7:
            print_info(f"Waiting 8 minutes before next iteration (simulating day {i+1})...")
            time.sleep(480)  # 8 minutes between runs
    
    step4_add_lineage()
    time.sleep(1)
    
    step5_add_data_quality_tests()
    
    elapsed = time.time() - start_time
    
    print_header("PRODUCTION MODE COMPLETE")
    print_success(f"Setup completed in {elapsed/60:.1f} minutes")
    print_info("Ready for production-like testing!")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Setup realistic demo data for Data Doctor",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  uv run python scripts/setup_realistic_demo.py --quick        # 2 minutes
  uv run python scripts/setup_realistic_demo.py --full         # 5 minutes
  uv run python scripts/setup_realistic_demo.py --production   # 1 hour
        """
    )
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--quick', action='store_true', help='Quick mode (2 min, 3 detection rules)')
    group.add_argument('--full', action='store_true', help='Full mode (5 min, all 6 detection rules)')
    group.add_argument('--production', action='store_true', help='Production mode (1 hour, realistic history)')
    
    args = parser.parse_args()
    
    try:
        if args.quick:
            quick_mode()
        elif args.full:
            full_mode()
        elif args.production:
            production_mode()
            
    except KeyboardInterrupt:
        print()
        print_error("Setup interrupted by user")
        sys.exit(1)
    except Exception as e:
        print()
        print_error(f"Setup failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
