"""Create 'what-if' scenarios to test Data Doctor detection rules.

This script creates various anomalies in the sample database to demonstrate
all 6 detection rules:

1. Schema Change - Delete a column
2. Data Quality Failure - Create failing test
3. Pipeline Failure - Create failed pipeline
4. Volume Anomaly - Delete 50% of rows
5. Distribution Drift - Change null proportions
6. Stale Data - Modify SLA for testing

Usage:
    uv run python scripts/create_what_if_scenarios.py --schema-change
    uv run python scripts/create_what_if_scenarios.py --data-quality
    uv run python scripts/create_what_if_scenarios.py --volume-anomaly
    uv run python scripts/create_what_if_scenarios.py --all
"""
import sys
import argparse
import mysql.connector
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from src.core.api_client import get_metadata_client

# Configuration
SAMPLE_DB_HOST = "localhost"
SAMPLE_DB_PORT = 3307  # External port
SAMPLE_DB_USER = "sample_user"
SAMPLE_DB_PASSWORD = "sample_pass"
DATABASE_NAME = "ecommerce_sample"
SERVICE_NAME = "sample_mysql_service"


def print_header(message: str):
    """Print formatted header."""
    print(f"\n{'='*60}")
    print(f"  {message}")
    print(f"{'='*60}\n")


def print_success(message: str):
    """Print success message."""
    print(f"✅ {message}")


def print_error(message: str):
    """Print error message."""
    print(f"❌ {message}")


def print_info(message: str):
    """Print info message."""
    print(f"ℹ️  {message}")


def get_db_connection():
    """Get MySQL database connection."""
    return mysql.connector.connect(
        host=SAMPLE_DB_HOST,
        port=SAMPLE_DB_PORT,
        user=SAMPLE_DB_USER,
        password=SAMPLE_DB_PASSWORD,
        database=DATABASE_NAME
    )


def scenario_schema_change():
    """Scenario 1: Schema Change - Delete a column."""
    print_header("Scenario 1: Schema Change")
    
    print_info("This will delete the 'country' column from dim_customer")
    print_info("Detection: SCHEMA_CHANGE")
    print()
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Delete column
        print_info("Executing: ALTER TABLE dim_customer DROP COLUMN country")
        cursor.execute("ALTER TABLE dim_customer DROP COLUMN country")
        conn.commit()
        
        cursor.close()
        conn.close()
        
        print_success("Column deleted successfully")
        print_info("Run profiler again to detect the change:")
        print_info("  cd backend && uv run python scripts/setup_realistic_demo.py --quick")
        print()
        print_info("Then test diagnosis:")
        print_info(f'  curl -X POST http://localhost:8000/api/v1/diagnose -H "Content-Type: application/json" -d \'{{"target_fqn": "{SERVICE_NAME}.{DATABASE_NAME}.dim_customer"}}\'')
        
    except Exception as e:
        print_error(f"Failed to delete column: {e}")
        raise


def scenario_data_quality():
    """Scenario 2: Data Quality Failure - Create failing test."""
    print_header("Scenario 2: Data Quality Failure")
    
    print_info("This will create duplicate emails to fail the uniqueness test")
    print_info("Detection: DATA_QUALITY_FAILURE")
    print()
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Create duplicate emails
        print_info("Creating duplicate emails...")
        cursor.execute("""
            UPDATE dim_customer 
            SET email = 'duplicate@example.com' 
            WHERE customer_id IN (1, 2, 3, 4, 5)
        """)
        conn.commit()
        
        cursor.close()
        conn.close()
        
        print_success("Duplicate emails created")
        print_info("The 'email_unique' test will now fail")
        print()
        print_info("Re-run tests:")
        print_info("  cd backend && uv run python scripts/setup_realistic_demo.py --quick")
        
    except Exception as e:
        print_error(f"Failed to create duplicates: {e}")
        raise


def scenario_volume_anomaly():
    """Scenario 3: Volume Anomaly - Delete 50% of rows."""
    print_header("Scenario 3: Volume Anomaly")
    
    print_info("This will delete 50% of orders to trigger volume anomaly")
    print_info("Detection: VOLUME_ANOMALY")
    print_info("Note: Requires historical profiles (use --full mode)")
    print()
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get current count
        cursor.execute("SELECT COUNT(*) FROM fact_orders")
        before_count = cursor.fetchone()[0]
        print_info(f"Current row count: {before_count}")
        
        # Delete 50%
        print_info("Deleting 50% of rows...")
        cursor.execute("DELETE FROM fact_orders WHERE order_id % 2 = 0")
        conn.commit()
        
        # Get new count
        cursor.execute("SELECT COUNT(*) FROM fact_orders")
        after_count = cursor.fetchone()[0]
        print_info(f"New row count: {after_count}")
        
        cursor.close()
        conn.close()
        
        print_success(f"Deleted {before_count - after_count} rows")
        print_info("Run profiler to detect anomaly:")
        print_info("  cd backend && uv run python scripts/setup_realistic_demo.py --quick")
        
    except Exception as e:
        print_error(f"Failed to delete rows: {e}")
        raise


def scenario_distribution_drift():
    """Scenario 4: Distribution Drift - Change null proportions."""
    print_header("Scenario 4: Distribution Drift")
    
    print_info("This will set 20% of emails to NULL to trigger distribution drift")
    print_info("Detection: DISTRIBUTION_DRIFT")
    print_info("Note: Requires historical profiles (use --full mode)")
    print()
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Set 20% to NULL
        print_info("Setting 20% of emails to NULL...")
        cursor.execute("""
            UPDATE dim_customer 
            SET email = NULL 
            WHERE customer_id % 5 = 0
        """)
        conn.commit()
        
        cursor.close()
        conn.close()
        
        print_success("Null values created")
        print_info("Run profiler to detect drift:")
        print_info("  cd backend && uv run python scripts/setup_realistic_demo.py --quick")
        
    except Exception as e:
        print_error(f"Failed to create nulls: {e}")
        raise


def scenario_stale_data():
    """Scenario 5: Stale Data - Instructions for testing."""
    print_header("Scenario 5: Stale Data")
    
    print_info("Stale data detection requires one of these approaches:")
    print()
    print_info("Option A: Wait 48 hours after last profiler run")
    print_info("  - Most realistic but time-consuming")
    print()
    print_info("Option B: Modify SLA threshold (RECOMMENDED FOR DEMO)")
    print_info("  1. Edit backend/src/constants.py")
    print_info("  2. Change DEFAULT_FRESHNESS_SLA_HOURS from 48 to 1")
    print_info("  3. Wait 1 hour after last profiler run")
    print_info("  4. Test diagnosis")
    print()
    print_info("Option C: Manual timestamp manipulation (advanced)")
    print_info("  - Requires direct database access to OpenMetadata's MySQL")
    print()
    print_success("For demo purposes, use Option B (modify SLA to 1 hour)")


def scenario_pipeline_failure():
    """Scenario 6: Pipeline Failure - Create failed pipeline."""
    print_header("Scenario 6: Pipeline Failure")
    
    print_info("Creating a failed pipeline requires OpenMetadata SDK")
    print_info("This is a manual step for now")
    print()
    print_info("Steps:")
    print_info("  1. Go to OpenMetadata UI (http://localhost:8585)")
    print_info("  2. Navigate to Services → Pipelines")
    print_info("  3. Add a new Pipeline Service (e.g., 'sample_airflow')")
    print_info("  4. Create a pipeline with status 'Failed'")
    print_info("  5. Add lineage: pipeline → dim_customer")
    print()
    print_info("Then test diagnosis on dim_customer")
    print_info("Detection: PIPELINE_FAILURE")


def scenario_disaster():
    """Scenario 7: Multiple concurrent anomalies (The Perfect Storm)."""
    print_header("Scenario 7: The Perfect Storm (Multiple Anomalies)")
    
    print_info("This will create multiple concurrent anomalies:")
    print_info("  1. Schema change (delete column)")
    print_info("  2. Volume anomaly (delete 50% rows)")
    print_info("  3. Distribution drift (20% nulls)")
    print_info("  4. Data quality failure (duplicates)")
    print()
    
    confirm = input("This will significantly modify the database. Continue? (yes/no): ")
    if confirm.lower() != 'yes':
        print_info("Cancelled")
        return
    
    try:
        # Execute all scenarios
        print_info("\n[1/4] Creating schema change...")
        scenario_schema_change()
        
        print_info("\n[2/4] Creating volume anomaly...")
        scenario_volume_anomaly()
        
        print_info("\n[3/4] Creating distribution drift...")
        scenario_distribution_drift()
        
        print_info("\n[4/4] Creating data quality failure...")
        scenario_data_quality()
        
        print_header("DISASTER SCENARIO COMPLETE")
        print_success("Multiple anomalies created successfully")
        print()
        print_info("Run profiler to detect all anomalies:")
        print_info("  cd backend && uv run python scripts/setup_realistic_demo.py --quick")
        print()
        print_info("Then test diagnosis:")
        print_info(f'  curl -X POST http://localhost:8000/api/v1/diagnose -H "Content-Type: application/json" -d \'{{"target_fqn": "{SERVICE_NAME}.{DATABASE_NAME}.dim_customer"}}\'')
        print()
        print_info("Expected results:")
        print_info("  - Primary root cause: SCHEMA_CHANGE")
        print_info("  - Contributing factors: VOLUME_ANOMALY, DISTRIBUTION_DRIFT, DATA_QUALITY_FAILURE")
        print_info("  - Confidence: HIGH (multiple signals)")
        print_info("  - Severity: HIGH (affects downstream tables)")
        
    except Exception as e:
        print_error(f"Disaster scenario failed: {e}")
        raise


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Create what-if scenarios for Data Doctor testing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  uv run python scripts/create_what_if_scenarios.py --schema-change
  uv run python scripts/create_what_if_scenarios.py --data-quality
  uv run python scripts/create_what_if_scenarios.py --volume-anomaly
  uv run python scripts/create_what_if_scenarios.py --all
  uv run python scripts/create_what_if_scenarios.py --disaster
        """
    )
    
    parser.add_argument('--schema-change', action='store_true', help='Create schema change scenario')
    parser.add_argument('--data-quality', action='store_true', help='Create data quality failure scenario')
    parser.add_argument('--volume-anomaly', action='store_true', help='Create volume anomaly scenario')
    parser.add_argument('--distribution-drift', action='store_true', help='Create distribution drift scenario')
    parser.add_argument('--stale-data', action='store_true', help='Show stale data instructions')
    parser.add_argument('--pipeline-failure', action='store_true', help='Show pipeline failure instructions')
    parser.add_argument('--disaster', action='store_true', help='Create multiple concurrent anomalies')
    parser.add_argument('--all', action='store_true', help='Show all scenarios')
    
    args = parser.parse_args()
    
    # If no arguments, show help
    if not any(vars(args).values()):
        parser.print_help()
        return
    
    try:
        if args.schema_change or args.all:
            scenario_schema_change()
        
        if args.data_quality or args.all:
            scenario_data_quality()
        
        if args.volume_anomaly or args.all:
            scenario_volume_anomaly()
        
        if args.distribution_drift or args.all:
            scenario_distribution_drift()
        
        if args.stale_data or args.all:
            scenario_stale_data()
        
        if args.pipeline_failure or args.all:
            scenario_pipeline_failure()
        
        if args.disaster:
            scenario_disaster()
            
    except KeyboardInterrupt:
        print()
        print_error("Interrupted by user")
        sys.exit(1)
    except Exception as e:
        print()
        print_error(f"Failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
