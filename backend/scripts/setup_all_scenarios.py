"""Setup all 24 scenario databases (3 datasets × 8 scenarios).

This script creates all possible combinations of datasets and scenarios
in OpenMetadata, allowing instant switching in the interactive demo.

Usage:
    uv run python scripts/setup_all_scenarios.py
"""
import sys
import time
import mysql.connector
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# Import the multi-dataset setup functions
from setup_multi_dataset_demo import (
    create_database_service,
    run_metadata_ingestion,
    run_profiler,
    add_lineage,
    add_data_quality_tests,
    create_demo_pipeline,
    apply_scenario_anomalies,
    wait_for_ingestion_completion,
    print_header,
    print_step,
    print_success,
    print_error,
    print_info,
    SAMPLE_DB_USER,
    SAMPLE_DB_PASSWORD
)

# Scenario configurations with port mappings
SCENARIO_CONFIGS = {
    "ecommerce": {
        "clean": {"port": 3310, "host": "ecommerce_clean", "database": "ecommerce_clean"},
        "schema_change": {"port": 3311, "host": "ecommerce_schema_change", "database": "ecommerce_schema_change"},
        "data_quality": {"port": 3312, "host": "ecommerce_data_quality", "database": "ecommerce_data_quality"},
        "volume_anomaly": {"port": 3313, "host": "ecommerce_volume_anomaly", "database": "ecommerce_volume_anomaly"},
        "distribution_drift": {"port": 3314, "host": "ecommerce_distribution_drift", "database": "ecommerce_distribution_drift"},
        "stale_data": {"port": 3315, "host": "ecommerce_stale_data", "database": "ecommerce_stale_data"},
        "pipeline_failure": {"port": 3316, "host": "ecommerce_pipeline_failure", "database": "ecommerce_pipeline_failure"},
        "multiple": {"port": 3317, "host": "ecommerce_multiple", "database": "ecommerce_multiple"},
    },
    "healthcare": {
        "clean": {"port": 3320, "host": "healthcare_clean", "database": "healthcare_clean"},
        "schema_change": {"port": 3321, "host": "healthcare_schema_change", "database": "healthcare_schema_change"},
        "data_quality": {"port": 3322, "host": "healthcare_data_quality", "database": "healthcare_data_quality"},
        "volume_anomaly": {"port": 3323, "host": "healthcare_volume_anomaly", "database": "healthcare_volume_anomaly"},
        "distribution_drift": {"port": 3324, "host": "healthcare_distribution_drift", "database": "healthcare_distribution_drift"},
        "stale_data": {"port": 3325, "host": "healthcare_stale_data", "database": "healthcare_stale_data"},
        "pipeline_failure": {"port": 3326, "host": "healthcare_pipeline_failure", "database": "healthcare_pipeline_failure"},
        "multiple": {"port": 3327, "host": "healthcare_multiple", "database": "healthcare_multiple"},
    },
    "finance": {
        "clean": {"port": 3330, "host": "finance_clean", "database": "finance_clean"},
        "schema_change": {"port": 3331, "host": "finance_schema_change", "database": "finance_schema_change"},
        "data_quality": {"port": 3332, "host": "finance_data_quality", "database": "finance_data_quality"},
        "volume_anomaly": {"port": 3333, "host": "finance_volume_anomaly", "database": "finance_volume_anomaly"},
        "distribution_drift": {"port": 3334, "host": "finance_distribution_drift", "database": "finance_distribution_drift"},
        "stale_data": {"port": 3335, "host": "finance_stale_data", "database": "finance_stale_data"},
        "pipeline_failure": {"port": 3336, "host": "finance_pipeline_failure", "database": "finance_pipeline_failure"},
        "multiple": {"port": 3337, "host": "finance_multiple", "database": "finance_multiple"},
    }
}

# Dataset table configurations
DATASET_TABLES = {
    "ecommerce": {
        "tables": ["dim_customer", "fact_orders", "fact_revenue"],
        "lineage": [
            ("dim_customer", "fact_orders", "Customer data flows into orders"),
            ("fact_orders", "fact_revenue", "Orders aggregated into revenue")
        ],
        "test_table": "dim_customer",
        "test_cases": [
            {"name": "customer_id_not_null", "testDefinitionName": "columnValuesToBeNotNull", "columnName": "customer_id"},
            {"name": "email_unique", "testDefinitionName": "columnValuesToBeUnique", "columnName": "email"},
            {"name": "email_not_null", "testDefinitionName": "columnValuesToBeNotNull", "columnName": "email"}
        ]
    },
    "healthcare": {
        "tables": ["dim_patient", "fact_visit", "fact_prescription", "fact_daily_metrics"],
        "lineage": [
            ("dim_patient", "fact_visit", "Patient data flows into visits"),
            ("fact_visit", "fact_prescription", "Visits generate prescriptions"),
            ("fact_visit", "fact_daily_metrics", "Visits aggregated into daily metrics")
        ],
        "test_table": "dim_patient",
        "test_cases": [
            {"name": "patient_id_not_null", "testDefinitionName": "columnValuesToBeNotNull", "columnName": "patient_id"},
            {"name": "medical_record_number_unique", "testDefinitionName": "columnValuesToBeUnique", "columnName": "medical_record_number"},
            {"name": "email_not_null", "testDefinitionName": "columnValuesToBeNotNull", "columnName": "email"}
        ]
    },
    "finance": {
        "tables": ["dim_account", "fact_transaction", "fact_balance_snapshot", "fact_daily_summary"],
        "lineage": [
            ("dim_account", "fact_transaction", "Account data flows into transactions"),
            ("dim_account", "fact_balance_snapshot", "Account balances tracked over time"),
            ("fact_transaction", "fact_daily_summary", "Transactions aggregated into daily summary")
        ],
        "test_table": "dim_account",
        "test_cases": [
            {"name": "account_id_not_null", "testDefinitionName": "columnValuesToBeNotNull", "columnName": "account_id"},
            {"name": "account_number_unique", "testDefinitionName": "columnValuesToBeUnique", "columnName": "account_number"},
            {"name": "account_status_not_null", "testDefinitionName": "columnValuesToBeNotNull", "columnName": "account_status"}
        ]
    }
}


def apply_scenario_to_database(dataset_name: str, scenario_name: str, port: int):
    """Apply anomaly scenario directly to database."""
    if scenario_name == "clean":
        print_info("Clean scenario - no modifications needed")
        return
    
    print_info(f"Applying {scenario_name} scenario to database...")
    
    try:
        conn = mysql.connector.connect(
            host="localhost",
            port=port,
            user=SAMPLE_DB_USER,
            password=SAMPLE_DB_PASSWORD,
            database=SCENARIO_CONFIGS[dataset_name][scenario_name]["database"]
        )
        cursor = conn.cursor()
        
        # Apply scenario-specific SQL
        if scenario_name == "schema_change":
            if dataset_name == "ecommerce":
                cursor.execute("ALTER TABLE dim_customer DROP COLUMN IF EXISTS country")
            elif dataset_name == "healthcare":
                cursor.execute("ALTER TABLE dim_patient DROP COLUMN IF EXISTS blood_type")
            elif dataset_name == "finance":
                cursor.execute("ALTER TABLE dim_account DROP COLUMN IF EXISTS credit_score")
                
        elif scenario_name == "data_quality":
            if dataset_name == "ecommerce":
                cursor.execute("UPDATE dim_customer SET email = 'duplicate@example.com' WHERE customer_id IN (1, 2, 3, 4, 5)")
            elif dataset_name == "healthcare":
                cursor.execute("UPDATE dim_patient SET medical_record_number = 'MRN00000001' WHERE patient_id IN (1, 2, 3, 4, 5)")
            elif dataset_name == "finance":
                cursor.execute("UPDATE dim_account SET account_number = 'ACC0000000001' WHERE account_id IN (1, 2, 3, 4, 5)")
                
        elif scenario_name == "volume_anomaly":
            if dataset_name == "ecommerce":
                cursor.execute("DELETE FROM fact_orders WHERE order_id % 2 = 0")
            elif dataset_name == "healthcare":
                cursor.execute("DELETE FROM fact_visit WHERE visit_id % 2 = 0")
            elif dataset_name == "finance":
                cursor.execute("DELETE FROM fact_transaction WHERE transaction_id % 2 = 0")
                
        elif scenario_name == "distribution_drift":
            if dataset_name == "ecommerce":
                cursor.execute("UPDATE dim_customer SET email = NULL WHERE customer_id % 5 = 0")
            elif dataset_name == "healthcare":
                cursor.execute("UPDATE dim_patient SET email = NULL WHERE patient_id % 5 = 0")
            elif dataset_name == "finance":
                cursor.execute("UPDATE dim_account SET email = NULL WHERE account_id % 5 = 0")
                
        elif scenario_name == "multiple":
            # Apply schema_change + volume_anomaly + distribution_drift
            if dataset_name == "ecommerce":
                cursor.execute("ALTER TABLE dim_customer DROP COLUMN IF EXISTS country")
                cursor.execute("DELETE FROM fact_orders WHERE order_id % 2 = 0")
                cursor.execute("UPDATE dim_customer SET email = NULL WHERE customer_id % 5 = 0")
            elif dataset_name == "healthcare":
                cursor.execute("ALTER TABLE dim_patient DROP COLUMN IF EXISTS blood_type")
                cursor.execute("DELETE FROM fact_visit WHERE visit_id % 2 = 0")
                cursor.execute("UPDATE dim_patient SET email = NULL WHERE patient_id % 5 = 0")
            elif dataset_name == "finance":
                cursor.execute("ALTER TABLE dim_account DROP COLUMN IF EXISTS credit_score")
                cursor.execute("DELETE FROM fact_transaction WHERE transaction_id % 2 = 0")
                cursor.execute("UPDATE dim_account SET email = NULL WHERE account_id % 5 = 0")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print_success(f"Scenario {scenario_name} applied to database")
        
    except Exception as e:
        print_error(f"Failed to apply scenario: {e}")
        # Don't raise - continue with other scenarios


def setup_scenario(dataset_name: str, scenario_name: str):
    """Setup a single dataset-scenario combination."""
    scenario_config = SCENARIO_CONFIGS[dataset_name][scenario_name]
    dataset_config = DATASET_TABLES[dataset_name]
    
    service_name = f"sample_mysql_{dataset_name}_{scenario_name}"
    
    print_header(f"Setting up {dataset_name.upper()} - {scenario_name.upper()}")
    
    config = {
        "service_name": service_name,
        "database_name": scenario_config["database"],
        "host": scenario_config["host"],
        "port": 3306,  # Internal Docker port
        "external_port": scenario_config["port"],
        "tables": dataset_config["tables"],
        "lineage": dataset_config["lineage"],
        "test_table": dataset_config["test_table"],
        "test_cases": dataset_config["test_cases"]
    }
    
    try:
        # Step 1: Create service
        print_step("1/6", "Creating database service...")
        create_database_service(config)
        time.sleep(2)
        
        # Step 2: Apply scenario to database BEFORE ingestion
        print_step("2/6", "Applying scenario to database...")
        apply_scenario_to_database(dataset_name, scenario_name, scenario_config["port"])
        time.sleep(2)
        
        # Step 3: Run metadata ingestion
        print_step("3/6", "Running metadata ingestion...")
        run_metadata_ingestion(config)
        time.sleep(2)
        
        # Step 4: Run profiler
        print_step("4/6", "Running profiler...")
        run_profiler(config)
        time.sleep(2)
        
        # Step 5: Add lineage
        print_step("5/6", "Adding lineage...")
        add_lineage(config)
        time.sleep(1)
        
        # Step 6: Create demo pipeline (for PIPELINE_FAILURE detection)
        print_step("6/8", "Creating demo pipeline...")
        create_demo_pipeline(config)
        time.sleep(2)
        
        # Step 7: Apply scenario-specific anomalies to OpenMetadata
        print_step("7/8", "Applying scenario anomalies to OpenMetadata...")
        apply_scenario_anomalies(config, scenario_name)
        time.sleep(2)
        
        # Step 8: Add tests
        print_step("8/8", "Adding data quality tests...")
        add_data_quality_tests(config)
        
        print_success(f"✅ {dataset_name}/{scenario_name} complete!")
        print()
        
    except Exception as e:
        print_error(f"Failed to setup {dataset_name}/{scenario_name}: {e}")
        import traceback
        traceback.print_exc()


def main():
    """Main entry point."""
    print_header("SETTING UP ALL 24 SCENARIO DATABASES")
    print_info("This will create 3 datasets × 8 scenarios = 24 databases in OpenMetadata")
    print_info("Estimated time: 20-25 minutes")
    print()
    
    input("Press Enter to continue or Ctrl+C to cancel...")
    print()
    
    start_time = time.time()
    total = 0
    success = 0
    
    for dataset_name in ["ecommerce", "healthcare", "finance"]:
        for scenario_name in ["clean", "schema_change", "data_quality", "volume_anomaly", 
                             "distribution_drift", "stale_data", "pipeline_failure", "multiple"]:
            total += 1
            try:
                setup_scenario(dataset_name, scenario_name)
                success += 1
            except KeyboardInterrupt:
                print()
                print_error("Setup interrupted by user")
                sys.exit(1)
            except Exception as e:
                print_error(f"Failed: {e}")
                # Continue with next scenario
            
            time.sleep(2)  # Brief pause between scenarios
    
    elapsed = time.time() - start_time
    
    print_header("SETUP COMPLETE")
    print_success(f"Successfully set up {success}/{total} scenarios")
    print_info(f"Total time: {elapsed/60:.1f} minutes")
    print()
    print_info("Next steps:")
    print_info("  1. Start backend: cd backend && uv run python -m uvicorn src.main:app --reload")
    print_info("  2. Start frontend: cd frontend && npm run dev")
    print_info("  3. Open browser: http://localhost:5173")
    print_info("  4. Use Interactive Demo Mode to switch between scenarios instantly!")


if __name__ == "__main__":
    main()
