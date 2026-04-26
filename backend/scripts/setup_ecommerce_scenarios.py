"""Setup E-commerce dataset with all 8 scenarios.

This script creates 8 scenario databases for the E-commerce dataset only.
Faster than setting up all 24 databases.

Usage:
    uv run python scripts/setup_ecommerce_scenarios.py
"""
import sys
import time
from pathlib import Path

# Add backend directory to path so we can import from src
sys.path.insert(0, str(Path(__file__).parent.parent))

from setup_all_scenarios import (
    setup_scenario,
    print_header,
    print_success,
    print_error,
    print_info
)

def main():
    """Main entry point."""
    print_header("SETTING UP E-COMMERCE SCENARIOS")
    print_info("This will create 1 dataset × 8 scenarios = 8 databases in OpenMetadata")
    print_info("Estimated time: 5-7 minutes")
    print()
    
    input("Press Enter to continue or Ctrl+C to cancel...")
    print()
    
    start_time = time.time()
    total = 0
    success = 0
    
    dataset_name = "ecommerce"
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
    
    print_header("E-COMMERCE SETUP COMPLETE")
    print_success(f"Successfully set up {success}/{total} scenarios")
    print_info(f"Total time: {elapsed/60:.1f} minutes")
    print()
    print_info("Next steps:")
    print_info("  1. Start backend: cd backend && uv run python -m uvicorn src.main:app --reload")
    print_info("  2. Start frontend: cd frontend && npm run dev")
    print_info("  3. Open browser: http://localhost:5173")
    print_info("  4. Select E-commerce dataset in Interactive Demo Mode!")


if __name__ == "__main__":
    main()
