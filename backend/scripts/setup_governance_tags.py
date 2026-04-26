"""Setup script for creating governance tags in OpenMetadata.

This script creates the DataQuality classification and its tags:
- Critical: Critical data quality issue - DO NOT USE
- Warning: Data quality warning - use with caution
- UnderInvestigation: Asset under investigation
- RootCause: Identified as root cause of incident
- Affected: Affected by upstream incident

Run this once before using governance tagging feature.

Usage:
    uv run python scripts/setup_governance_tags.py
"""
import sys
from pathlib import Path

# Add backend directory to path so we can import from src
sys.path.insert(0, str(Path(__file__).parent.parent))

from metadata.generated.schema.api.classification.createClassification import (
    CreateClassificationRequest,
)
from metadata.generated.schema.api.classification.createTag import CreateTagRequest

from src.core.api_client import get_metadata_client


def setup_governance_tags():
    """Create DataQuality classification and tags in OpenMetadata."""
    print("🏷️  Setting up governance tags in OpenMetadata...")
    
    try:
        # Get metadata client
        client = get_metadata_client()
        print(f"✅ Connected to OpenMetadata at {client._client.config.hostPort}")
        
        # Create classification
        print("\n📋 Creating DataQuality classification...")
        classification = CreateClassificationRequest(
            name="DataQuality",
            description="Data quality governance tags for marking unreliable assets"
        )
        
        try:
            client._client.create_or_update(classification)
            print("✅ DataQuality classification created")
        except Exception as e:
            if "already exists" in str(e).lower():
                print("ℹ️  DataQuality classification already exists")
            else:
                raise
        
        # Create tags
        tags = [
            ("Critical", "🔴 Critical data quality issue - DO NOT USE", "#FF0000"),
            ("Warning", "⚠️  Data quality warning - use with caution", "#FFA500"),
            ("UnderInvestigation", "🔍 Asset under investigation", "#FFFF00"),
            ("RootCause", "🎯 Identified as root cause of incident", "#FF00FF"),
            ("Affected", "📉 Affected by upstream incident", "#0000FF")
        ]
        
        print("\n🏷️  Creating tags...")
        for tag_name, description, color in tags:
            try:
                tag = CreateTagRequest(
                    classification="DataQuality",
                    name=tag_name,
                    description=description,
                    style={
                        "color": color,
                        "iconURL": ""
                    }
                )
                client._client.create_or_update(tag)
                print(f"  ✅ {tag_name}")
            except Exception as e:
                if "already exists" in str(e).lower():
                    print(f"  ℹ️  {tag_name} (already exists)")
                else:
                    print(f"  ❌ {tag_name}: {e}")
        
        print("\n✅ Governance tags setup complete!")
        print("\nAvailable tags:")
        print("  - DataQuality.Critical")
        print("  - DataQuality.Warning")
        print("  - DataQuality.UnderInvestigation")
        print("  - DataQuality.RootCause")
        print("  - DataQuality.Affected")
        print("\nYou can now enable governance tagging in your .env:")
        print("  ENABLE_GOVERNANCE_TAGGING=true")
        
    except Exception as e:
        print(f"\n❌ Setup failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    setup_governance_tags()
