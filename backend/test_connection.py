"""Quick test script to verify OpenMetadata connection."""
import sys
from src.core.api_client import OpenMetadataClient
from src.config import settings

def test_connection():
    """Test OpenMetadata connection."""
    print(f"🔍 Testing connection to: {settings.OPENMETADATA_HOST_PORT}")
    print(f"🔑 JWT Token configured: {'Yes' if settings.OPENMETADATA_JWT_TOKEN else 'No'}")
    print()
    
    try:
        client = OpenMetadataClient()
        print("✅ Client initialized")
        
        client.connect()
        print("✅ Connected to OpenMetadata")
        
        is_healthy = client.health_check()
        print(f"✅ Health check: {'Healthy' if is_healthy else 'Unhealthy'}")
        
        print("\n🎉 Connection test successful!")
        return True
        
    except Exception as e:
        print(f"\n❌ Connection test failed: {e}")
        return False

if __name__ == "__main__":
    success = test_connection()
    sys.exit(0 if success else 1)
