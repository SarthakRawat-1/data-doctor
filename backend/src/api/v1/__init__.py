"""API v1 routes."""
from fastapi import APIRouter

from .diagnosis import router as diagnosis_router
from .health import router as health_router
from .demo import router as demo_router

# Create v1 router
router = APIRouter()

# Include sub-routers
router.include_router(health_router, tags=["health"])
router.include_router(diagnosis_router, tags=["diagnosis"])
router.include_router(demo_router, prefix="/demo", tags=["demo"])
