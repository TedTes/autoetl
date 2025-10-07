"""API routes"""

from api.routes.building_permits import router as building_permits_router,download_router

__all__ = ["building_permits_router","download_router"]