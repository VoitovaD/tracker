from fastapi import APIRouter

from .ping import router as ping_router
from .v1 import first_version_router

API_PREFIX = "/api"

main_router = APIRouter()

main_router.include_router(ping_router)
main_router.include_router(first_version_router, prefix=API_PREFIX)