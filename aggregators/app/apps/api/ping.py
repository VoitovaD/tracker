import logging

from fastapi import APIRouter


logger = logging.getLogger(__name__)
router = APIRouter()


# @router.get("/healthz", include_in_schema=False)
@router.get("/healthz")
async def readiness_probe() -> str:
    return "OK"