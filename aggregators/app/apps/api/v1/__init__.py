from fastapi import APIRouter

VERSION_PREFIX = "/v1"

first_version_router = APIRouter(prefix=VERSION_PREFIX)