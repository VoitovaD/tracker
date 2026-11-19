from ninja import NinjaAPI

from internal.api import internal_router
from workout.api import test_router


api = NinjaAPI(
    title="API",
    version="1.0.0",
    docs_url="api/v1/docs",
    openapi_url="api/v1/openapi.json",
    urls_namespace="api",
)
api.add_router("", internal_router)
api.add_router("api/v1", test_router)
