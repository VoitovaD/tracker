from django.db import connection
from ninja import Router
from ninja.responses import Response


internal_router = Router()

@internal_router.get(
    "/",
    tags=["healthcheck"],
    response={200: str}
)
def get_app_healthcheck(request) -> Response:
    return "OK"


@internal_router.get(
    "/db",
    tags=["healthcheck"],
    response={200: str, 500: str}
)
def get_db_healthcheck(request) -> Response:
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            cursor.fetchone()
    except Exception as e:
        return Response(f"Error to connect DB: {e}", status=500)
    
    return Response(f"Connect to DB", status=200)