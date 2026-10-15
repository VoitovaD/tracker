from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.apps.api import main_router
from app.config import app_settings


def setup_middlewares(application: FastAPI) -> None:
    application.add_middleware(
        CORSMiddleware,
        allow_origins=app_settings.allow_origins_list,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )


def setup_routers(application: FastAPI) -> None:
    application.include_router(main_router)


def get_app(app_name: str) -> FastAPI:
    application = FastAPI(
        title=app_name,
        root_path=app_settings.ROOT_PATH,
        debug=app_settings.DEBUG,
    )
    setup_middlewares(application)
    setup_routers(application)
    return application


app = get_app(app_name=app_settings.SERVICE_NAME)