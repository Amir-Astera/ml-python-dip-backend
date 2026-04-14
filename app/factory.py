from dotenv import load_dotenv
from fastapi import FastAPI

load_dotenv()
from fastapi.middleware.cors import CORSMiddleware

from app.db import init_db
from app.services.auth_service import ensure_default_user
from app.routers.analytics import router as analytics_router
from app.routers.auth import router as auth_router
from app.routers.budgets import router as budgets_router
from app.routers.dashboard import router as dashboard_router
from app.routers.income_sources import router as income_sources_router
from app.routers.loans import router as loans_router
from app.routers.ml import router as ml_router
from app.routers.reminders import router as reminders_router
from app.routers.transactions import router as transactions_router


def create_app():
    app = FastAPI(title='FinFlow ML API')

    app.add_middleware(
        CORSMiddleware,
        allow_origins=['http://localhost:5173', 'http://127.0.0.1:5173'],
        allow_credentials=True,
        allow_methods=['*'],
        allow_headers=['*'],
    )

    @app.on_event('startup')
    def startup_event():
        init_db()
        ensure_default_user()

    @app.get('/')
    def root():
        return {'message': 'FinFlow ML backend is running'}

    app.include_router(auth_router)
    app.include_router(dashboard_router)
    app.include_router(transactions_router)
    app.include_router(income_sources_router)
    app.include_router(budgets_router)
    app.include_router(analytics_router)
    app.include_router(ml_router)
    app.include_router(loans_router)
    app.include_router(reminders_router)

    return app
