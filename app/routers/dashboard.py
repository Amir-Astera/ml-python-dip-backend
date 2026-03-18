from fastapi import APIRouter, Depends

from app.dependencies import get_current_user
from app.services.finance_service import get_dashboard_payload

router = APIRouter(tags=['dashboard'])


@router.get('/dashboard/overview')
def dashboard_overview(current_user=Depends(get_current_user)):
    return get_dashboard_payload(current_user['id'])
