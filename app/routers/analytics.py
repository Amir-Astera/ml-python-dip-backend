from fastapi import APIRouter, Depends

from app.dependencies import get_current_user
from app.services.finance_service import analytics_payload

router = APIRouter(tags=['analytics'])


@router.get('/analytics/overview')
def analytics_overview(current_user=Depends(get_current_user)):
    return analytics_payload(current_user['id'])
