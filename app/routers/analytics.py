from typing import Optional

from fastapi import APIRouter, Depends, Query

from app.dependencies import get_current_user
from app.services.finance_service import analytics_payload

router = APIRouter(tags=['analytics'])


@router.get('/analytics/overview')
def analytics_overview(
    current_user=Depends(get_current_user),
    date_from: Optional[str] = Query(default=None, description="YYYY-MM-DD"),
    date_to: Optional[str] = Query(default=None, description="YYYY-MM-DD"),
):
    return analytics_payload(current_user['id'], date_from=date_from, date_to=date_to)
