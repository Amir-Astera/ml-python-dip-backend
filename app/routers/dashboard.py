from typing import Optional

from fastapi import APIRouter, Depends, Query

from app.dependencies import get_current_user
from app.services.finance_service import get_dashboard_payload

router = APIRouter(tags=['dashboard'])


@router.get('/dashboard/overview')
def dashboard_overview(
    current_user=Depends(get_current_user),
    search: str = Query(default=''),
    category: str = Query(default='all'),
    date_from: Optional[str] = Query(default=None),
    date_to: Optional[str] = Query(default=None),
):
    return get_dashboard_payload(
        current_user['id'],
        search=search,
        category=category,
        date_from=date_from,
        date_to=date_to,
    )
