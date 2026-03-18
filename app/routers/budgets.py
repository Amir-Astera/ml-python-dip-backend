from fastapi import APIRouter, Depends

from app.dependencies import get_current_user
from app.schemas import BudgetPayload
from app.services.finance_service import budget_rows, save_budget_payload

router = APIRouter(tags=['budgets'])


@router.get('/budgets')
def get_budgets(current_user=Depends(get_current_user)):
    return {'items': budget_rows(current_user['id'])}


@router.post('/budgets')
def save_budget(payload: BudgetPayload, current_user=Depends(get_current_user)):
    return save_budget_payload(current_user['id'], payload)
