from fastapi import APIRouter, Depends, Query

from app.dependencies import get_current_user
from app.schemas import TransactionPayload
from app.services.finance_service import create_transaction_payload, delete_transaction_payload, get_transactions_payload

router = APIRouter(tags=['transactions'])


@router.get('/transactions')
def get_transactions(
    search: str = Query(default=''),
    category: str = Query(default='all'),
    current_user=Depends(get_current_user),
):
    return get_transactions_payload(current_user['id'], search=search, category=category)


@router.post('/transactions')
def create_transaction(payload: TransactionPayload, current_user=Depends(get_current_user)):
    return create_transaction_payload(current_user['id'], payload)


@router.delete('/transactions/{transaction_id}')
def delete_transaction(transaction_id: int, current_user=Depends(get_current_user)):
    return delete_transaction_payload(current_user['id'], transaction_id)
