from fastapi import APIRouter, Depends

from app.dependencies import get_current_user
from app.schemas import MLClassifyPayload
from app.services.ml_service import classify_expense_payload, retrain_ml_payload, user_ml_overview_payload

router = APIRouter(prefix='/ml', tags=['ml'])


@router.get('/overview')
def ml_overview(current_user=Depends(get_current_user)):
    return user_ml_overview_payload(current_user['id'])


@router.post('/classify-expense')
def classify_expense(payload: MLClassifyPayload, current_user=Depends(get_current_user)):
    return classify_expense_payload(payload)


@router.post('/retrain')
def retrain_ml(current_user=Depends(get_current_user)):
    return retrain_ml_payload()
