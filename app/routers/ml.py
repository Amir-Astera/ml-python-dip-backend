from fastapi import APIRouter, Depends

from app.dependencies import get_current_user
from app.schemas import MLClassifyPayload
from app.ml.training import ml_retrain_readiness
from app.services.ml_service import classify_expense_payload, interpret_ml_payload, patterns_payload, retrain_ml_payload, user_ml_overview_payload

router = APIRouter(prefix='/ml', tags=['ml'])


@router.get('/overview')
def ml_overview(current_user=Depends(get_current_user)):
    return user_ml_overview_payload(current_user['id'])


@router.post('/classify-expense')
def classify_expense(payload: MLClassifyPayload, current_user=Depends(get_current_user)):
    return classify_expense_payload(payload, user_id=current_user['id'])


@router.get('/patterns')
def ml_patterns(current_user=Depends(get_current_user)):
    return patterns_payload(current_user['id'])


@router.get('/interpret')
def ml_interpret(current_user=Depends(get_current_user)):
    return interpret_ml_payload(current_user['id'])


@router.get('/retrain-info')
def retrain_info(current_user=Depends(get_current_user)):
    return ml_retrain_readiness()


@router.post('/retrain')
def retrain_ml(current_user=Depends(get_current_user)):
    return retrain_ml_payload()
