from fastapi import APIRouter

from app.schemas import LoginPayload, RegisterPayload
from app.services.auth_service import login_user, register_user

router = APIRouter(prefix='/auth', tags=['auth'])


@router.post('/register')
def register(payload: RegisterPayload):
    return register_user(payload)


@router.post('/login')
def login(payload: LoginPayload):
    return login_user(payload)
