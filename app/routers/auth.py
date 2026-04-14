from fastapi import APIRouter, Header

from app.schemas import LoginPayload, RegisterPayload
from app.services.auth_service import delete_session_by_token, login_user, register_user

router = APIRouter(prefix='/auth', tags=['auth'])


@router.post('/register')
def register(payload: RegisterPayload):
    return register_user(payload)


@router.post('/login')
def login(payload: LoginPayload):
    return login_user(payload)


@router.post('/logout')
def logout(authorization: str = Header(default='')):
    if authorization.startswith('Bearer '):
        delete_session_by_token(authorization.replace('Bearer ', ''))
    return {'ok': True}
