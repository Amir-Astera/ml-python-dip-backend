from fastapi import Header, HTTPException

from app.services.auth_service import get_user_by_email, tokens_db


def get_current_user(authorization: str = Header(default="")):
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Требуется авторизация")

    token = authorization.replace("Bearer ", "")
    email = tokens_db.get(token)
    if not email:
        raise HTTPException(status_code=401, detail="Сессия не найдена")

    user = get_user_by_email(email)
    if user is None:
        raise HTTPException(status_code=404, detail="Пользователь не найден")

    return user
