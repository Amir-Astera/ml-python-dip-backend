from fastapi import Header, HTTPException

from app.db import get_connection


def get_current_user(authorization: str = Header(default="")):
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Требуется авторизация")

    token = authorization.replace("Bearer ", "").strip()
    with get_connection() as connection:
        user = connection.execute(
            """
            SELECT u.*
            FROM users u
            INNER JOIN sessions s ON s.user_id = u.id
            WHERE s.token = ?
            """,
            (token,),
        ).fetchone()

    if user is None:
        raise HTTPException(status_code=401, detail="Сессия не найдена или устарела")

    return user
