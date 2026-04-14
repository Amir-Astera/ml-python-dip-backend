from datetime import datetime
from uuid import uuid4

from fastapi import HTTPException

from app.db import ensure_seed_data, get_connection
from app.password_util import hash_password, verify_password


def get_user_by_email(email: str):
    with get_connection() as connection:
        return connection.execute('SELECT * FROM users WHERE email = ?', (email.lower(),)).fetchone()


def get_user_by_id(user_id: int):
    with get_connection() as connection:
        return connection.execute('SELECT * FROM users WHERE id = ?', (user_id,)).fetchone()


def serialize_user(user_row):
    return {
        'id': user_row['id'],
        'name': user_row['name'],
        'email': user_row['email'],
    }


def create_token_for_user(user_id: int):
    token = f'token-{uuid4().hex}'
    now = datetime.utcnow().isoformat()
    with get_connection() as connection:
        connection.execute(
            'INSERT INTO sessions (user_id, token, created_at) VALUES (?, ?, ?)',
            (user_id, token, now),
        )
        connection.commit()
    return token


def delete_session_by_token(token: str):
    tok = (token or '').strip()
    if not tok:
        return
    with get_connection() as connection:
        connection.execute('DELETE FROM sessions WHERE token = ?', (tok,))
        connection.commit()


def register_user(payload):
    email = payload.email.lower()
    if get_user_by_email(email):
        raise HTTPException(status_code=400, detail='Пользователь с таким email уже существует')

    secret = hash_password(payload.password)
    with get_connection() as connection:
        user = connection.execute(
            'INSERT INTO users (name, email, password, is_seeded) VALUES (?, ?, ?, ?) RETURNING *',
            (payload.name, email, secret, False),
        )
        user = user.fetchone()
        connection.commit()

    token = create_token_for_user(user['id'])
    return {'token': token, 'user': serialize_user(user)}


def login_user(payload):
    email = payload.email.lower()
    user = get_user_by_email(email)
    if user is None or not verify_password(payload.password, user['password']):
        raise HTTPException(status_code=401, detail='Неверный email или пароль')

    token = create_token_for_user(user['id'])
    return {'token': token, 'user': serialize_user(user)}


def ensure_default_user():
    default_user = get_user_by_email('student@finflow.kz')
    if default_user is None:
        with get_connection() as connection:
            default_user = connection.execute(
                'INSERT INTO users (name, email, password, is_seeded) VALUES (?, ?, ?, ?) RETURNING *',
                ('Студент', 'student@finflow.kz', '123456', True),
            )
            default_user = default_user.fetchone()
            connection.commit()
    elif not default_user['is_seeded']:
        with get_connection() as connection:
            connection.execute('UPDATE users SET is_seeded = ? WHERE id = ?', (True, default_user['id']))
            connection.commit()
        default_user = get_user_by_id(default_user['id'])

    ensure_seed_data(default_user['id'], seed_demo=True)
