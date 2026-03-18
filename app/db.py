import os
from datetime import datetime

import psycopg
from psycopg.rows import dict_row

from app.constants import DEFAULT_BUDGETS, DEFAULT_TRANSACTIONS

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@127.0.0.1:5432/finflow')


def _convert_placeholders(query: str):
    return query.replace('?', '%s')


class DatabaseConnection:
    def __init__(self):
        self._connection = psycopg.connect(DATABASE_URL, row_factory=dict_row)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is not None:
            self._connection.rollback()
        self._connection.close()

    def cursor(self):
        return self._connection.cursor()

    def execute(self, query: str, params=None):
        cursor = self._connection.cursor()
        cursor.execute(_convert_placeholders(query), params or ())
        return cursor

    def executemany(self, query: str, param_sets):
        cursor = self._connection.cursor()
        cursor.executemany(_convert_placeholders(query), param_sets)
        return cursor

    def commit(self):
        self._connection.commit()


def get_connection():
    return DatabaseConnection()


def init_db():
    with get_connection() as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id BIGSERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL UNIQUE,
                password TEXT NOT NULL,
                is_seeded BOOLEAN NOT NULL DEFAULT FALSE
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS transactions (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                title TEXT NOT NULL,
                category TEXT NOT NULL,
                tx_type TEXT NOT NULL,
                amount DOUBLE PRECISION NOT NULL,
                tx_date TEXT NOT NULL,
                note TEXT DEFAULT '',
                created_at TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users (id)
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS budgets (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                category TEXT NOT NULL,
                limit_amount DOUBLE PRECISION NOT NULL,
                UNIQUE(user_id, category),
                FOREIGN KEY (user_id) REFERENCES users (id)
            )
            """
        )
        connection.commit()


def ensure_seed_data(user_id: int):
    with get_connection() as connection:
        existing_transactions = connection.execute(
            'SELECT COUNT(*) AS total FROM transactions WHERE user_id = ?',
            (user_id,),
        ).fetchone()['total']
        existing_budgets = connection.execute(
            'SELECT COUNT(*) AS total FROM budgets WHERE user_id = ?',
            (user_id,),
        ).fetchone()['total']

        if existing_budgets == 0:
            connection.executemany(
                'INSERT INTO budgets (user_id, category, limit_amount) VALUES (?, ?, ?)',
                [(user_id, category, limit_amount) for category, limit_amount in DEFAULT_BUDGETS],
            )

        if existing_transactions == 0:
            connection.executemany(
                """
                INSERT INTO transactions (user_id, title, category, tx_type, amount, tx_date, note, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (user_id, title, category, tx_type, amount, tx_date, note, datetime.utcnow().isoformat())
                    for title, category, tx_type, amount, tx_date, note in DEFAULT_TRANSACTIONS
                ],
            )

        connection.commit()
