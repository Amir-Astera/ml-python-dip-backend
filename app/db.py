import os
from datetime import datetime

import psycopg
from psycopg.rows import dict_row

from app.constants import DEFAULT_BUDGETS

# Увеличьте, чтобы принудительно пересоздать демо-набор у тестовой учётки (student@finflow.kz).
CURRENT_DEMO_DATA_VERSION = 1

# Суммы расходов за месяц (₸) в духе табл. 2 docum.md + сезонность (пик в декабре, март).
_RESEARCH_MONTH_EXPENSE_TOTALS = [
    (2024, 1, 285000),
    (2024, 2, 260000),
    (2024, 3, 295000),
    (2024, 4, 270000),
    (2024, 5, 265000),
    (2024, 6, 258000),
    (2024, 7, 272000),
    (2024, 8, 268000),
    (2024, 9, 281000),
    (2024, 10, 266000),
    (2024, 11, 279000),
    (2024, 12, 310000),
    (2025, 1, 298000),
    (2025, 2, 262000),
    (2025, 3, 302000),
    (2025, 4, 275000),
    (2025, 5, 269000),
    (2025, 6, 261000),
    (2025, 7, 274000),
    (2025, 8, 271000),
    (2025, 9, 283000),
    (2025, 10, 267000),
    (2025, 11, 285000),
    (2025, 12, 318000),
    (2026, 1, 291000),
    (2026, 2, 268000),
    (2026, 3, 305000),
]

_DEMO_NOTE = "Демо-набор для диплома: статистика и ML (не удалять вручную при тестах)"


def _allocate_month_expenses(total: int) -> dict[str, int]:
    parts = {
        "Groceries": round(total * 0.38),
        "Housing": round(total * 0.25),
        "Transport": round(total * 0.11),
        "Entertainment": round(total * 0.08),
        "Utilities": round(total * 0.07),
        "Health": round(total * 0.05),
        "Education": round(total * 0.025),
        "Shopping": round(total * 0.025),
    }
    diff = int(total - sum(parts.values()))
    parts["Groceries"] = max(0, parts["Groceries"] + diff)
    return parts


def _split_amounts(total: int, ratios: list[tuple[str, float]]) -> list[tuple[str, int]]:
    raw = [max(1, round(total * r)) for _, r in ratios]
    s = sum(raw)
    if s != total and raw:
        raw[0] += total - s
    return [(ratios[i][0], raw[i]) for i in range(len(ratios))]


def _expense_pieces(category: str, amount: int, y: int, m: int) -> list[tuple[str, int, int]]:
    """Возвращает (title, amount, day) — несколько операций на категорию для ML и описательной статистики."""
    key = f"{m:02d}.{y}"
    if amount <= 0:
        return []
    if category == "Groceries":
        chunks = _split_amounts(
            amount,
            [
                (f"Magnum Алматы — продукты {key}", 0.42),
                (f"Green Market — овощи и фрукты {key}", 0.28),
                (f"Пекарня у дома {key}", 0.14),
                (f"Молочные продукты и яйца {key}", 0.16),
            ],
        )
        days = [7, 14, 21, 26]
        return [(t, a, days[i]) for i, (t, a) in enumerate(chunks)]
    if category == "Housing":
        return [(f"Аренда квартиры (договор) {key}", amount, 5)]
    if category == "Transport":
        chunks = _split_amounts(
            amount,
            [(f"Метро и автобус {key}", 0.35), (f"Яндекс Такси {key}", 0.45), (f"Парковка и топливо {key}", 0.2)],
        )
        days = [8, 16, 24]
        return [(t, a, days[i]) for i, (t, a) in enumerate(chunks)]
    if category == "Entertainment":
        chunks = _split_amounts(
            amount,
            [(f"Кино / IMAX {key}", 0.25), (f"Кафе с друзьями {key}", 0.35), (f"Подписки (музыка, стриминг) {key}", 0.4)],
        )
        days = [11, 19, 27]
        return [(t, a, days[i]) for i, (t, a) in enumerate(chunks)]
    if category == "Utilities":
        u1 = round(amount * 0.55)
        u2 = amount - u1
        return [
            (f"Коммунальные услуги (тепло/вода) {key}", max(1, u1), 10),
            (f"Интернет и мобильная связь {key}", max(1, u2), 12),
        ]
    if category == "Health":
        chunks = _split_amounts(amount, [(f"Аптека {key}", 0.55), (f"Поликлиника / анализы {key}", 0.45)])
        days = [9, 23]
        return [(t, a, days[i]) for i, (t, a) in enumerate(chunks)]
    if category == "Education":
        chunks = _split_amounts(amount, [(f"Онлайн-курс (Coursera) {key}", 0.6), (f"Учебники и канцтовары {key}", 0.4)])
        days = [6, 20]
        return [(t, a, days[i]) for i, (t, a) in enumerate(chunks)]
    if category == "Shopping":
        chunks = _split_amounts(amount, [(f"Ozon / маркетплейс {key}", 0.65), (f"Одежда и обувь {key}", 0.35)])
        days = [13, 25]
        return [(t, a, days[i]) for i, (t, a) in enumerate(chunks)]
    return [(f"{category} {key}", amount, 15)]


def _student_ml_stat_demo_rows(user_id: int, created_at: str) -> list[tuple]:
    """Плотный демо-набор: 27 месяцев, много категорий и операций — для ML и стат. главы диплома."""
    rows: list[tuple] = []
    for y, m, month_total in _RESEARCH_MONTH_EXPENSE_TOTALS:
        parts = _allocate_month_expenses(month_total)
        for cat, subtotal in parts.items():
            for title, amt, day in _expense_pieces(cat, subtotal, y, m):
                rows.append(
                    (
                        user_id,
                        title,
                        cat,
                        "expense",
                        float(amt),
                        f"{y}-{m:02d}-{min(day, 28):02d}",
                        _DEMO_NOTE,
                        created_at,
                        "",
                    )
                )
        base_salary = 520000 + (y - 2024) * 12000 + m * 900
        rows.append(
            (
                user_id,
                f"ООО «ТехноСервис» — заработная плата {m:02d}.{y}",
                "Income",
                "income",
                float(base_salary),
                f"{y}-{m:02d}-05",
                _DEMO_NOTE,
                created_at,
                "Зарплата",
            )
        )
        if m == 3:
            rows.append(
                (
                    user_id,
                    f"Премия (Наурыз) {y}",
                    "Income",
                    "income",
                    42000.0,
                    f"{y}-{m:02d}-22",
                    _DEMO_NOTE,
                    created_at,
                    "Премия",
                )
            )
        if m == 12:
            rows.append(
                (
                    user_id,
                    f"Годовая премия {y}",
                    "Income",
                    "income",
                    78000.0,
                    f"{y}-{m:02d}-28",
                    _DEMO_NOTE,
                    created_at,
                    "Премия",
                )
            )
        if m in (7, 8) and y >= 2024:
            rows.append(
                (
                    user_id,
                    f"Фриланс — проект UI {m:02d}.{y}",
                    "Income",
                    "income",
                    28000.0 + (m - 7) * 4000.0,
                    f"{y}-{m:02d}-18",
                    _DEMO_NOTE,
                    created_at,
                    "Фриланс",
                )
            )
    return rows


def _demo_loan_rows(user_id: int, created_at: str) -> list[tuple]:
    return [
        (
            user_id,
            "Автокредит (демо Kaspi Bank)",
            3200000.0,
            1180000.0,
            38500.0,
            "2026-04-08",
            _DEMO_NOTE,
            created_at,
        ),
        (
            user_id,
            "Потребительский кредит (демо)",
            680000.0,
            172000.0,
            19200.0,
            "2026-04-15",
            _DEMO_NOTE,
            created_at,
        ),
    ]


def _demo_reminder_rows(user_id: int, created_at: str) -> list[tuple]:
    return [
        (user_id, "Перевод арендодателю", "2026-04-05", _DEMO_NOTE, created_at),
        (user_id, "Оплата коммуналки", "2026-04-10", _DEMO_NOTE, created_at),
        (user_id, "Проверка лимитов бюджета", "2026-04-28", _DEMO_NOTE, created_at),
    ]

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
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS loans (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                title TEXT NOT NULL,
                total_amount DOUBLE PRECISION NOT NULL,
                remaining_amount DOUBLE PRECISION NOT NULL,
                payment_per_month DOUBLE PRECISION,
                next_payment_date TEXT DEFAULT '',
                note TEXT DEFAULT '',
                created_at TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users (id)
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS reminders (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                title TEXT NOT NULL,
                event_date TEXT NOT NULL,
                note TEXT DEFAULT '',
                created_at TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users (id)
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS sessions (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                token TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users (id)
            )
            """
        )
        cursor.execute(
            "ALTER TABLE transactions ADD COLUMN IF NOT EXISTS income_source TEXT DEFAULT ''"
        )
        cursor.execute(
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS demo_tx_version INTEGER DEFAULT 0"
        )
        connection.commit()


def ensure_seed_data(user_id: int, seed_demo: bool = False):
    """Демо-данные только если seed_demo=True (например, тестовый student@). Новые регистрации — без сидов."""
    if not seed_demo:
        return
    with get_connection() as connection:
        user_row = connection.execute(
            "SELECT COALESCE(demo_tx_version, 0) AS v FROM users WHERE id = ?",
            (user_id,),
        ).fetchone()
        stored_ver = int(user_row["v"] or 0) if user_row else 0

        if stored_ver < CURRENT_DEMO_DATA_VERSION:
            connection.execute("DELETE FROM reminders WHERE user_id = ?", (user_id,))
            connection.execute("DELETE FROM loans WHERE user_id = ?", (user_id,))
            connection.execute("DELETE FROM budgets WHERE user_id = ?", (user_id,))
            connection.execute("DELETE FROM transactions WHERE user_id = ?", (user_id,))
            connection.execute(
                "UPDATE users SET demo_tx_version = ? WHERE id = ?",
                (CURRENT_DEMO_DATA_VERSION, user_id),
            )

        existing_transactions = connection.execute(
            'SELECT COUNT(*) AS total FROM transactions WHERE user_id = ?',
            (user_id,),
        ).fetchone()['total']
        existing_budgets = connection.execute(
            'SELECT COUNT(*) AS total FROM budgets WHERE user_id = ?',
            (user_id,),
        ).fetchone()['total']
        existing_loans = connection.execute(
            "SELECT COUNT(*) AS total FROM loans WHERE user_id = ?",
            (user_id,),
        ).fetchone()["total"]
        existing_reminders = connection.execute(
            "SELECT COUNT(*) AS total FROM reminders WHERE user_id = ?",
            (user_id,),
        ).fetchone()["total"]

        if existing_budgets == 0:
            connection.executemany(
                'INSERT INTO budgets (user_id, category, limit_amount) VALUES (?, ?, ?)',
                [(user_id, category, limit_amount) for category, limit_amount in DEFAULT_BUDGETS],
            )

        created = datetime.utcnow().isoformat()
        if existing_transactions == 0:
            demo_rows = _student_ml_stat_demo_rows(user_id, created)
            connection.executemany(
                """
                INSERT INTO transactions (user_id, title, category, tx_type, amount, tx_date, note, created_at, income_source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                demo_rows,
            )

        if existing_loans == 0:
            connection.executemany(
                """
                INSERT INTO loans (user_id, title, total_amount, remaining_amount, payment_per_month, next_payment_date, note, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                _demo_loan_rows(user_id, created),
            )

        if existing_reminders == 0:
            connection.executemany(
                """
                INSERT INTO reminders (user_id, title, event_date, note, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                _demo_reminder_rows(user_id, created),
            )

        connection.commit()
