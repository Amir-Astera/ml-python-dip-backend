from datetime import datetime

from fastapi import HTTPException

from app.constants import CATEGORY_META, MONTH_LABELS
from app.db import get_connection


def currency(amount: float, signed: bool = False):
    value = f"{abs(amount):,.0f}".replace(",", " ")
    if signed:
        sign = "+" if amount >= 0 else "-"
        return f"{sign}₸ {value}"
    return f"₸ {value}"


def month_label(month_key: str):
    _, month = month_key.split("-")
    return MONTH_LABELS.get(month, month)


def date_label(date_value: str, category: str):
    date_obj = datetime.strptime(date_value, "%Y-%m-%d")
    return f"{date_obj.day:02d} {MONTH_LABELS.get(date_obj.strftime('%m'))} {date_obj.year} • {category}"


def category_meta(category: str):
    return CATEGORY_META.get(
        category,
        {"icon": "•", "iconColor": "var(--accent-blue)", "iconBackground": "var(--bg-body)", "color": "var(--accent-blue)"},
    )


def ml_overview_snapshot(user_id: int):
    try:
        from app.services.ml_service import user_ml_overview_payload

        return user_ml_overview_payload(user_id)
    except Exception:
        return None


def ml_forecast_summary(user_id: int):
    overview = ml_overview_snapshot(user_id)
    if not overview or not overview.get("forecast"):
        return None

    top_item = overview["forecast"][0]
    return {
        "month": overview.get("forecastMonth"),
        "topCategory": top_item["category"],
        "predictedAmount": top_item["predictedAmount"],
        "predictedLabel": top_item["predictedLabel"],
        "status": top_item["status"],
        "percentOfBudget": top_item.get("percentOfBudget"),
    }


def latest_period_for_user(user_id: int):
    with get_connection() as connection:
        row = connection.execute(
            "SELECT MAX(substr(tx_date, 1, 7)) AS period FROM transactions WHERE user_id = ?",
            (user_id,),
        ).fetchone()
    return row["period"] or datetime.utcnow().strftime("%Y-%m")


def fetch_transactions_for_user(user_id: int, search: str = "", category: str = "all"):
    query = "SELECT * FROM transactions WHERE user_id = ?"
    params = [user_id]

    if search:
        query += " AND (LOWER(title) LIKE ? OR LOWER(note) LIKE ?)"
        like_value = f"%{search.lower()}%"
        params.extend([like_value, like_value])

    if category and category != "all":
        query += " AND category = ?"
        params.append(category)

    query += " ORDER BY tx_date DESC, id DESC"

    with get_connection() as connection:
        return connection.execute(query, params).fetchall()


def serialize_transaction(row):
    meta = category_meta(row["category"])
    signed_amount = row["amount"] if row["tx_type"] == "income" else -row["amount"]
    return {
        "id": row["id"],
        "title": row["title"],
        "category": row["category"],
        "type": row["tx_type"],
        "amount": row["amount"],
        "amountLabel": currency(signed_amount, signed=True),
        "date": row["tx_date"],
        "dateLabel": date_label(row["tx_date"], row["category"]),
        "note": row["note"],
        "positive": row["tx_type"] == "income",
        "icon": meta["icon"],
        "iconColor": meta["iconColor"],
        "iconBackground": meta["iconBackground"],
    }


def dashboard_summary_cards(user_id: int):
    latest_period = latest_period_for_user(user_id)
    with get_connection() as connection:
        totals = connection.execute(
            """
            SELECT
                COALESCE(SUM(CASE WHEN tx_type = 'income' THEN amount ELSE 0 END), 0) AS income_total,
                COALESCE(SUM(CASE WHEN tx_type = 'expense' THEN amount ELSE 0 END), 0) AS expense_total,
                COUNT(*) AS operations_total
            FROM transactions
            WHERE user_id = ?
            """,
            (user_id,),
        ).fetchone()
        latest_month = connection.execute(
            """
            SELECT
                COALESCE(SUM(CASE WHEN tx_type = 'income' THEN amount ELSE 0 END), 0) AS income_total,
                COALESCE(SUM(CASE WHEN tx_type = 'expense' THEN amount ELSE 0 END), 0) AS expense_total,
                COUNT(*) AS operations_total
            FROM transactions
            WHERE user_id = ? AND substr(tx_date, 1, 7) = ?
            """,
            (user_id, latest_period),
        ).fetchone()
        previous_period_row = connection.execute(
            """
            SELECT substr(tx_date, 1, 7) AS month_key
            FROM transactions
            WHERE user_id = ? AND substr(tx_date, 1, 7) < ?
            GROUP BY substr(tx_date, 1, 7)
            ORDER BY month_key DESC
            LIMIT 1
            """,
            (user_id, latest_period),
        ).fetchone()

        previous_month = {"income_total": 0, "expense_total": 0}
        if previous_period_row:
            previous_month = connection.execute(
                """
                SELECT
                    COALESCE(SUM(CASE WHEN tx_type = 'income' THEN amount ELSE 0 END), 0) AS income_total,
                    COALESCE(SUM(CASE WHEN tx_type = 'expense' THEN amount ELSE 0 END), 0) AS expense_total
                FROM transactions
                WHERE user_id = ? AND substr(tx_date, 1, 7) = ?
                """,
                (user_id, previous_period_row["month_key"]),
            ).fetchone()

    balance = totals["income_total"] - totals["expense_total"]
    month_balance = latest_month["income_total"] - latest_month["expense_total"]
    forecast = balance + month_balance * 0.35
    income_change = latest_month["income_total"] - previous_month["income_total"]
    expense_change = latest_month["expense_total"] - previous_month["expense_total"]
    ml_summary = ml_forecast_summary(user_id)

    forecast_value = currency(forecast)
    forecast_description = "Прогноз на основе текущей динамики расходов"
    if ml_summary:
        forecast_value = ml_summary["predictedLabel"]
        forecast_description = f"ML-прогноз самой затратной категории {ml_summary['topCategory']} на {ml_summary['month']}"

    return [
        {
            "title": "Net Balance",
            "badge": currency(month_balance, signed=True),
            "badgeClass": "badge-neutral",
            "progress": "100%",
            "fill": "var(--accent-blue)",
            "value": currency(balance),
            "description": "Доступно по всем счетам пользователя",
        },
        {
            "title": "Monthly Income",
            "badge": f"{abs(int(income_change // 1000))}k",
            "badgeClass": "badge-positive",
            "progress": "68%",
            "fill": "var(--accent-green)",
            "value": currency(latest_month["income_total"]),
            "description": f"Доходы за {month_label(latest_period)}",
        },
        {
            "title": "Monthly Expenses",
            "badge": f"{abs(int(expense_change // 1000))}k",
            "badgeClass": "badge-negative",
            "progress": "46%",
            "fill": "var(--accent-pink)",
            "value": currency(latest_month["expense_total"]),
            "description": f"{latest_month['operations_total']} операций за месяц",
        },
        {
            "title": "EOM Forecast",
            "badge": "ML",
            "badgeClass": "badge-ml",
            "progress": "82%",
            "fill": "linear-gradient(90deg, var(--accent-purple), var(--accent-cyan))",
            "value": forecast_value,
            "description": forecast_description,
        },
    ]


def dashboard_cashflow(user_id: int):
    with get_connection() as connection:
        rows = connection.execute(
            """
            SELECT
                substr(tx_date, 1, 7) AS month_key,
                COALESCE(SUM(CASE WHEN tx_type = 'income' THEN amount ELSE 0 END), 0) AS income_total,
                COALESCE(SUM(CASE WHEN tx_type = 'expense' THEN amount ELSE 0 END), 0) AS expense_total
            FROM transactions
            WHERE user_id = ?
            GROUP BY substr(tx_date, 1, 7)
            ORDER BY month_key DESC
            LIMIT 6
            """,
            (user_id,),
        ).fetchall()

    rows = list(reversed(rows))
    max_value = max([max(row["income_total"], row["expense_total"]) for row in rows] or [1])
    result = []
    for row in rows:
        result.append(
            {
                "label": month_label(row["month_key"]),
                "income": f"{max(12, round(row['income_total'] / max_value * 100))}%",
                "expense": f"{max(10, round(row['expense_total'] / max_value * 100))}%",
            }
        )
    return result


def dashboard_budgets(user_id: int):
    latest_period = latest_period_for_user(user_id)
    with get_connection() as connection:
        rows = connection.execute(
            """
            SELECT
                b.category,
                b.limit_amount,
                COALESCE(SUM(CASE WHEN t.tx_type = 'expense' AND substr(t.tx_date, 1, 7) = ? THEN t.amount ELSE 0 END), 0) AS spent_amount
            FROM budgets b
            LEFT JOIN transactions t ON t.user_id = b.user_id AND t.category = b.category
            WHERE b.user_id = ?
            GROUP BY b.id, b.category, b.limit_amount
            ORDER BY b.id ASC
            LIMIT 4
            """,
            (latest_period, user_id),
        ).fetchall()

    cards = []
    for row in rows:
        percent = 0 if row["limit_amount"] == 0 else int(round(row["spent_amount"] / row["limit_amount"] * 100))
        meta = category_meta(row["category"])
        cards.append(
            {
                "name": row["category"],
                "percent": f"{percent}%",
                "amount": currency(row["spent_amount"]),
                "color": meta["color"],
                "percentColor": meta["color"] if percent >= 90 else None,
            }
        )
    return cards


def dashboard_insight(user_id: int):
    latest_period = latest_period_for_user(user_id)
    with get_connection() as connection:
        row = connection.execute(
            """
            SELECT category, SUM(amount) AS total_expense
            FROM transactions
            WHERE user_id = ? AND tx_type = 'expense' AND substr(tx_date, 1, 7) = ?
            GROUP BY category
            ORDER BY total_expense DESC
            LIMIT 1
            """,
            (user_id, latest_period),
        ).fetchone()

    if row is None:
        return "Пока недостаточно данных для анализа. Добавьте операции, и система построит рекомендации."

    ml_summary = ml_forecast_summary(user_id)
    if ml_summary:
        return f"Самая крупная категория расходов в {month_label(latest_period)} — {row['category']}. ML-прогноз на {ml_summary['month']} показывает повышенную нагрузку по категории {ml_summary['topCategory']} на сумму {ml_summary['predictedLabel']}."

    return f"Самая крупная категория расходов в {month_label(latest_period)} — {row['category']}. Модель рекомендует пересмотреть лимит по этой группе и сохранить резерв на обязательные платежи."


def budget_rows(user_id: int):
    latest_period = latest_period_for_user(user_id)
    with get_connection() as connection:
        rows = connection.execute(
            """
            SELECT
                b.id,
                b.category,
                b.limit_amount,
                COALESCE(SUM(CASE WHEN t.tx_type = 'expense' AND substr(t.tx_date, 1, 7) = ? THEN t.amount ELSE 0 END), 0) AS spent_amount
            FROM budgets b
            LEFT JOIN transactions t ON t.user_id = b.user_id AND t.category = b.category
            WHERE b.user_id = ?
            GROUP BY b.id, b.category, b.limit_amount
            ORDER BY b.category ASC
            """,
            (latest_period, user_id),
        ).fetchall()

    items = []
    for row in rows:
        spent_amount = row["spent_amount"]
        remaining = row["limit_amount"] - spent_amount
        percent = 0 if row["limit_amount"] == 0 else int(round(spent_amount / row["limit_amount"] * 100))
        meta = category_meta(row["category"])
        items.append(
            {
                "id": row["id"],
                "category": row["category"],
                "limitAmount": row["limit_amount"],
                "limitLabel": currency(row["limit_amount"]),
                "spentAmount": spent_amount,
                "spentLabel": currency(spent_amount),
                "remainingAmount": remaining,
                "remainingLabel": currency(max(remaining, 0)),
                "percent": percent,
                "percentLabel": f"{percent}%",
                "color": meta["color"],
                "icon": meta["icon"],
            }
        )
    return items


def analytics_payload(user_id: int):
    with get_connection() as connection:
        totals = connection.execute(
            """
            SELECT
                COALESCE(SUM(CASE WHEN tx_type = 'income' THEN amount ELSE 0 END), 0) AS income_total,
                COALESCE(SUM(CASE WHEN tx_type = 'expense' THEN amount ELSE 0 END), 0) AS expense_total,
                COUNT(*) AS operations_total
            FROM transactions
            WHERE user_id = ?
            """,
            (user_id,),
        ).fetchone()
        by_category = connection.execute(
            """
            SELECT category, SUM(amount) AS total_expense
            FROM transactions
            WHERE user_id = ? AND tx_type = 'expense'
            GROUP BY category
            ORDER BY total_expense DESC
            """,
            (user_id,),
        ).fetchall()
        monthly_rows = connection.execute(
            """
            SELECT
                substr(tx_date, 1, 7) AS month_key,
                COALESCE(SUM(CASE WHEN tx_type = 'income' THEN amount ELSE 0 END), 0) AS income_total,
                COALESCE(SUM(CASE WHEN tx_type = 'expense' THEN amount ELSE 0 END), 0) AS expense_total
            FROM transactions
            WHERE user_id = ?
            GROUP BY substr(tx_date, 1, 7)
            ORDER BY month_key DESC
            LIMIT 6
            """,
            (user_id,),
        ).fetchall()

    balance = totals["income_total"] - totals["expense_total"]
    savings_rate = int(round(balance / totals["income_total"] * 100)) if totals["income_total"] else 0
    total_expenses = sum(row["total_expense"] for row in by_category) or 1
    ml_overview = ml_overview_snapshot(user_id)

    category_breakdown = []
    for row in by_category:
        meta = category_meta(row["category"])
        category_breakdown.append(
            {
                "category": row["category"],
                "amount": row["total_expense"],
                "amountLabel": currency(row["total_expense"]),
                "percent": int(round(row["total_expense"] / total_expenses * 100)),
                "color": meta["color"],
            }
        )

    monthly_trend = [
        {
            "label": month_label(row["month_key"]),
            "income": row["income_total"],
            "expense": row["expense_total"],
            "incomeLabel": currency(row["income_total"]),
            "expenseLabel": currency(row["expense_total"]),
        }
        for row in reversed(monthly_rows)
    ]

    top_category = category_breakdown[0]["category"] if category_breakdown else "нет данных"

    return {
        "summaryCards": [
            {"title": "Общий доход", "value": currency(totals['income_total']), "caption": "Сумма всех поступлений"},
            {"title": "Общий расход", "value": currency(totals['expense_total']), "caption": "Сумма всех списаний"},
            {"title": "Баланс", "value": currency(balance), "caption": "Разница между доходом и расходом"},
            {"title": "Норма сбережений", "value": f"{savings_rate}%", "caption": "Доля сохранённых средств"},
        ],
        "categoryBreakdown": category_breakdown,
        "monthlyTrend": monthly_trend,
        "mlOverview": ml_overview,
        "insights": [
            f"Наибольшая доля расходов приходится на категорию «{top_category}».",
            f"За весь период сохранено {currency(balance)} чистого остатка.",
            f"В системе уже обработано {totals['operations_total']} операций пользователя.",
        ],
    }


def get_transactions_payload(user_id: int, search: str = "", category: str = "all"):
    rows = fetch_transactions_for_user(user_id, search=search, category=category)
    income_total = sum(row["amount"] for row in rows if row["tx_type"] == "income")
    expense_total = sum(row["amount"] for row in rows if row["tx_type"] == "expense")
    return {
        "items": [serialize_transaction(row) for row in rows],
        "summary": {
            "count": len(rows),
            "incomeLabel": currency(income_total),
            "expenseLabel": currency(expense_total),
        },
        "categories": sorted({row["category"] for row in rows if row["category"] != "Income"}),
    }


def create_transaction_payload(user_id: int, payload):
    transaction_type = payload.transaction_type.lower()
    if transaction_type not in {"income", "expense"}:
        raise HTTPException(status_code=400, detail="Тип операции должен быть income или expense")

    try:
        datetime.strptime(payload.transaction_date, "%Y-%m-%d")
    except ValueError as error:
        raise HTTPException(status_code=400, detail="Дата должна быть в формате YYYY-MM-DD") from error

    with get_connection() as connection:
        row = connection.execute(
            """
            INSERT INTO transactions (user_id, title, category, tx_type, amount, tx_date, note, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            RETURNING *
            """,
            (
                user_id,
                payload.title,
                payload.category,
                transaction_type,
                payload.amount,
                payload.transaction_date,
                payload.note,
                datetime.utcnow().isoformat(),
            ),
        )
        row = row.fetchone()
        connection.commit()

    return {"item": serialize_transaction(row), "message": "Операция добавлена"}


def delete_transaction_payload(user_id: int, transaction_id: int):
    with get_connection() as connection:
        row = connection.execute(
            "SELECT id FROM transactions WHERE id = ? AND user_id = ?",
            (transaction_id, user_id),
        ).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Операция не найдена")
        connection.execute("DELETE FROM transactions WHERE id = ? AND user_id = ?", (transaction_id, user_id))
        connection.commit()
    return {"message": "Операция удалена"}


def save_budget_payload(user_id: int, payload):
    with get_connection() as connection:
        existing = connection.execute(
            "SELECT id FROM budgets WHERE user_id = ? AND category = ?",
            (user_id, payload.category),
        ).fetchone()
        if existing is None:
            connection.execute(
                "INSERT INTO budgets (user_id, category, limit_amount) VALUES (?, ?, ?)",
                (user_id, payload.category, payload.limit_amount),
            )
        else:
            connection.execute(
                "UPDATE budgets SET limit_amount = ? WHERE user_id = ? AND category = ?",
                (payload.limit_amount, user_id, payload.category),
            )
        connection.commit()
    return {"items": budget_rows(user_id), "message": "Бюджет сохранён"}


def get_dashboard_payload(user_id: int):
    transactions = fetch_transactions_for_user(user_id)
    return {
        "summaryCards": dashboard_summary_cards(user_id),
        "cashflow": dashboard_cashflow(user_id),
        "insight": dashboard_insight(user_id),
        "budgets": dashboard_budgets(user_id),
        "mlForecast": ml_forecast_summary(user_id),
        "transactions": [serialize_transaction(row) for row in transactions[:4]],
    }
