from datetime import datetime

from fastapi import HTTPException

from app.constants import CATEGORY_META, DEFAULT_BUDGETS, MONTH_LABELS
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


def _parse_analytics_date(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    if not stripped:
        return None
    try:
        datetime.strptime(stripped, "%Y-%m-%d")
    except ValueError as error:
        raise HTTPException(status_code=400, detail="Некорректная дата, ожидается формат YYYY-MM-DD") from error
    return stripped


def _normalize_analytics_range(date_from: str | None, date_to: str | None):
    df = _parse_analytics_date(date_from)
    dt = _parse_analytics_date(date_to)
    if df and dt and df > dt:
        return dt, df
    return df, dt


def _analytics_date_sql_fragment(date_from: str | None, date_to: str | None):
    clauses = []
    params: list = []
    if date_from:
        clauses.append("tx_date >= ?")
        params.append(date_from)
    if date_to:
        clauses.append("tx_date <= ?")
        params.append(date_to)
    if not clauses:
        return "", []
    return " AND " + " AND ".join(clauses), params


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


def fetch_transactions_for_user(
    user_id: int,
    search: str = "",
    category: str = "all",
    date_from: str | None = None,
    date_to: str | None = None,
):
    df, dt = _normalize_analytics_range(date_from, date_to)
    date_frag, date_params = _analytics_date_sql_fragment(df, dt)
    query = "SELECT * FROM transactions WHERE user_id = ?"
    params = [user_id]

    if search:
        query += " AND (LOWER(title) LIKE ? OR LOWER(note) LIKE ?)"
        like_value = f"%{search.lower()}%"
        params.extend([like_value, like_value])

    if category and category != "all":
        query += " AND category = ?"
        params.append(category)

    query += date_frag
    params.extend(date_params)

    query += " ORDER BY tx_date DESC, id DESC"

    with get_connection() as connection:
        return connection.execute(query, params).fetchall()


def serialize_transaction(row):
    meta = category_meta(row["category"])
    signed_amount = row["amount"] if row["tx_type"] == "income" else -row["amount"]
    src = (row.get("income_source") or "").strip()
    base_dl = date_label(row["tx_date"], row["category"])
    if src and row["tx_type"] == "income":
        date_dl = f"{base_dl} · {src}"
    else:
        date_dl = base_dl
    return {
        "id": row["id"],
        "title": row["title"],
        "category": row["category"],
        "type": row["tx_type"],
        "amount": row["amount"],
        "amountLabel": currency(signed_amount, signed=True),
        "date": row["tx_date"],
        "dateLabel": date_dl,
        "note": row["note"],
        "incomeSource": src,
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
        inc_pct = max(8, round(row["income_total"] / max_value * 100))
        exp_pct = max(8, round(row["expense_total"] / max_value * 100))
        inc_l = currency(row["income_total"])
        exp_l = currency(row["expense_total"])
        result.append(
            {
                "label": month_label(row["month_key"]),
                "monthKey": row["month_key"],
                "income": f"{inc_pct}%",
                "expense": f"{exp_pct}%",
                "incomeLabel": inc_l,
                "expenseLabel": exp_l,
                "incomeTotal": float(row["income_total"]),
                "expenseTotal": float(row["expense_total"]),
                "barTitle": f"{month_label(row['month_key'])}: доход {inc_l}, расход {exp_l}",
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
        month_totals = connection.execute(
            """
            SELECT
                COALESCE(SUM(CASE WHEN tx_type = 'income' THEN amount ELSE 0 END), 0) AS inc,
                COALESCE(SUM(CASE WHEN tx_type = 'expense' THEN amount ELSE 0 END), 0) AS exp
            FROM transactions
            WHERE user_id = ? AND substr(tx_date, 1, 7) = ?
            """,
            (user_id, latest_period),
        ).fetchone()

    if row is None:
        return "Пока недостаточно данных для анализа. Добавьте операции, и система построит рекомендации."

    ml_summary = ml_forecast_summary(user_id)
    if ml_summary:
        fallback = (
            f"Самая крупная категория расходов в {month_label(latest_period)} — {row['category']}. "
            f"ML-прогноз на {ml_summary['month']}: категория «{ml_summary['topCategory']}», ожидается {ml_summary['predictedLabel']}."
        )
    else:
        fallback = (
            f"Самая крупная категория расходов в {month_label(latest_period)} — {row['category']}. "
            "Имеет смысл пересмотреть лимит по этой группе и оставить резерв на обязательные платежи."
        )

    from app.services.gemini_service import gemini_dashboard_advice

    facts = (
        f"Месяц: {month_label(latest_period)}. Доходы за месяц: {currency(month_totals['inc'])}. "
        f"Расходы за месяц: {currency(month_totals['exp'])}. "
        f"Крупнейшая категория расходов: {row['category']}, сумма {currency(row['total_expense'])}."
    )
    if ml_summary:
        facts += (
            f" Прогноз ML: в {ml_summary['month']} по «{ml_summary['topCategory']}» ожидается {ml_summary['predictedLabel']}."
        )
    ai = gemini_dashboard_advice(facts)
    return ai if ai else fallback


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


def analytics_payload(user_id: int, date_from: str | None = None, date_to: str | None = None):
    df, dt = _normalize_analytics_range(date_from, date_to)
    frag, date_params = _analytics_date_sql_fragment(df, dt)
    period_caption = "Сумма за выбранный интервал дат" if (df or dt) else "Сумма за весь период учёта"
    period_description = "Все данные"
    if df and dt:
        period_description = f"{df} — {dt}"
    elif df:
        period_description = f"с {df}"
    elif dt:
        period_description = f"по {dt}"

    base_params = [user_id, *date_params]
    with get_connection() as connection:
        totals = connection.execute(
            f"""
            SELECT
                COALESCE(SUM(CASE WHEN tx_type = 'income' THEN amount ELSE 0 END), 0) AS income_total,
                COALESCE(SUM(CASE WHEN tx_type = 'expense' THEN amount ELSE 0 END), 0) AS expense_total,
                COUNT(*) AS operations_total
            FROM transactions
            WHERE user_id = ?{frag}
            """,
            tuple(base_params),
        ).fetchone()
        by_category = connection.execute(
            f"""
            SELECT category, SUM(amount) AS total_expense
            FROM transactions
            WHERE user_id = ? AND tx_type = 'expense'{frag}
            GROUP BY category
            ORDER BY total_expense DESC
            """,
            tuple(base_params),
        ).fetchall()
        monthly_rows = connection.execute(
            f"""
            SELECT
                substr(tx_date, 1, 7) AS month_key,
                COALESCE(SUM(CASE WHEN tx_type = 'income' THEN amount ELSE 0 END), 0) AS income_total,
                COALESCE(SUM(CASE WHEN tx_type = 'expense' THEN amount ELSE 0 END), 0) AS expense_total
            FROM transactions
            WHERE user_id = ?{frag}
            GROUP BY substr(tx_date, 1, 7)
            ORDER BY month_key DESC
            LIMIT 6
            """,
            tuple(base_params),
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
            "barTitle": f"{month_label(row['month_key'])}: доход {currency(row['income_total'])}, расход {currency(row['expense_total'])}",
        }
        for row in reversed(monthly_rows)
    ]

    top_category = category_breakdown[0]["category"] if category_breakdown else "нет данных"
    if df or dt:
        insight_scope = f"Наибольшая доля расходов за выбранный период приходится на категорию «{top_category}»."
        insight_balance = f"За выбранный период накоплено {currency(balance)} чистого остатка."
        insight_ops = f"В выбранном интервале учтено {totals['operations_total']} операций."
    else:
        insight_scope = f"Наибольшая доля расходов за весь период учёта приходится на категорию «{top_category}»."
        insight_balance = f"За весь период учёта накоплено {currency(balance)} чистого остатка."
        insight_ops = f"В системе учтено {totals['operations_total']} операций пользователя."

    insights_list = [insight_scope, insight_balance, insight_ops]
    top3 = category_breakdown[:3]
    cat_line = ", ".join(f"{c['category']} {c['amountLabel']} ({c['percent']}%)" for c in top3) if top3 else "нет категорий"
    facts_analytics = (
        f"Период: {period_description}. Доходы: {currency(totals['income_total'])}, расходы: {currency(totals['expense_total'])}, "
        f"баланс: {currency(balance)}, операций: {totals['operations_total']}. Топ категорий расходов: {cat_line}."
    )
    from app.services.gemini_service import gemini_analytics_bullets

    ai_bullets = gemini_analytics_bullets(facts_analytics)
    if ai_bullets:
        insights_list = ai_bullets

    from app.ml.training import ml_retrain_readiness

    return {
        "period": {
            "dateFrom": df,
            "dateTo": dt,
            "description": period_description,
        },
        "summaryCards": [
            {"title": "Общий доход", "value": currency(totals['income_total']), "caption": period_caption},
            {"title": "Общий расход", "value": currency(totals['expense_total']), "caption": period_caption},
            {"title": "Баланс", "value": currency(balance), "caption": "Разница между доходом и расходом"},
            {"title": "Норма сбережений", "value": f"{savings_rate}%", "caption": "Доля сохранённых средств"},
        ],
        "categoryBreakdown": category_breakdown,
        "monthlyTrend": monthly_trend,
        "mlOverview": ml_overview,
        "mlRetrain": ml_retrain_readiness(),
        "insights": insights_list,
    }


def _user_transactions_totals(user_id: int):
    with get_connection() as connection:
        row = connection.execute(
            """
            SELECT
                COALESCE(SUM(CASE WHEN tx_type = 'income' THEN amount ELSE 0 END), 0) AS income_total,
                COALESCE(SUM(CASE WHEN tx_type = 'expense' THEN amount ELSE 0 END), 0) AS expense_total
            FROM transactions
            WHERE user_id = ?
            """,
            (user_id,),
        ).fetchone()
    return row["income_total"], row["expense_total"]


def _user_transaction_categories(user_id: int):
    with get_connection() as connection:
        rows = connection.execute(
            """
            SELECT DISTINCT category
            FROM transactions
            WHERE user_id = ?
            ORDER BY category ASC
            """,
            (user_id,),
        ).fetchall()
    return [row["category"] for row in rows]


def get_transactions_payload(
    user_id: int,
    search: str = "",
    category: str = "all",
    date_from: str | None = None,
    date_to: str | None = None,
):
    rows = fetch_transactions_for_user(
        user_id,
        search=search,
        category=category,
        date_from=date_from,
        date_to=date_to,
    )
    income_total, expense_total = _user_transactions_totals(user_id)
    from_db = _user_transaction_categories(user_id)
    default_cats = {name for name, _ in DEFAULT_BUDGETS} | {"Health", "Education", "Shopping"}
    categories = sorted(set(from_db) | default_cats)
    return {
        "items": [serialize_transaction(row) for row in rows],
        "summary": {
            "count": len(rows),
            "incomeLabel": currency(income_total),
            "expenseLabel": currency(expense_total),
        },
        "categories": [c for c in categories if c != "Income"],
    }


def create_transaction_payload(user_id: int, payload):
    transaction_type = payload.transaction_type.lower()
    if transaction_type not in {"income", "expense"}:
        raise HTTPException(status_code=400, detail="Тип операции должен быть income или expense")

    try:
        datetime.strptime(payload.transaction_date, "%Y-%m-%d")
    except ValueError as error:
        raise HTTPException(status_code=400, detail="Дата должна быть в формате YYYY-MM-DD") from error

    category = payload.category
    if transaction_type == "expense":
        try:
            from app.schemas import MLClassifyPayload
            from app.services.ml_service import classify_expense_payload

            ml = classify_expense_payload(
                MLClassifyPayload(
                    title=payload.title,
                    amount=payload.amount,
                    transaction_date=payload.transaction_date,
                    note=payload.note,
                )
            )
            if ml.get("status") == "ready" and ml.get("predictedCategory"):
                conf = ml.get("confidence")
                if conf is None or float(conf) >= 0.2:
                    category = ml["predictedCategory"]
        except Exception:
            pass

    income_src = ""
    if transaction_type == "income":
        income_src = (payload.income_source or "").strip()[:80]

    with get_connection() as connection:
        row = connection.execute(
            """
            INSERT INTO transactions (user_id, title, category, tx_type, amount, tx_date, note, created_at, income_source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            RETURNING *
            """,
            (
                user_id,
                payload.title,
                category,
                transaction_type,
                payload.amount,
                payload.transaction_date,
                payload.note,
                datetime.utcnow().isoformat(),
                income_src,
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


def get_dashboard_payload(
    user_id: int,
    search: str = "",
    category: str = "all",
    date_from: str | None = None,
    date_to: str | None = None,
):
    from app.ml.training import ml_retrain_readiness

    df, dt = _normalize_analytics_range(date_from, date_to)
    rows = fetch_transactions_for_user(
        user_id,
        search=search,
        category=category,
        date_from=date_from,
        date_to=date_to,
    )
    return {
        "summaryCards": dashboard_summary_cards(user_id),
        "cashflow": dashboard_cashflow(user_id),
        "insight": dashboard_insight(user_id),
        "budgets": dashboard_budgets(user_id),
        "mlForecast": ml_forecast_summary(user_id),
        "mlRetrain": ml_retrain_readiness(),
        "transactions": [serialize_transaction(row) for row in rows[:20]],
        "listFilters": {"search": search, "category": category, "dateFrom": df, "dateTo": dt},
    }
