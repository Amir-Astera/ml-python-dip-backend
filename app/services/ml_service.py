from collections import defaultdict
from datetime import datetime

from app.db import get_connection
from app.ml.training import load_classifier, load_forecast_bundle, load_metrics, ml_retrain_readiness, train_ml_models
from app.services.finance_service import category_meta, currency


def ensure_ml_assets():
    return load_metrics()


def _amount_bucket(amount: float):
    if amount < 5000:
        return 'tiny'
    if amount < 15000:
        return 'small'
    if amount < 40000:
        return 'medium'
    if amount < 90000:
        return 'large'
    return 'xlarge'


def _classification_text(title: str, note: str, amount: float, transaction_date: str):
    try:
        month = datetime.strptime(transaction_date, '%Y-%m-%d').month
    except ValueError:
        month = 1
    return ' | '.join(
        [
            title.lower(),
            note.lower(),
            f'month_{month}',
            f'amount_{_amount_bucket(amount)}',
            'band_unknown',
            'profile_app_user',
        ]
    )


def _income_band_for_user(user_id: int):
    with get_connection() as connection:
        row = connection.execute(
            """
            SELECT AVG(monthly_income) AS avg_income
            FROM (
                SELECT substr(tx_date, 1, 7) AS month_key, SUM(amount) AS monthly_income
                FROM transactions
                WHERE user_id = ? AND tx_type = 'income'
                GROUP BY substr(tx_date, 1, 7)
            )
            """,
            (user_id,),
        ).fetchone()

    avg_income = row['avg_income'] if row and row['avg_income'] is not None else 0
    if avg_income < 250000:
        return 'low'
    if avg_income < 400000:
        return 'medium'
    if avg_income < 600000:
        return 'medium_high'
    return 'high'


def classify_expense_payload(payload):
    metrics = load_metrics()
    classifier = load_classifier()
    if classifier is None:
        return {
            'status': metrics.get('status', 'not_trained'),
            'message': metrics.get('message', 'ML-модель пока не обучена на реальных данных.'),
            'models': metrics.get('models', {}),
            'dataset': metrics.get('dataset', {}),
        }

    features = [
        _classification_text(
            payload.title,
            payload.note,
            payload.amount,
            payload.transaction_date,
        )
    ]
    predicted_category = classifier.predict(features)[0]
    confidence = None
    probabilities = []
    if hasattr(classifier, 'predict_proba'):
        proba = classifier.predict_proba(features)[0]
        labels = list(classifier.classes_)
        ranked = sorted(zip(labels, proba), key=lambda item: item[1], reverse=True)
        confidence = round(float(ranked[0][1]), 4)
        probabilities = [
            {'category': label, 'confidence': round(float(score), 4)} for label, score in ranked[:3]
        ]

    meta = category_meta(predicted_category)
    return {
        'status': 'ready',
        'predictedCategory': predicted_category,
        'confidence': confidence,
        'topPredictions': probabilities,
        'icon': meta['icon'],
        'color': meta['color'],
        'message': f'Модель относит операцию к категории «{predicted_category}».',
    }


def _user_monthly_expenses(user_id: int):
    with get_connection() as connection:
        rows = connection.execute(
            """
            SELECT
                category,
                substr(tx_date, 1, 7) AS month_key,
                SUM(amount) AS total_expense
            FROM transactions
            WHERE user_id = ? AND tx_type = 'expense'
            GROUP BY category, substr(tx_date, 1, 7)
            ORDER BY month_key ASC
            """,
            (user_id,),
        ).fetchall()
    return rows


def _user_budget_map(user_id: int):
    with get_connection() as connection:
        rows = connection.execute(
            'SELECT category, limit_amount FROM budgets WHERE user_id = ?',
            (user_id,),
        ).fetchall()
    return {row['category']: row['limit_amount'] for row in rows}


def _next_month_key(month_key: str):
    year, month = [int(value) for value in month_key.split('-')]
    month += 1
    if month > 12:
        month = 1
        year += 1
    return f'{year}-{month:02d}'


def _status_by_ratio(ratio: float):
    if ratio >= 1:
        return 'danger'
    if ratio >= 0.85:
        return 'warning'
    return 'safe'


def user_ml_overview_payload(user_id: int):
    metrics = load_metrics()
    monthly_rows = _user_monthly_expenses(user_id)
    budget_map = _user_budget_map(user_id)
    forecast_bundle = load_forecast_bundle()

    if not monthly_rows:
        return {
            'status': metrics.get('status', 'no_data'),
            'dataset': metrics.get('dataset', {}),
            'models': metrics.get('models', {}),
            'forecast': [],
            'insights': ['Недостаточно реальных расходов пользователя для прогноза.'],
            'message': 'Сначала добавьте реальные расходы пользователя.',
            'mlRetrain': ml_retrain_readiness(),
        }

    grouped = defaultdict(dict)
    ordered_months = sorted({row['month_key'] for row in monthly_rows})
    latest_month = ordered_months[-1]
    next_month = _next_month_key(latest_month)
    income_band = _income_band_for_user(user_id)

    for row in monthly_rows:
        grouped[row['category']][row['month_key']] = float(row['total_expense'])

    forecast_items = []
    if forecast_bundle is not None:
        vectorizer = forecast_bundle['vectorizer']
        model = forecast_bundle['model']

        for category, month_values in grouped.items():
            history = [month_values.get(month_key, 0.0) for month_key in ordered_months]
            previous_amount = history[-1]
            rolling_mean = sum(history[-3:]) / max(1, len(history[-3:]))
            feature = {
                'profile': 'app_user',
                'income_band': income_band,
                'category': category,
                'month': int(next_month.split('-')[1]),
                'year': int(next_month.split('-')[0]),
                'previous_amount': previous_amount,
                'rolling_mean': rolling_mean,
            }
            predicted_amount = max(0.0, float(model.predict(vectorizer.transform([feature]))[0]))
            limit_amount = budget_map.get(category)
            ratio = predicted_amount / limit_amount if limit_amount else 0
            meta = category_meta(category)
            forecast_items.append(
                {
                    'category': category,
                    'predictedAmount': round(predicted_amount, 2),
                    'predictedLabel': currency(predicted_amount),
                    'budgetLimit': limit_amount,
                    'budgetLabel': currency(limit_amount) if limit_amount else 'Лимит не задан',
                    'status': _status_by_ratio(ratio) if limit_amount else 'no_budget',
                    'percentOfBudget': int(round(ratio * 100)) if limit_amount else None,
                    'color': meta['color'],
                    'icon': meta['icon'],
                }
            )

    forecast_items.sort(key=lambda item: item['predictedAmount'], reverse=True)
    top_forecast = forecast_items[0]['category'] if forecast_items else 'нет данных'
    risk_items = [item for item in forecast_items if item['status'] in {'warning', 'danger'}]

    insights = [
        f'На следующий период модель ожидает самые высокие расходы в категории «{top_forecast}».',
        f'Для обучения использовано {metrics.get("dataset", {}).get("records", 0)} реальных записей транзакций.',
    ]
    if risk_items:
        insights.append(f'Есть {len(risk_items)} категории с риском приближения к лимиту бюджета.')
    else:
        insights.append('По текущему прогнозу критичных превышений бюджета не ожидается.')

    return {
        'status': metrics.get('status', 'not_trained'),
        'dataset': metrics.get('dataset', {}),
        'models': metrics.get('models', {}),
        'forecastMonth': next_month,
        'forecast': forecast_items,
        'insights': insights,
        'message': metrics.get('message', 'ML-аналитика подготовлена на реальных данных.'),
        'mlRetrain': ml_retrain_readiness(),
    }


def retrain_ml_payload():
    return train_ml_models(force=True)
