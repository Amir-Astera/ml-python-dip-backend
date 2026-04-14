import statistics
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


def _classification_text(title: str, note: str, amount: float, transaction_date: str, income_band: str = 'band_unknown'):
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
            f'band_{income_band}',
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


def classify_expense_payload(payload, user_id: int | None = None):
    metrics = load_metrics()
    classifier = load_classifier()
    if classifier is None:
        return {
            'status': metrics.get('status', 'not_trained'),
            'message': metrics.get('message', 'ML-модель пока не обучена на реальных данных.'),
            'models': metrics.get('models', {}),
            'dataset': metrics.get('dataset', {}),
        }

    band = _income_band_for_user(user_id) if user_id is not None else 'band_unknown'
    features = [
        _classification_text(
            payload.title,
            payload.note,
            payload.amount,
            payload.transaction_date,
            band,
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


def _detect_patterns(user_id: int) -> dict:
    """Detect recurring payments, anomalies, and seasonal cycles for a user."""
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT title, category, amount, tx_date
            FROM transactions
            WHERE user_id = ? AND tx_type = 'expense'
            ORDER BY tx_date ASC
            """,
            (user_id,),
        ).fetchall()

    if not rows:
        return {'recurring': [], 'anomalies': [], 'seasonal': [], 'summary': 'Нет данных для анализа паттернов.'}

    # ---- 1. Recurring payments ----
    # Group by normalized title+category, check if appears in 3+ different months
    title_months: dict = defaultdict(set)
    title_amounts: dict = defaultdict(list)
    title_category: dict = {}
    for r in rows:
        key = (r['title'].strip().lower(), r['category'])
        month_key = r['tx_date'][:7]
        title_months[key].add(month_key)
        title_amounts[key].append(float(r['amount']))
        title_category[key] = r['category']

    recurring = []
    for key, months in title_months.items():
        if len(months) >= 3:
            amounts = title_amounts[key]
            avg_amt = statistics.mean(amounts)
            # Check stability: std/mean < 0.25
            if len(amounts) > 1:
                try:
                    cv = statistics.stdev(amounts) / avg_amt if avg_amt > 0 else 1
                except Exception:
                    cv = 1
            else:
                cv = 0
            if cv < 0.3:
                recurring.append({
                    'title': key[0].title(),
                    'category': key[1],
                    'avgAmount': round(avg_amt, 2),
                    'avgLabel': currency(avg_amt),
                    'monthsCount': len(months),
                    'note': f'Повторяется {len(months)} месяцев подряд',
                })
    recurring.sort(key=lambda x: x['avgAmount'], reverse=True)
    recurring = recurring[:6]

    # ---- 2. Anomalies (per category: amount > mean + 2*std) ----
    cat_amounts: dict = defaultdict(list)
    for r in rows:
        cat_amounts[r['category']].append((float(r['amount']), r['title'], r['tx_date']))

    anomalies = []
    for cat, entries in cat_amounts.items():
        if len(entries) < 4:
            continue
        vals = [e[0] for e in entries]
        mean_val = statistics.mean(vals)
        try:
            std_val = statistics.stdev(vals)
        except Exception:
            continue
        threshold = mean_val + 2 * std_val
        for amt, title, tx_date in entries:
            if amt > threshold and amt > mean_val * 1.5:
                anomalies.append({
                    'title': title,
                    'category': cat,
                    'amount': round(amt, 2),
                    'amountLabel': currency(amt),
                    'date': tx_date,
                    'categoryMean': round(mean_val, 2),
                    'categoryMeanLabel': currency(mean_val),
                    'note': f'В {round(amt / mean_val, 1)}x раз выше среднего по категории',
                })
    anomalies.sort(key=lambda x: x['amount'], reverse=True)
    anomalies = anomalies[:5]

    # ---- 3. Seasonal cycles (which months are highest per category) ----
    cat_month_totals: dict = defaultdict(lambda: defaultdict(float))
    for r in rows:
        month_num = int(r['tx_date'][5:7])
        cat_month_totals[r['category']][month_num] += float(r['amount'])

    MONTH_RU = {
        1: 'Январь', 2: 'Февраль', 3: 'Март', 4: 'Апрель',
        5: 'Май', 6: 'Июнь', 7: 'Июль', 8: 'Август',
        9: 'Сентябрь', 10: 'Октябрь', 11: 'Ноябрь', 12: 'Декабрь',
    }
    seasonal = []
    for cat, month_data in cat_month_totals.items():
        if len(month_data) < 3:
            continue
        peak_month = max(month_data, key=lambda m: month_data[m])
        peak_val = month_data[peak_month]
        avg_val = statistics.mean(month_data.values())
        if peak_val > avg_val * 1.3:
            seasonal.append({
                'category': cat,
                'peakMonth': MONTH_RU.get(peak_month, str(peak_month)),
                'peakAmount': round(peak_val, 2),
                'peakLabel': currency(peak_val),
                'avgLabel': currency(avg_val),
                'note': f'Пик расходов в {MONTH_RU.get(peak_month, str(peak_month))} — в {round(peak_val / avg_val, 1)}x выше среднего',
            })
    seasonal.sort(key=lambda x: x['peakAmount'], reverse=True)
    seasonal = seasonal[:5]

    total_found = len(recurring) + len(anomalies) + len(seasonal)
    if total_found == 0:
        summary = 'Паттерны не обнаружены — добавьте больше операций за несколько месяцев.'
    else:
        parts = []
        if recurring:
            parts.append(f'{len(recurring)} регулярных платежей')
        if anomalies:
            parts.append(f'{len(anomalies)} аномальных трат')
        if seasonal:
            parts.append(f'{len(seasonal)} сезонных паттернов')
        summary = 'Обнаружено: ' + ', '.join(parts) + '.'

    return {
        'recurring': recurring,
        'anomalies': anomalies,
        'seasonal': seasonal,
        'summary': summary,
    }


def patterns_payload(user_id: int) -> dict:
    return _detect_patterns(user_id)


def interpret_ml_payload(user_id: int) -> dict:
    """Return human-readable interpretation of ML models: feature importance,
    model quality assessment, and per-user forecast explanation."""
    from app.ml.training import load_classifier, load_forecast_bundle, load_metrics

    metrics = load_metrics()
    classifier = load_classifier()
    forecast_bundle = load_forecast_bundle()

    # ── 1. Classifier interpretation ──────────────────────────────────────
    classifier_interp = []
    if classifier is not None:
        try:
            vectorizer = classifier.named_steps['vectorizer']
            model = classifier.named_steps['model']
            feature_names = vectorizer.get_feature_names_out()
            # LogisticRegression coef_ shape: (n_classes, n_features)
            classes = list(model.classes_)
            coef = model.coef_
            top_features_per_class = []
            for i, cls in enumerate(classes):
                top_idx = coef[i].argsort()[-5:][::-1]
                top_words = [feature_names[j] for j in top_idx]
                top_features_per_class.append({
                    'category': cls,
                    'topWords': top_words,
                })
            classifier_interp = top_features_per_class
        except Exception:
            classifier_interp = []

    # ── 2. Forecast model feature importance ──────────────────────────────
    forecast_interp = []
    if forecast_bundle is not None:
        try:
            vec = forecast_bundle['vectorizer']
            model = forecast_bundle['model']
            feature_names = vec.get_feature_names_out()
            importances = model.feature_importances_
            top_idx = importances.argsort()[-8:][::-1]
            forecast_interp = [
                {
                    'feature': feature_names[i],
                    'importance': round(float(importances[i]), 4),
                    'importancePct': round(float(importances[i]) * 100, 1),
                }
                for i in top_idx
            ]
        except Exception:
            forecast_interp = []

    # ── 3. Model quality assessment ───────────────────────────────────────
    cls_metrics = metrics.get('models', {}).get('classifier', {})
    fc_metrics = metrics.get('models', {}).get('forecast', {})
    accuracy = cls_metrics.get('accuracy')
    mae = fc_metrics.get('mae')
    r2 = fc_metrics.get('r2')

    quality_notes = []
    if accuracy is not None:
        pct = round(accuracy * 100)
        if pct >= 85:
            quality_notes.append(f'Классификатор работает отлично — точность {pct}%. Категории определяются корректно для подавляющего большинства операций.')
        elif pct >= 70:
            quality_notes.append(f'Классификатор работает хорошо — точность {pct}%. Часть операций может требовать ручной корректировки.')
        else:
            quality_notes.append(f'Точность классификатора {pct}% — модели нужно больше размеченных данных. Добавьте больше расходов по разным категориям.')

    if r2 is not None:
        if r2 >= 0.7:
            quality_notes.append(f'Модель прогноза объясняет {round(r2 * 100)}% вариации расходов (R²={r2}) — прогноз надёжен.')
        elif r2 >= 0.4:
            quality_notes.append(f'Модель прогноза умеренно точна (R²={r2}). Прогноз даёт ориентир, но возможны отклонения.')
        else:
            quality_notes.append(f'Модель прогноза пока имеет низкий R²={r2}. Нужно больше данных за несколько месяцев.')

    if mae is not None:
        quality_notes.append(f'Средняя ошибка прогноза (MAE) составляет {int(mae):,} ₸ — настолько модель может ошибаться по каждой категории.')

    # ── 4. Per-user forecast explanation ──────────────────────────────────
    monthly_rows = _user_monthly_expenses(user_id)
    budget_map = _user_budget_map(user_id)
    forecast_explanations = []

    if forecast_bundle is not None and monthly_rows:
        grouped = defaultdict(dict)
        ordered_months = sorted({row['month_key'] for row in monthly_rows})
        next_month = _next_month_key(ordered_months[-1])
        income_band = _income_band_for_user(user_id)
        vec = forecast_bundle['vectorizer']
        model = forecast_bundle['model']

        for row in monthly_rows:
            grouped[row['category']][row['month_key']] = float(row['total_expense'])

        for category, month_values in grouped.items():
            history = [month_values.get(mk, 0.0) for mk in ordered_months]
            if len(history) < 2:
                continue
            prev = history[-1]
            rolling = sum(history[-3:]) / max(1, len(history[-3:]))
            trend = prev - history[-2] if len(history) >= 2 else 0.0
            feature = {
                'profile': 'app_user',
                'income_band': income_band,
                'category': category,
                'month': int(next_month.split('-')[1]),
                'year': int(next_month.split('-')[0]),
                'previous_amount': prev,
                'rolling_mean': rolling,
            }
            predicted = max(0.0, float(model.predict(vec.transform([feature]))[0]))
            limit = budget_map.get(category)

            # Human-readable trend explanation
            if trend > prev * 0.15:
                trend_note = f'расходы растут (+{currency(trend)} к прошлому месяцу)'
            elif trend < -prev * 0.15:
                trend_note = f'расходы снижаются ({currency(trend)} к прошлому месяцу)'
            else:
                trend_note = 'расходы стабильны'

            explanation = (
                f'Прогноз {currency(predicted)} основан на среднем за 3 месяца '
                f'({currency(rolling)}) и прошлом месяце ({currency(prev)}). {trend_note.capitalize()}.'
            )
            if limit:
                ratio = predicted / limit
                if ratio >= 1.0:
                    explanation += f' Прогноз превышает установленный лимит {currency(limit)} — рекомендуется скорректировать бюджет.'
                elif ratio >= 0.85:
                    explanation += f' Расходы приближаются к лимиту {currency(limit)}.'

            forecast_explanations.append({
                'category': category,
                'explanation': explanation,
                'predictedLabel': currency(predicted),
                'prevLabel': currency(prev),
                'rollingLabel': currency(rolling),
            })

        forecast_explanations.sort(key=lambda x: x['predictedLabel'], reverse=False)

    # ── 5. Gemini narrative (if key set) ──────────────────────────────────
    gemini_narrative = None
    if quality_notes and forecast_explanations:
        try:
            from app.services.gemini_service import gemini_text_ru
            top3 = ', '.join(
                f"{e['category']} ({e['predictedLabel']})" for e in forecast_explanations[-3:]
            )
            prompt = (
                'Ты финансовый аналитик. Кратко (3–4 предложения, без Markdown) объясни пользователю '
                'результаты ML-анализа его финансов на русском языке.\n'
                f'Точность классификатора: {round(accuracy * 100) if accuracy else "нет"}%. '
                f'MAE прогноза: {int(mae) if mae else "нет"} тенге. '
                f'Топ прогнозируемых категорий: {top3}.'
            )
            gemini_narrative = gemini_text_ru(prompt, max_len=600)
        except Exception:
            gemini_narrative = None

    return {
        'classifierFeatures': classifier_interp,
        'forecastImportance': forecast_interp,
        'qualityNotes': quality_notes,
        'forecastExplanations': forecast_explanations,
        'geminiNarrative': gemini_narrative,
        'status': metrics.get('status', 'not_trained'),
    }
