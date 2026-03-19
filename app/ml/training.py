from collections import Counter, defaultdict
from json import dump, load
from pathlib import Path
import pickle

from app.db import get_connection
from app.ml.synthetic_data import DATA_DIR

CLASSIFIER_PATH = DATA_DIR / 'expense_classifier.pkl'
FORECAST_PATH = DATA_DIR / 'expense_forecast.pkl'
METRICS_PATH = DATA_DIR / 'ml_metrics.json'
REAL_DATASET_SOURCE = 'postgres_transactions'


def _safe_import_ml_dependencies():
    try:
        from sklearn.ensemble import RandomForestRegressor
        from sklearn.feature_extraction import DictVectorizer
        from sklearn.feature_extraction.text import TfidfVectorizer
        from sklearn.linear_model import LogisticRegression
        from sklearn.metrics import accuracy_score, mean_absolute_error, r2_score
        from sklearn.model_selection import train_test_split
        from sklearn.pipeline import Pipeline
    except ImportError:
        return None

    return {
        'RandomForestRegressor': RandomForestRegressor,
        'DictVectorizer': DictVectorizer,
        'TfidfVectorizer': TfidfVectorizer,
        'LogisticRegression': LogisticRegression,
        'accuracy_score': accuracy_score,
        'mean_absolute_error': mean_absolute_error,
        'r2_score': r2_score,
        'train_test_split': train_test_split,
        'Pipeline': Pipeline,
    }


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


def _income_band(avg_income: float):
    if avg_income < 250000:
        return 'low'
    if avg_income < 400000:
        return 'medium'
    if avg_income < 600000:
        return 'medium_high'
    return 'high'


def _load_real_rows():
    with get_connection() as connection:
        income_rows = connection.execute(
            """
            SELECT user_id, AVG(monthly_income) AS avg_income
            FROM (
                SELECT user_id, substr(tx_date, 1, 7) AS month_key, SUM(amount) AS monthly_income
                FROM transactions
                WHERE tx_type = 'income'
                GROUP BY user_id, substr(tx_date, 1, 7)
            ) income_by_month
            GROUP BY user_id
            """
        ).fetchall()
        transaction_rows = connection.execute(
            """
            SELECT id, user_id, title, COALESCE(note, '') AS note, category, tx_type, amount, tx_date
            FROM transactions
            ORDER BY user_id ASC, tx_date ASC, id ASC
            """
        ).fetchall()

    income_map = {
        row['user_id']: float(row['avg_income']) if row['avg_income'] is not None else 0.0 for row in income_rows
    }
    rows = []
    for row in transaction_rows:
        year = int(row['tx_date'][0:4])
        month = int(row['tx_date'][5:7])
        rows.append(
            {
                'user_id': row['user_id'],
                'profile': 'app_user',
                'income_band': _income_band(income_map.get(row['user_id'], 0.0)),
                'tx_type': row['tx_type'],
                'title': row['title'],
                'note': row['note'] or '',
                'category': row['category'],
                'amount': float(row['amount']),
                'tx_date': row['tx_date'],
                'year': year,
                'month': month,
            }
        )
    return rows


def _expense_text(row):
    return ' | '.join(
        [
            row['title'].lower(),
            row['note'].lower(),
            f"month_{row['month']}",
            f"amount_{_amount_bucket(float(row['amount']))}",
            f"band_{row['income_band']}",
            f"profile_{row['profile']}",
        ]
    )


def _build_classifier_dataset(rows):
    samples = []
    labels = []
    for row in rows:
        if row['tx_type'] != 'expense':
            continue
        samples.append(_expense_text(row))
        labels.append(row['category'])
    return samples, labels


def _sorted_month_keys(rows):
    keys = sorted({f"{row['year']}-{int(row['month']):02d}" for row in rows})
    return keys


def _build_forecast_dataset(rows):
    expense_rows = [row for row in rows if row['tx_type'] == 'expense']
    month_keys = _sorted_month_keys(expense_rows)
    grouped = defaultdict(lambda: defaultdict(float))
    profile_map = {}

    for row in expense_rows:
        key = (row['user_id'], row['category'])
        month_key = f"{row['year']}-{int(row['month']):02d}"
        grouped[key][month_key] += float(row['amount'])
        profile_map[key] = {
            'profile': row['profile'],
            'income_band': row['income_band'],
            'category': row['category'],
        }

    features = []
    targets = []

    for key, monthly_data in grouped.items():
        previous_amount = None
        rolling_values = []
        for month_key in month_keys:
            current_amount = monthly_data.get(month_key, 0.0)
            if previous_amount is not None:
                year, month = month_key.split('-')
                rolling_mean = sum(rolling_values[-3:]) / max(1, len(rolling_values[-3:]))
                payload = {
                    'profile': profile_map[key]['profile'],
                    'income_band': profile_map[key]['income_band'],
                    'category': profile_map[key]['category'],
                    'month': int(month),
                    'year': int(year),
                    'previous_amount': previous_amount,
                    'rolling_mean': rolling_mean,
                }
                features.append(payload)
                targets.append(current_amount)
            previous_amount = current_amount
            rolling_values.append(current_amount)

    return features, targets


def _dataset_stats(rows):
    expense_rows = [row for row in rows if row['tx_type'] == 'expense']
    return {
        'source': REAL_DATASET_SOURCE,
        'records': len(rows),
        'expenseRecords': len(expense_rows),
        'users': len({row['user_id'] for row in rows}),
        'categories': sorted({row['category'] for row in expense_rows}),
    }


def _save_pickle(path: Path, payload):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with path.open('wb') as file:
        pickle.dump(payload, file)


def _remove_if_exists(path: Path):
    if path.exists():
        path.unlink()


def _load_pickle(path: Path):
    with path.open('rb') as file:
        return pickle.load(file)


def _save_metrics(payload: dict):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with METRICS_PATH.open('w', encoding='utf-8') as file:
        dump(payload, file, ensure_ascii=False, indent=2)


def load_metrics():
    if not METRICS_PATH.exists():
        return {}
    with METRICS_PATH.open('r', encoding='utf-8') as file:
        return load(file)


def train_ml_models(force: bool = False):
    rows = _load_real_rows()
    stats = _dataset_stats(rows)
    deps = _safe_import_ml_dependencies()

    if deps is None:
        _remove_if_exists(CLASSIFIER_PATH)
        _remove_if_exists(FORECAST_PATH)
        result = {
            'status': 'dependencies_missing',
            'dataset': stats,
            'message': 'Для обучения моделей нужно установить scikit-learn.',
            'models': {},
        }
        _save_metrics(result)
        return result

    if not force and METRICS_PATH.exists():
        cached = load_metrics()
        if cached.get('dataset', {}).get('source') == REAL_DATASET_SOURCE:
            cached.setdefault('dataset', stats)
            return cached

    x_cls, y_cls = _build_classifier_dataset(rows)
    train_test_split = deps['train_test_split']
    accuracy_score = deps['accuracy_score']
    Pipeline = deps['Pipeline']
    TfidfVectorizer = deps['TfidfVectorizer']
    LogisticRegression = deps['LogisticRegression']
    classifier_metrics = {
        'status': 'insufficient_data',
        'message': 'Для обучения классификатора нужны реальные размеченные расходы минимум в 2 категориях.',
    }
    class_counts = Counter(y_cls)
    can_train_classifier = (
        len(x_cls) >= 12 and len(class_counts) >= 2 and class_counts and min(class_counts.values()) >= 2
    )

    if can_train_classifier:
        try:
            x_train_cls, x_test_cls, y_train_cls, y_test_cls = train_test_split(
                x_cls,
                y_cls,
                test_size=0.2,
                random_state=42,
                stratify=y_cls,
            )
            classifier = Pipeline(
                [
                    ('vectorizer', TfidfVectorizer(ngram_range=(1, 2))),
                    ('model', LogisticRegression(max_iter=1500)),
                ]
            )
            classifier.fit(x_train_cls, y_train_cls)
            cls_predictions = classifier.predict(x_test_cls)
            classifier_accuracy = accuracy_score(y_test_cls, cls_predictions)
            _save_pickle(CLASSIFIER_PATH, classifier)
            classifier_metrics = {
                'status': 'ready',
                'algorithm': 'LogisticRegression + TF-IDF',
                'accuracy': round(float(classifier_accuracy), 4),
                'classes': sorted(set(y_cls)),
                'trainSamples': len(x_train_cls),
                'testSamples': len(x_test_cls),
            }
        except ValueError:
            _remove_if_exists(CLASSIFIER_PATH)
    else:
        _remove_if_exists(CLASSIFIER_PATH)

    x_forecast, y_forecast = _build_forecast_dataset(rows)
    DictVectorizer = deps['DictVectorizer']
    RandomForestRegressor = deps['RandomForestRegressor']
    mean_absolute_error = deps['mean_absolute_error']
    r2_score = deps['r2_score']
    forecast_metrics = {
        'status': 'insufficient_data',
        'message': 'Для прогноза нужны реальные расходы минимум за несколько месяцев.',
    }

    if len(x_forecast) >= 8:
        try:
            x_train_fc, x_test_fc, y_train_fc, y_test_fc = train_test_split(
                x_forecast,
                y_forecast,
                test_size=0.2,
                random_state=42,
            )
            forecast_vectorizer = DictVectorizer(sparse=False)
            x_train_fc_vectorized = forecast_vectorizer.fit_transform(x_train_fc)
            x_test_fc_vectorized = forecast_vectorizer.transform(x_test_fc)
            forecast_model = RandomForestRegressor(n_estimators=180, random_state=42)
            forecast_model.fit(x_train_fc_vectorized, y_train_fc)
            fc_predictions = forecast_model.predict(x_test_fc_vectorized)
            forecast_mae = mean_absolute_error(y_test_fc, fc_predictions)
            forecast_r2 = r2_score(y_test_fc, fc_predictions)
            _save_pickle(FORECAST_PATH, {'vectorizer': forecast_vectorizer, 'model': forecast_model})
            forecast_metrics = {
                'status': 'ready',
                'algorithm': 'RandomForestRegressor',
                'mae': round(float(forecast_mae), 2),
                'r2': round(float(forecast_r2), 4),
                'trainSamples': len(x_train_fc),
                'testSamples': len(x_test_fc),
            }
        except ValueError:
            _remove_if_exists(FORECAST_PATH)
    else:
        _remove_if_exists(FORECAST_PATH)

    ready_models = sum(
        1 for item in (classifier_metrics, forecast_metrics) if item.get('status') == 'ready'
    )
    if ready_models == 2:
        status = 'ready'
        message = 'ML-модели обучены на реальных транзакциях пользователей.'
    elif ready_models == 1:
        status = 'partially_ready'
        message = 'Часть ML-моделей обучена на реальных данных. Для полной аналитики нужно больше истории операций.'
    else:
        status = 'insufficient_data'
        message = 'Для ML пока недостаточно реальных данных. Добавьте больше операций и затем запустите переобучение.'

    metrics = {
        'status': status,
        'dataset': stats,
        'models': {
            'classifier': classifier_metrics,
            'forecast': forecast_metrics,
        },
        'message': message,
    }
    _save_metrics(metrics)
    return metrics


def load_classifier():
    metrics = load_metrics()
    if metrics.get('dataset', {}).get('source') != REAL_DATASET_SOURCE:
        return None
    if not CLASSIFIER_PATH.exists():
        return None
    return _load_pickle(CLASSIFIER_PATH)


def load_forecast_bundle():
    metrics = load_metrics()
    if metrics.get('dataset', {}).get('source') != REAL_DATASET_SOURCE:
        return None
    if not FORECAST_PATH.exists():
        return None
    return _load_pickle(FORECAST_PATH)
