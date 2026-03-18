from collections import defaultdict
from json import dump, load
from pathlib import Path
import pickle

from app.ml.synthetic_data import DATA_DIR, DATASET_PATH, generate_synthetic_dataset, load_synthetic_rows

CLASSIFIER_PATH = DATA_DIR / 'expense_classifier.pkl'
FORECAST_PATH = DATA_DIR / 'expense_forecast.pkl'
METRICS_PATH = DATA_DIR / 'ml_metrics.json'


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
        key = (row['synthetic_user_id'], row['category'])
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
        'datasetPath': str(DATASET_PATH),
        'records': len(rows),
        'expenseRecords': len(expense_rows),
        'syntheticUsers': len({row['synthetic_user_id'] for row in rows}),
        'categories': sorted({row['category'] for row in expense_rows}),
    }


def _save_pickle(path: Path, payload):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with path.open('wb') as file:
        pickle.dump(payload, file)


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
    dataset_path = generate_synthetic_dataset()
    rows = load_synthetic_rows(dataset_path)
    stats = _dataset_stats(rows)
    deps = _safe_import_ml_dependencies()

    if deps is None:
        result = {
            'status': 'dependencies_missing',
            'dataset': stats,
            'message': 'Для обучения моделей нужно установить scikit-learn.',
        }
        _save_metrics(result)
        return result

    if not force and CLASSIFIER_PATH.exists() and FORECAST_PATH.exists() and METRICS_PATH.exists():
        cached = load_metrics()
        cached.setdefault('dataset', stats)
        return cached

    x_cls, y_cls = _build_classifier_dataset(rows)
    train_test_split = deps['train_test_split']
    accuracy_score = deps['accuracy_score']
    Pipeline = deps['Pipeline']
    TfidfVectorizer = deps['TfidfVectorizer']
    LogisticRegression = deps['LogisticRegression']

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

    x_forecast, y_forecast = _build_forecast_dataset(rows)
    x_train_fc, x_test_fc, y_train_fc, y_test_fc = train_test_split(
        x_forecast,
        y_forecast,
        test_size=0.2,
        random_state=42,
    )
    DictVectorizer = deps['DictVectorizer']
    RandomForestRegressor = deps['RandomForestRegressor']
    mean_absolute_error = deps['mean_absolute_error']
    r2_score = deps['r2_score']

    forecast_vectorizer = DictVectorizer(sparse=False)
    x_train_fc_vectorized = forecast_vectorizer.fit_transform(x_train_fc)
    x_test_fc_vectorized = forecast_vectorizer.transform(x_test_fc)
    forecast_model = RandomForestRegressor(n_estimators=180, random_state=42)
    forecast_model.fit(x_train_fc_vectorized, y_train_fc)
    fc_predictions = forecast_model.predict(x_test_fc_vectorized)
    forecast_mae = mean_absolute_error(y_test_fc, fc_predictions)
    forecast_r2 = r2_score(y_test_fc, fc_predictions)
    _save_pickle(FORECAST_PATH, {'vectorizer': forecast_vectorizer, 'model': forecast_model})

    metrics = {
        'status': 'ready',
        'dataset': stats,
        'models': {
            'classifier': {
                'algorithm': 'LogisticRegression + TF-IDF',
                'accuracy': round(float(classifier_accuracy), 4),
                'classes': sorted(set(y_cls)),
                'trainSamples': len(x_train_cls),
                'testSamples': len(x_test_cls),
            },
            'forecast': {
                'algorithm': 'RandomForestRegressor',
                'mae': round(float(forecast_mae), 2),
                'r2': round(float(forecast_r2), 4),
                'trainSamples': len(x_train_fc),
                'testSamples': len(x_test_fc),
            },
        },
        'message': 'ML-модели обучены на синтетическом датасете.',
    }
    _save_metrics(metrics)
    return metrics


def load_classifier():
    if not CLASSIFIER_PATH.exists():
        train_ml_models()
    if not CLASSIFIER_PATH.exists():
        return None
    return _load_pickle(CLASSIFIER_PATH)


def load_forecast_bundle():
    if not FORECAST_PATH.exists():
        train_ml_models()
    if not FORECAST_PATH.exists():
        return None
    return _load_pickle(FORECAST_PATH)
