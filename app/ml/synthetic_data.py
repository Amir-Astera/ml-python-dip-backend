from csv import DictReader, DictWriter
from datetime import date
from pathlib import Path
from random import Random

DATA_DIR = Path(__file__).resolve().parent / 'data'
DATASET_PATH = DATA_DIR / 'synthetic_transactions.csv'

EXPENSE_PATTERNS = {
    'Housing': {
        'titles': ['Apartment Rent', 'Utility Bill', 'Home Repairs', 'Condominium Fee'],
        'notes': ['monthly rent', 'home payment', 'utility costs', 'housing needs'],
        'amount_range': (70000, 220000),
        'weight': 1.0,
        'seasonality': {12: 1.08, 1: 1.05, 6: 0.98, 7: 0.98},
    },
    'Groceries': {
        'titles': ['Magnum Supermarket', 'Small Grocery Store', 'Green Bazaar', 'Food Basket'],
        'notes': ['food shopping', 'weekly products', 'family groceries', 'kitchen supplies'],
        'amount_range': (6000, 38000),
        'weight': 1.35,
        'seasonality': {9: 1.08, 12: 1.22, 1: 1.14},
    },
    'Transport': {
        'titles': ['Yandex Taxi', 'Bus Card', 'Fuel Station', 'Parking Fee'],
        'notes': ['commute to work', 'fuel for car', 'city transport', 'daily transport'],
        'amount_range': (1500, 26000),
        'weight': 1.1,
        'seasonality': {1: 0.92, 7: 1.08, 8: 1.05},
    },
    'Entertainment': {
        'titles': ['Cinema City', 'Cafe Visit', 'Streaming Subscription', 'Weekend Fun'],
        'notes': ['rest and fun', 'friends outing', 'movie night', 'subscription payment'],
        'amount_range': (2000, 32000),
        'weight': 0.8,
        'seasonality': {6: 1.14, 7: 1.2, 12: 1.26},
    },
    'Utilities': {
        'titles': ['Beeline Internet', 'Electricity Payment', 'Water Bill', 'Mobile Plan'],
        'notes': ['internet payment', 'utility payment', 'phone bill', 'home services'],
        'amount_range': (3500, 24000),
        'weight': 0.9,
        'seasonality': {1: 1.18, 2: 1.15, 12: 1.1},
    },
    'Health': {
        'titles': ['Pharmacy Purchase', 'Clinic Visit', 'Medical Tests', 'Dentist Payment'],
        'notes': ['medicine', 'health check', 'doctor appointment', 'treatment costs'],
        'amount_range': (2500, 45000),
        'weight': 0.45,
        'seasonality': {2: 1.05, 10: 1.08, 11: 1.1},
    },
    'Education': {
        'titles': ['Online Course', 'Books Purchase', 'Tuition Payment', 'Language Lessons'],
        'notes': ['study costs', 'education payment', 'learning materials', 'course fee'],
        'amount_range': (4000, 60000),
        'weight': 0.35,
        'seasonality': {1: 1.05, 2: 1.04, 9: 1.28},
    },
    'Shopping': {
        'titles': ['Clothing Store', 'Market Purchase', 'Household Goods', 'Online Order'],
        'notes': ['new clothes', 'home shopping', 'essential purchase', 'online marketplace'],
        'amount_range': (3500, 42000),
        'weight': 0.75,
        'seasonality': {3: 1.08, 11: 1.18, 12: 1.24},
    },
}

INCOME_PATTERNS = [
    ('Tech Salary', 'monthly salary'),
    ('Freelance Project', 'freelance income'),
    ('Bonus Payment', 'work bonus'),
    ('Family Transfer', 'transfer from family'),
]

USER_PROFILES = [
    {'label': 'student', 'income_band': 'low', 'income_range': (180000, 260000), 'expense_scale': 0.78},
    {'label': 'junior_specialist', 'income_band': 'medium', 'income_range': (260000, 380000), 'expense_scale': 0.9},
    {'label': 'family_worker', 'income_band': 'medium_high', 'income_range': (380000, 560000), 'expense_scale': 1.08},
    {'label': 'senior_specialist', 'income_band': 'high', 'income_range': (560000, 820000), 'expense_scale': 1.22},
]


def _month_iter(start_year: int = 2023, start_month: int = 1, months: int = 24):
    result = []
    year = start_year
    month = start_month
    for _ in range(months):
        result.append((year, month))
        month += 1
        if month > 12:
            month = 1
            year += 1
    return result


def _season_multiplier(pattern: dict, month: int):
    return pattern.get('seasonality', {}).get(month, 1.0)


def _random_amount(rng: Random, low: int, high: int, multiplier: float):
    value = rng.randint(low, high)
    return round(value * multiplier, 2)


def generate_synthetic_dataset(path: Path = DATASET_PATH, users_count: int = 80, months_count: int = 24, seed: int = 42):
    if path.exists():
        return path

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    rng = Random(seed)
    fieldnames = [
        'synthetic_user_id',
        'profile',
        'income_band',
        'tx_type',
        'title',
        'note',
        'category',
        'amount',
        'tx_date',
        'year',
        'month',
    ]

    months = _month_iter(months=months_count)
    rows = []

    for synthetic_user_id in range(1, users_count + 1):
        profile = rng.choice(USER_PROFILES)
        base_income = rng.randint(*profile['income_range'])

        for year, month in months:
            monthly_income = round(base_income * rng.uniform(0.94, 1.08), 2)
            salary_title, salary_note = rng.choice(INCOME_PATTERNS[:2])
            rows.append(
                {
                    'synthetic_user_id': synthetic_user_id,
                    'profile': profile['label'],
                    'income_band': profile['income_band'],
                    'tx_type': 'income',
                    'title': salary_title,
                    'note': salary_note,
                    'category': 'Income',
                    'amount': monthly_income,
                    'tx_date': date(year, month, rng.randint(3, 18)).isoformat(),
                    'year': year,
                    'month': month,
                }
            )

            if rng.random() > 0.55:
                extra_title, extra_note = rng.choice(INCOME_PATTERNS[1:])
                rows.append(
                    {
                        'synthetic_user_id': synthetic_user_id,
                        'profile': profile['label'],
                        'income_band': profile['income_band'],
                        'tx_type': 'income',
                        'title': extra_title,
                        'note': extra_note,
                        'category': 'Income',
                        'amount': round(monthly_income * rng.uniform(0.08, 0.24), 2),
                        'tx_date': date(year, month, rng.randint(18, 27)).isoformat(),
                        'year': year,
                        'month': month,
                    }
                )

            for category, pattern in EXPENSE_PATTERNS.items():
                tx_count = max(1, int(round(pattern['weight'] * rng.uniform(1, 3))))
                if category in {'Health', 'Education'} and rng.random() > 0.55:
                    tx_count = 1
                if category == 'Housing':
                    tx_count = 1

                for _ in range(tx_count):
                    title = rng.choice(pattern['titles'])
                    note = rng.choice(pattern['notes'])
                    category_factor = _season_multiplier(pattern, month)
                    amount = _random_amount(
                        rng,
                        pattern['amount_range'][0],
                        pattern['amount_range'][1],
                        profile['expense_scale'] * category_factor * rng.uniform(0.88, 1.12),
                    )
                    rows.append(
                        {
                            'synthetic_user_id': synthetic_user_id,
                            'profile': profile['label'],
                            'income_band': profile['income_band'],
                            'tx_type': 'expense',
                            'title': title,
                            'note': note,
                            'category': category,
                            'amount': amount,
                            'tx_date': date(year, month, rng.randint(1, 28)).isoformat(),
                            'year': year,
                            'month': month,
                        }
                    )

    with path.open('w', newline='', encoding='utf-8') as file:
        writer = DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    return path


def load_synthetic_rows(path: Path = DATASET_PATH):
    dataset_path = generate_synthetic_dataset(path=path)
    with dataset_path.open('r', encoding='utf-8') as file:
        return list(DictReader(file))
