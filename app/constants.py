CATEGORY_META = {
    "Income": {"icon": "$", "iconColor": "var(--accent-green)", "iconBackground": "var(--accent-green-light)", "color": "var(--accent-green)"},
    "Housing": {"icon": "H", "iconColor": "var(--accent-cyan)", "iconBackground": "var(--bg-body)", "color": "var(--accent-cyan)"},
    "Groceries": {"icon": "G", "iconColor": "var(--accent-orange)", "iconBackground": "var(--bg-body)", "color": "var(--accent-orange)"},
    "Transport": {"icon": "T", "iconColor": "var(--accent-dashboard-blue)", "iconBackground": "var(--bg-body)", "color": "var(--accent-dashboard-blue)"},
    "Entertainment": {"icon": "E", "iconColor": "var(--accent-pink)", "iconBackground": "var(--bg-body)", "color": "var(--accent-pink)"},
    "Utilities": {"icon": "U", "iconColor": "var(--accent-purple)", "iconBackground": "var(--bg-body)", "color": "var(--accent-purple)"},
    "Health": {"icon": "+", "iconColor": "var(--accent-red)", "iconBackground": "var(--bg-body)", "color": "var(--accent-red)"},
    "Education": {"icon": "Ed", "iconColor": "var(--accent-blue)", "iconBackground": "var(--bg-body)", "color": "var(--accent-blue)"},
    "Shopping": {"icon": "S", "iconColor": "var(--accent-pink)", "iconBackground": "var(--bg-body)", "color": "var(--accent-pink)"},
}

# Подсказки для поля «источник дохода» (доходы)
DEFAULT_INCOME_SOURCE_NAMES = (
    "Зарплата",
    "Премия",
    "Фриланс",
    "Подработка",
    "Перевод",
    "Другое",
)

DEFAULT_BUDGETS = [
    ("Housing", 180000),
    ("Groceries", 85000),
    ("Transport", 40000),
    ("Entertainment", 50000),
    ("Utilities", 35000),
]

# последний элемент — источник дохода (для расходов пустая строка)
DEFAULT_TRANSACTIONS = [
    ("TechCorp Salary", "Income", "income", 390000, "2026-03-15", "Основной доход", "Зарплата"),
    ("Apartment Rent", "Housing", "expense", 145000, "2026-03-18", "Ежемесячная аренда", ""),
    ("Magnum Supermarket", "Groceries", "expense", 28200, "2026-03-23", "Продукты", ""),
    ("TechCorp Salary", "Income", "income", 395000, "2026-04-15", "Основной доход", "Зарплата"),
    ("Beeline Home Internet", "Utilities", "expense", 9600, "2026-04-17", "Интернет", ""),
    ("TechCorp Salary", "Income", "income", 401000, "2026-05-15", "Основной доход", "Зарплата"),
    ("Yandex Taxi", "Transport", "expense", 14500, "2026-05-22", "Поездки по городу", ""),
    ("TechCorp Salary", "Income", "income", 408000, "2026-06-15", "Основной доход", "Зарплата"),
    ("Magnum Supermarket", "Groceries", "expense", 32450, "2026-06-19", "Продукты", ""),
    ("Freelance Project", "Income", "income", 72000, "2026-07-10", "Дополнительный доход", "Фриланс"),
    ("Cinema City", "Entertainment", "expense", 26400, "2026-07-24", "Отдых", ""),
    ("TechCorp Salary", "Income", "income", 412000, "2026-08-15", "Основной доход", "Зарплата"),
    ("Apartment Rent", "Housing", "expense", 145000, "2026-08-18", "Ежемесячная аренда", ""),
    ("Beeline Home Internet", "Utilities", "expense", 9650, "2026-08-12", "Интернет", ""),
    ("Yandex Plus", "Entertainment", "expense", 2999, "2026-08-23", "Подписка", ""),
    ("Magnum Supermarket", "Groceries", "expense", 18420, "2026-08-24", "Продукты", ""),
]

MONTH_LABELS = {
    "01": "Jan",
    "02": "Feb",
    "03": "Mar",
    "04": "Apr",
    "05": "May",
    "06": "Jun",
    "07": "Jul",
    "08": "Aug",
    "09": "Sep",
    "10": "Oct",
    "11": "Nov",
    "12": "Dec",
}
