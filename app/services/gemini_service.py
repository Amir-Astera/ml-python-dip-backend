# Советы по цифрам через Gemini (если задан GEMINI_API_KEY). Без ключа — вернёт None.
import os


def _model_name():
    return os.getenv("GEMINI_MODEL", "gemini-2.0-flash").strip() or "gemini-2.0-flash"


def gemini_text_ru(prompt: str, max_len: int = 1200) -> str | None:
    key = os.getenv("GEMINI_API_KEY", "").strip()
    if not key:
        return None
    try:
        from google import genai

        client = genai.Client(api_key=key)
        resp = client.models.generate_content(model=_model_name(), contents=prompt)
        text = (getattr(resp, "text", None) or "").strip()
        return text[:max_len] if text else None
    except Exception:
        return None


def gemini_dashboard_advice(facts_ru: str) -> str | None:
    prompt = (
        "Ты финансовый помощник для студенческого приложения учёта личных финансов.\n"
        "По фактам ниже дай один связный совет на русском: 2–4 предложения, без Markdown и без нумерации.\n\n"
        + facts_ru
    )
    return gemini_text_ru(prompt, max_len=900)


def gemini_analytics_bullets(facts_ru: str) -> list[str] | None:
    prompt = (
        "Ты финансовый помощник. По данным ниже дай ровно 3 коротких совета на русском.\n"
        "Каждый совет — отдельная строка. Без Markdown, без нумерации, без пустых строк.\n\n"
        + facts_ru
    )
    raw = gemini_text_ru(prompt, max_len=1200)
    if not raw:
        return None
    lines = [ln.strip(" -•\t") for ln in raw.splitlines() if ln.strip()]
    if len(lines) >= 3:
        return lines[:3]
    parts = [p.strip() for p in raw.replace(";", ".").split(".") if len(p.strip()) > 15]
    if len(parts) >= 3:
        return parts[:3]
    return None
