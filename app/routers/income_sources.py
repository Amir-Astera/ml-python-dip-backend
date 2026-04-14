from fastapi import APIRouter, Depends

from app.constants import DEFAULT_INCOME_SOURCE_NAMES
from app.db import get_connection
from app.dependencies import get_current_user

router = APIRouter(tags=["income_sources"])


@router.get("/income-sources")
def list_income_sources(user=Depends(get_current_user)):
    uid = user["id"]
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT TRIM(income_source) AS src
            FROM transactions
            WHERE user_id = ? AND tx_type = 'income' AND TRIM(COALESCE(income_source, '')) <> ''
            """,
            (uid,),
        ).fetchall()
    used = {r["src"] for r in rows if r.get("src")}
    names = sorted(set(DEFAULT_INCOME_SOURCE_NAMES) | used)
    return {"names": names}
