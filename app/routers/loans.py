# Простой CRUD по займам — логика рядом с роутами, без отдельного сервиса
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from app.db import get_connection
from app.dependencies import get_current_user
from app.services.finance_service import currency

router = APIRouter(tags=["loans"])


class LoanBody(BaseModel):
    title: str = Field(min_length=1, max_length=120)
    total_amount: float = Field(gt=0)
    remaining_amount: float | None = None
    payment_per_month: float | None = None
    next_payment_date: str = ""
    note: str = Field(default="", max_length=300)


def _format_loan_row(row):
    pay = row["payment_per_month"]
    return {
        "id": row["id"],
        "title": row["title"],
        "totalLabel": currency(row["total_amount"]),
        "remainingLabel": currency(row["remaining_amount"]),
        "paymentLabel": currency(pay) if pay is not None else "—",
        "nextPaymentDate": row["next_payment_date"] or "",
        "note": row["note"] or "",
    }


@router.get("/loans")
def get_loans(user=Depends(get_current_user)):
    uid = user["id"]
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM loans WHERE user_id = ? ORDER BY id DESC",
            (uid,),
        ).fetchall()
    return {"items": [_format_loan_row(r) for r in rows]}


@router.post("/loans")
def create_loan(body: LoanBody, user=Depends(get_current_user)):
    uid = user["id"]
    remaining = body.total_amount if body.remaining_amount is None else body.remaining_amount
    if remaining > body.total_amount:
        raise HTTPException(status_code=400, detail="Остаток не больше суммы займа")
    if remaining < 0:
        raise HTTPException(status_code=400, detail="Остаток не может быть отрицательным")

    nd = body.next_payment_date.strip()
    if nd:
        try:
            datetime.strptime(nd, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="Дата платежа в формате ГГГГ-ММ-ДД") from None

    created = datetime.utcnow().isoformat()
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO loans (user_id, title, total_amount, remaining_amount, payment_per_month, next_payment_date, note, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                uid,
                body.title.strip(),
                body.total_amount,
                remaining,
                body.payment_per_month,
                nd,
                body.note.strip(),
                created,
            ),
        )
        conn.commit()
    return get_loans(user)


@router.delete("/loans/{loan_id}")
def delete_loan(loan_id: int, user=Depends(get_current_user)):
    uid = user["id"]
    with get_connection() as conn:
        row = conn.execute(
            "SELECT id FROM loans WHERE id = ? AND user_id = ?",
            (loan_id, uid),
        ).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Запись не найдена")
        conn.execute("DELETE FROM loans WHERE id = ? AND user_id = ?", (loan_id, uid))
        conn.commit()
    return {"message": "Удалено"}
