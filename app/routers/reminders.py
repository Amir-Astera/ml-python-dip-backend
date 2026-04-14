# Напоминания о датах (день рождения, платёж и т.д.)
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from app.db import get_connection
from app.dependencies import get_current_user

router = APIRouter(tags=["reminders"])


class ReminderBody(BaseModel):
    title: str = Field(min_length=1, max_length=120)
    event_date: str = Field(min_length=10, max_length=10)
    note: str = Field(default="", max_length=300)


@router.get("/reminders")
def get_reminders(user=Depends(get_current_user)):
    uid = user["id"]
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM reminders WHERE user_id = ? ORDER BY event_date ASC, id ASC",
            (uid,),
        ).fetchall()
    items = []
    for r in rows:
        items.append(
            {
                "id": r["id"],
                "title": r["title"],
                "eventDate": r["event_date"],
                "note": r["note"] or "",
            }
        )
    return {"items": items}


@router.post("/reminders")
def create_reminder(body: ReminderBody, user=Depends(get_current_user)):
    try:
        datetime.strptime(body.event_date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Дата события: ГГГГ-ММ-ДД") from None

    uid = user["id"]
    created = datetime.utcnow().isoformat()
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO reminders (user_id, title, event_date, note, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (uid, body.title.strip(), body.event_date, body.note.strip(), created),
        )
        conn.commit()
    return get_reminders(user)


@router.delete("/reminders/{reminder_id}")
def delete_reminder(reminder_id: int, user=Depends(get_current_user)):
    uid = user["id"]
    with get_connection() as conn:
        row = conn.execute(
            "SELECT id FROM reminders WHERE id = ? AND user_id = ?",
            (reminder_id, uid),
        ).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Не найдено")
        conn.execute("DELETE FROM reminders WHERE id = ? AND user_id = ?", (reminder_id, uid))
        conn.commit()
    return {"message": "Удалено"}
