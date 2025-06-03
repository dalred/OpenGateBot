import sqlite3
import time
from typing import Optional


def get_db_connection(
    retries: int = 3, delay: float = 0.5
) -> Optional[sqlite3.Connection]:
    for attempt in range(1, retries + 1):
        try:
            conn = sqlite3.connect("access.db")
            conn.row_factory = sqlite3.Row
            return conn
        except sqlite3.Error as e:
            print(f"[DB Error] Attempt {attempt}/{retries}: {e}")
            if attempt < retries:
                time.sleep(delay)
    return None


def get_access_time_for_user(user_id: str) -> Optional[str]:
    conn = get_db_connection()
    if not conn:
        return None
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT access_time FROM access_control WHERE user_id = ?", (str(user_id),)
        )
        row = cursor.fetchone()
        if row and row["access_time"]:
            return row["access_time"].strip().lower()
        return None
    except sqlite3.Error as e:
        print(f"[DB access_time error] {e}")
        return None
    finally:
        conn.close()


def get_user_aprove_status(user_id: str) -> Optional[str]:
    conn = get_db_connection()
    if not conn:
        return None
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT aprove FROM access_control WHERE user_id = ?", (str(user_id),)
        )
        row = cursor.fetchone()
        if row and row["aprove"]:
            return row["aprove"].strip().lower()
        return None
    except sqlite3.Error as e:
        print(f"[DB aprove error] {e}")
        return None
    finally:
        conn.close()


def update_user_phone(user_id: str, new_phone: str) -> str:
    conn = get_db_connection()
    if not conn:
        return "error"
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT phone FROM access_control WHERE user_id = ?", (user_id,))
        row = cursor.fetchone()
        if not row:
            return "not_found"
        if new_phone == (row["phone"] or ""):
            return "same"
        cursor.execute(
            "UPDATE access_control SET phone = ?, aprove = 'pending' WHERE user_id = ?",
            (new_phone, user_id),
        )
        conn.commit()
        return "updated"
    except sqlite3.Error as e:
        print(f"[DB update_user_phone error] {e}")
        return "error"
    finally:
        conn.close()


def insert_new_user(
    user_id, username, fio, phone, aprove, access_time, updated_at, telegram_link
):
    conn = get_db_connection()
    if not conn:
        return
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO access_control (
                user_id, username, fio, phone,
                aprove, access_time, updated_at, telegram_link
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                user_id,
                username,
                fio,
                phone,
                aprove,
                access_time,
                updated_at,
                telegram_link,
            ),
        )
        conn.commit()
    except sqlite3.Error as e:
        print(f"[DB insert_new_user error] {e}")
    finally:
        conn.close()


def set_user_approval_status(user_id: str, status: str) -> bool:
    conn = get_db_connection()
    if not conn:
        return False
    try:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE access_control SET aprove = ?, updated_at = CURRENT_TIMESTAMP WHERE user_id = ?",
            (status.lower(), str(user_id)),
        )
        conn.commit()
        return cursor.rowcount > 0
    except sqlite3.Error as e:
        print(f"[DB set_user_approval_status error] {e}")
        return False
    finally:
        conn.close()


def get_user_record(user_id: str) -> Optional[dict]:
    conn = get_db_connection()
    if not conn:
        return None
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM access_control WHERE user_id = ?", (str(user_id),)
        )
        row = cursor.fetchone()
        return dict(row) if row else None
    except sqlite3.Error as e:
        print(f"[DB get_user_record error] {e}")
        return None
    finally:
        conn.close()
