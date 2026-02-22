from fastapi import FastAPI, UploadFile, File
from pydantic import BaseModel
from typing import Optional
import os
import shutil
import importlib

from app.db import get_conn
from app.nlq import nl_to_intent

app = FastAPI(title="FIRE API", version="1.3")


class NLQRequest(BaseModel):
    query: str


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/nlq")
def nlq(req: NLQRequest):
    parsed = nl_to_intent(req.query)
    if not parsed.ok:
        return {"ok": False, "message": parsed.message}

    conn = get_conn()
    try:
        cur = conn.cursor()
        # ✅ важно: поддержка params из нового NLQ
        cur.execute(parsed.sql, parsed.params or ())
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        return {
            "ok": True,
            "message": parsed.message,
            "intent": parsed.intent,
            "columns": cols,
            "rows": rows,         # оставляем как было (list[tuple]) — твой UI это умеет
            "chart": parsed.chart,
        }
    finally:
        conn.close()


@app.get("/assignments")
def get_assignments():
    """
    ДЛЯ Explain Viewer:
    Возвращает назначения + AI поля + JSON explanation.
    ВАЖНО: показываем только те тикеты, у которых есть assignment.
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT
              t.id AS ticket_id,
              t.client_guid,
              COALESCE(NULLIF(TRIM(t.city), ''), 'UNKNOWN') AS city,
              COALESCE(NULLIF(TRIM(t.address_text), ''), '') AS address_text,

              COALESCE(NULLIF(TRIM(t.segment), ''), 'Mass') AS segment,
              COALESCE(NULLIF(TRIM(ai.issue_type), ''), 'UNKNOWN') AS issue_type,
              COALESCE(NULLIF(TRIM(ai.sentiment), ''), 'UNKNOWN') AS sentiment,
              COALESCE(ai.priority, 0) AS priority,
              COALESCE(NULLIF(TRIM(ai.language), ''), 'RU') AS language,

              -- ✅ добавили AI summary + recommendation
              COALESCE(NULLIF(TRIM(ai.summary), ''), '') AS ai_summary,
              COALESCE(NULLIF(TRIM(ai.recommended_action), ''), '') AS ai_recommendation,

              a.office_name,
              m.full_name AS manager_name,
              m.position,
              m.current_load,

              COALESCE(re.explanation::text, '{}') AS explanation,
              a.assigned_at,
              a.algorithm
            FROM assignments a
            JOIN tickets t ON t.id = a.ticket_id
            LEFT JOIN ticket_ai ai ON ai.ticket_id = t.id
            LEFT JOIN managers m ON m.id = a.manager_id
            LEFT JOIN routing_explanations re ON re.assignment_id = a.id
            ORDER BY a.assigned_at DESC, t.id DESC;
        """)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in rows]
    finally:
        conn.close()


@app.get("/geo_points")
def geo_points():
    """
    Точки тикетов + координаты их офиса (чтобы рисовать линии).
    office_name берём из assignments, если нет — из tg.nearest_office.
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            WITH x AS (
              SELECT
                t.id AS ticket_id,
                COALESCE(NULLIF(TRIM(t.city), ''), 'UNKNOWN') AS city,
                COALESCE(NULLIF(TRIM(ai.issue_type), ''), 'UNKNOWN') AS issue_type,
                COALESCE(NULLIF(TRIM(ai.sentiment), ''), 'UNKNOWN') AS sentiment,
                COALESCE(ai.priority, 0) AS priority,
                tg.geocode_status,
                tg.distance_m,

                -- итоговый офис для визуализации
                COALESCE(a.office_name, tg.nearest_office, 'UNKNOWN') AS office_name,

                -- координаты тикета
                ST_X(tg.client_location::geometry) AS lon,
                ST_Y(tg.client_location::geometry) AS lat
              FROM ticket_geo tg
              JOIN tickets t ON t.id = tg.ticket_id
              LEFT JOIN ticket_ai ai ON ai.ticket_id = t.id
              LEFT JOIN assignments a ON a.ticket_id = t.id
              WHERE tg.client_location IS NOT NULL
            )
            SELECT
              x.*,
              -- координаты офиса (для линий)
              ST_X(b.location::geometry) AS office_lon,
              ST_Y(b.location::geometry) AS office_lat
            FROM x
            LEFT JOIN business_units b
              ON b.office_name = x.office_name
            ;
        """)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in rows]
    finally:
        conn.close()


@app.get("/office_points")
def office_points():
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT
              office_name,
              ST_X(location::geometry) AS lon,
              ST_Y(location::geometry) AS lat
            FROM business_units
            WHERE location IS NOT NULL
            ORDER BY office_name;
        """)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in rows]
    finally:
        conn.close()


def _import_any(*names):
    last_err: Optional[Exception] = None
    for n in names:
        try:
            return importlib.import_module(n)
        except Exception as e:
            last_err = e
    raise last_err


@app.post("/pipeline/run")
def run_pipeline(
    tickets: UploadFile = File(...),
):
    """
    ✅ ТОЛЬКО tickets.csv
    Upload → ./data/tickets.csv → load_data → enrich_stub → route
    """
    os.makedirs("data", exist_ok=True)

    with open("data/tickets.csv", "wb") as f:
        shutil.copyfileobj(tickets.file, f)

    # модули могут быть в корне или в app/
    load_data = _import_any("load_data", "app.load_data")
    enrich_stub = _import_any("enrich_stub", "app.enrich_stub")
    route = _import_any("route", "app.route")

    # если у тебя main() называется иначе — скажи, быстро поменяю
    load_data.main()
    enrich_stub.main()
    route.main()

    return {"ok": True, "message": "Pipeline completed (tickets.csv only)"}