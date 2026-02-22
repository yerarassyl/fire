import json
from app.db import get_conn
from app.utils import rr_key, build_explanation
from app.geocoder import geocode_address_precise

ASTANA = "Астана"
ALMATY = "Алматы"

CHIEF_TITLE = "главный специалист"  # <-- фикс: правильное название должности


def is_foreign(country: str) -> bool:
    c = (country or "").strip().lower()
    if not c:
        return False
    return not any(x in c for x in ["казахстан", "kz", "qazaq", "қазақстан"])


def pick_50_50(ticket_id: int):
    return ASTANA if (ticket_id % 2 == 0) else ALMATY


def _norm_pos(pos: str) -> str:
    # нормализация для надёжного сравнения
    return " ".join((pos or "").strip().lower().split())


def get_candidates(conn, office, need_vip, need_chief, lang):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, full_name, position, skills, current_load
        FROM managers
        WHERE is_active = TRUE
          AND office_name = %s
        """,
        (office,),
    )
    managers = cur.fetchall()

    filtered = []
    for mid, name, pos, skills, load in managers:
        if need_vip and "VIP" not in (skills or []):
            continue

        # ✅ FIX HERE: было "Глав спец", стало "Главный специалист" (и сравнение нормализовано)
        if need_chief and _norm_pos(pos) != CHIEF_TITLE:
            continue

        if lang in ["ENG", "KZ"] and lang not in (skills or []):
            continue

        filtered.append((mid, name, pos, skills, load))

    filtered.sort(key=lambda x: x[4])  # lowest load first
    return filtered


def round_robin_pick(conn, key, top2):
    if len(top2) == 1:
        return top2[0]

    cur = conn.cursor()
    cur.execute("SELECT last_manager_id FROM rr_state WHERE rr_key=%s;", (key,))
    row = cur.fetchone()
    last = row[0] if row else None

    first, second = top2[0], top2[1]
    picked = second if last == first[0] else first

    cur.execute(
        """
        INSERT INTO rr_state(rr_key, last_manager_id)
        VALUES (%s, %s)
        ON CONFLICT (rr_key) DO UPDATE
          SET last_manager_id=EXCLUDED.last_manager_id, updated_at=NOW()
        """,
        (key, picked[0]),
    )
    return picked


def nearest_offices_by_postgis(conn, lon: float, lat: float, limit: int = 10):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT office_name,
               ST_Distance(
                 location,
                 ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography
               ) AS dist_m
        FROM business_units
        WHERE location IS NOT NULL
        ORDER BY dist_m ASC
        LIMIT %s;
        """,
        (lon, lat, limit),
    )
    return cur.fetchall()


def pick_office_with_matching_manager(conn, lon, lat, need_vip, need_chief, lang, try_k=10):
    nearest = nearest_offices_by_postgis(conn, lon, lat, limit=try_k)

    tried = []
    for idx, (office, dist_m) in enumerate(nearest, start=1):
        cands = get_candidates(conn, office, need_vip, need_chief, lang)
        tried.append({
            "rank": idx,
            "office": office,
            "dist_m": int(dist_m) if dist_m is not None else None,
            "need_vip": need_vip,
            "need_chief": need_chief,
            "lang": lang,
            "candidates": len(cands),
        })
        if cands:
            return office, dist_m, idx, tried

    return None, None, None, tried


def main():
    conn = get_conn()
    try:
        cur = conn.cursor()

        cur.execute(
            """
            SELECT t.id, t.country, t.address_text, t.segment,
                   ai.issue_type, ai.language
            FROM tickets t
            LEFT JOIN ticket_ai ai ON ai.ticket_id=t.id
            LEFT JOIN assignments a ON a.ticket_id=t.id
            WHERE a.id IS NULL
            ORDER BY t.id ASC;
            """
        )
        tickets = cur.fetchall()
        assigned = 0

        for ticket_id, country, address_text, segment, issue_type, lang in tickets:
            seg = (segment or "").upper()
            issue_type = issue_type or "Консультация"
            lang = (lang or "RU").upper()

            need_vip = seg in ["VIP", "PRIORITY"]
            need_chief = (issue_type == "Смена данных")

            filters = {}
            coords = None
            dist_m = None

            # 1) GEO per ТЗ
            if not address_text or not str(address_text).strip():
                office = pick_50_50(ticket_id)
                geocode_status = "UNKNOWN"
                filters["geo"] = "Адрес пустой -> 50/50 Астана/Алматы"
            elif is_foreign(country):
                office = pick_50_50(ticket_id)
                geocode_status = "FOREIGN"
                filters["geo"] = "Зарубеж -> 50/50 Астана/Алматы"
            else:
                coords = geocode_address_precise(address_text)
                if not coords:
                    office = pick_50_50(ticket_id)
                    geocode_status = "FAILED"
                    filters["geo"] = "2GIS geocode FAILED -> 50/50"
                else:
                    lon, lat = coords
                    office2, dist2, rank, tried = pick_office_with_matching_manager(
                        conn, lon, lat, need_vip, need_chief, lang, try_k=10
                    )

                    if office2:
                        office = office2
                        dist_m = dist2
                        geocode_status = "OK"
                        filters["geo"] = f"2GIS OK -> nearest office={office}, dist_m≈{int(dist_m) if dist_m else 0}"
                        filters["geo_rank"] = rank
                        filters["geo_tried_offices"] = tried
                    else:
                        office = pick_50_50(ticket_id)
                        geocode_status = "NO_MATCH_IN_NEAREST"
                        filters["geo"] = f"2GIS OK, but no manager match in {10} nearest -> 50/50"
                        filters["geo_tried_offices"] = tried

            # 2) сохраняем ticket_geo
            if coords:
                lon, lat = coords
                cur.execute(
                    """
                    INSERT INTO ticket_geo(ticket_id, client_location, geocode_status, nearest_office, distance_m)
                    VALUES (%s, ST_SetSRID(ST_MakePoint(%s,%s),4326)::geography, %s, %s, %s)
                    ON CONFLICT (ticket_id) DO UPDATE SET
                      client_location=EXCLUDED.client_location,
                      geocode_status=EXCLUDED.geocode_status,
                      nearest_office=EXCLUDED.nearest_office,
                      distance_m=EXCLUDED.distance_m
                    """,
                    (ticket_id, lon, lat, geocode_status, office, dist_m),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO ticket_geo(ticket_id, geocode_status, nearest_office, distance_m)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (ticket_id) DO UPDATE SET
                      geocode_status=EXCLUDED.geocode_status,
                      nearest_office=EXCLUDED.nearest_office,
                      distance_m=EXCLUDED.distance_m
                    """,
                    (ticket_id, geocode_status, office, dist_m),
                )

            # 3) rules descriptions
            filters["vip_rule"] = f"segment={seg or 'Mass'} -> VIP {'required' if need_vip else 'no'}"
            filters["position_rule"] = (
                f"Смена данных -> требуется должность '{CHIEF_TITLE}'"
                if issue_type == "Смена данных"
                else "position not required"
            )
            filters["lang_rule"] = f"language={lang} -> need skill {lang}" if lang in ["ENG", "KZ"] else "language=RU -> none"

            # 4) кандидаты в выбранном офисе
            cands = get_candidates(conn, office, need_vip, need_chief, lang)

            if not cands:
                filters["fallback"] = "No candidates in chosen office -> global search (strict filters), pick min load"
                cur2 = conn.cursor()
                cur2.execute(
                    "SELECT id, full_name, position, skills, current_load, office_name FROM managers WHERE is_active=TRUE"
                )
                allm = cur2.fetchall()

                relaxed = []
                for mid, name, pos, skills, load, off in allm:
                    if need_vip and "VIP" not in (skills or []):
                        continue
                    if need_chief and _norm_pos(pos) != CHIEF_TITLE:
                        continue
                    if lang in ["ENG", "KZ"] and lang not in (skills or []):
                        continue
                    relaxed.append((mid, name, pos, skills, load, off))

                relaxed.sort(key=lambda x: x[4])
                if not relaxed:
                    continue

                office = relaxed[0][5]
                top2 = [(r[0], r[1], r[2], r[3], r[4]) for r in relaxed[:2]]
                filters["fallback_office"] = f"Selected office={office} by global min load"
            else:
                top2 = cands[:2]

            rrk = rr_key(office, need_vip, lang, need_chief)
            picked_m = round_robin_pick(conn, rrk, top2)
            manager_id, manager_name, manager_pos, manager_skills, manager_load = picked_m

            # 5) assignment
            cur.execute(
                """
                INSERT INTO assignments(ticket_id, office_name, manager_id, algorithm)
                VALUES (%s,%s,%s,%s)
                RETURNING id
                """,
                (ticket_id, office, manager_id, "FIRE_GEO_KNEAREST"),
            )
            assignment_id = cur.fetchone()[0]

            candidates_payload = [
                {"manager_id": m[0], "full_name": m[1], "position": m[2], "load": m[4]} for m in top2
            ]
            rr_payload = {"rr_key": rrk, "picked_manager_id": manager_id, "picked_full_name": manager_name}

            explanation = build_explanation(filters, candidates_payload, rr_payload)

            cur.execute(
                "INSERT INTO routing_explanations(assignment_id, explanation) VALUES (%s, %s::jsonb)",
                (assignment_id, json.dumps(explanation, ensure_ascii=False)),
            )
            cur.execute("UPDATE managers SET current_load=current_load+1 WHERE id=%s", (manager_id,))
            assigned += 1

        conn.commit()
        print(f"🗺️✅ FIRE_GEO_KNEAREST: assigned={assigned}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()