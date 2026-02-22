import re
import pandas as pd
from app.db import get_conn
from app.geocoder import geocode_address_precise

TICKETS_CSV = "tickets.csv"
MANAGERS_CSV = "managers.csv"
BUS_CSV = "business_units.csv"


def _read_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, encoding="utf-8-sig")
    df.columns = [str(c).strip() for c in df.columns]
    return df


def reset_all_data(conn):
    cur = conn.cursor()
    cur.execute("""
        TRUNCATE routing_explanations,
                 assignments,
                 ticket_geo,
                 ticket_ai,
                 tickets,
                 managers
        RESTART IDENTITY CASCADE;
    """)
    conn.commit()


def parse_skills(cell):
    if cell is None or (isinstance(cell, float) and pd.isna(cell)):
        return []
    s = str(cell).strip()
    s = s.replace("[", "").replace("]", "").replace("'", "").replace('"', "")
    return [p.strip() for p in s.split(",") if p.strip()]


def clean_office_address(addr: str) -> str:
    """
    Убираем то, что часто мешает геокодингу.
    """
    if not addr:
        return ""
    a = str(addr).replace("\x0b", " ").replace("\r", " ").replace("\n", " ")
    a = a.replace("«", "").replace("»", "").replace("“", "").replace("”", "")
    # режем хвосты вида "БЦ ..., этаж ..., офис ..."
    a = re.sub(r"\b(бц|бизнес-центр)\b.*$", "", a, flags=re.IGNORECASE)
    a = re.sub(r"\b(этаж|офис|кабинет|помещение)\b.*$", "", a, flags=re.IGNORECASE)
    a = re.sub(r"\s+", " ", a).strip(" ,.;")
    return a


def load_business_units(conn):
    df = _read_csv(BUS_CSV)

    office_col = next((c for c in df.columns if c.lower() in ("офис", "office")), None)
    addr_col = next((c for c in df.columns if ("адрес" in c.lower() or "address" in c.lower())), None)

    if not office_col:
        raise ValueError(f"Не найдена колонка 'Офис/Office' в {BUS_CSV}. Колонки: {list(df.columns)}")

    cur = conn.cursor()
    for _, r in df.iterrows():
        office = str(r[office_col]).strip()
        raw_addr = str(r[addr_col]).strip() if addr_col and pd.notna(r[addr_col]) else None

        cur.execute(
            """
            INSERT INTO business_units(office_name, address_text)
            VALUES (%s, %s)
            ON CONFLICT (office_name) DO UPDATE
              SET address_text = EXCLUDED.address_text
            """,
            (office, raw_addr),
        )
    conn.commit()

    # ✅ Геокодим ВСЕ офисы: "очищенный адрес + город(office_name) + Казахстан"
    cur.execute("SELECT office_name, address_text FROM business_units ORDER BY office_name;")
    offices = cur.fetchall()

    updated = 0
    for office_name, address_text in offices:
        if not address_text or not str(address_text).strip():
            continue

        cleaned = clean_office_address(address_text)
        query = f"{cleaned}, {office_name}, Казахстан"

        coords = geocode_address_precise(query)
        if not coords:
            print(f"❌ Office geocode FAILED: {office_name} -> {query}")
            continue

        lon, lat = coords
        cur.execute(
            """
            UPDATE business_units
            SET location = ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography
            WHERE office_name = %s
            """,
            (lon, lat, office_name),
        )
        updated += 1
        print(f"✅ Office geocode OK: {office_name}")

    conn.commit()
    print(f"📍 Offices with coords: {updated}/{len(offices)}")


def load_managers(conn):
    df = _read_csv(MANAGERS_CSV)

    def find_contains(substrs):
        for c in df.columns:
            low = c.lower()
            if any(s in low for s in substrs):
                return c
        return None

    name_col = find_contains(["фио", "full_name", "name"])
    pos_col = find_contains(["долж", "position"])
    skills_col = find_contains(["навык", "skill"])
    office_col = find_contains(["бизнес", "офис", "office"])
    load_col = find_contains(["нагруз", "load", "обращ"])

    if not all([name_col, pos_col, office_col]):
        raise ValueError(f"Не найдены обязательные колонки в {MANAGERS_CSV}. Колонки: {list(df.columns)}")

    cur = conn.cursor()
    for _, r in df.iterrows():
        full_name = str(r[name_col]).strip()
        position = str(r[pos_col]).strip()
        office = str(r[office_col]).strip()
        skills = parse_skills(r[skills_col]) if skills_col and pd.notna(r[skills_col]) else []
        current_load = int(r[load_col]) if load_col and pd.notna(r[load_col]) else 0

        cur.execute(
            """
            INSERT INTO managers(full_name, position, skills, office_name, current_load, is_active)
            VALUES (%s, %s, %s, %s, %s, TRUE)
            """,
            (full_name, position, skills, office, current_load),
        )
    conn.commit()


def load_tickets(conn):
    df = _read_csv(TICKETS_CSV)

    REQUIRED = [
        "GUID клиента",
        "Пол клиента",
        "Дата рождения",
        "Описание",
        "Вложения",
        "Сегмент клиента",
        "Страна",
        "Область",
        "Населённый пункт",
        "Улица",
        "Дом",
    ]
    missing = [c for c in REQUIRED if c not in df.columns]
    if missing:
        raise ValueError(f"В {TICKETS_CSV} не найдены колонки: {missing}")

    cur = conn.cursor()

    for _, r in df.iterrows():
        def val(col):
            v = r[col]
            if pd.isna(v):
                return None
            s = str(v).strip()
            return s if s != "" else None

        country = val("Страна")
        city = val("Населённый пункт")
        street = val("Улица")
        house = val("Дом")

        # ✅ Новый формат адреса (БЕЗ области)
        address_parts = [city, street, house]

        # добавим Казахстан если не зарубеж
        if country and "казахстан" in country.lower():
            address_parts.append("Казахстан")

        address_text = ", ".join([p for p in address_parts if p]).strip() or None

        cur.execute(
            """
            INSERT INTO tickets(
              client_guid, gender, birth_date, segment, description, attachment_ref,
              country, region, city, street, house, address_text
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                val("GUID клиента"),
                val("Пол клиента"),
                r["Дата рождения"] if pd.notna(r["Дата рождения"]) else None,
                val("Сегмент клиента"),
                val("Описание"),
                val("Вложения"),
                country,
                val("Область"),  # область сохраняем в БД, но не используем для геокода
                city,
                street,
                house,
                address_text,
            ),
        )

    conn.commit()


def main():
    conn = get_conn()
    try:
        print("🔄 Reset old data...")
        reset_all_data(conn)

        print("🏢 Load business units + geocode ALL offices (2GIS)...")
        load_business_units(conn)

        print("👨‍💼 Load managers...")
        load_managers(conn)

        print("🎫 Load tickets (full address_text)...")
        load_tickets(conn)

        print("✅ Done. Now run: python -m app.enrich_stub && python -m app.route")
    finally:
        conn.close()


if __name__ == "__main__":
    main()