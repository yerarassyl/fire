import re
from dataclasses import dataclass
from typing import Dict, Any, Optional, List, Tuple

# -----------------------------
# DIMENSIONS + METRICS CONFIG
# -----------------------------
DIMS = {
    "город": ("tickets", "city"),
    "офис": ("assignments", "office_name"),
    "менеджер": ("managers", "full_name"),
    "тип": ("ticket_ai", "issue_type"),
    "тональность": ("ticket_ai", "sentiment"),
    "язык": ("ticket_ai", "language"),
    "сегмент": ("tickets", "segment"),
}

# Псевдо-измерение "дата" (для line). У тебя точно есть assignments.assigned_at в /assignments.
# Если захочешь тренды по созданию тикетов — добавь tickets.created_at в БД.
TIME_DIMS = {"дата": ("assignments", "assigned_at")}

METRICS = {
    "count": {"sql": "COUNT(*)", "alias": "cnt"},
    "avg_priority": {"sql": "AVG(COALESCE(ticket_ai.priority,0))", "alias": "avg_priority"},
}

@dataclass
class ParsedNLQ:
    ok: bool
    intent: Optional[str] = None
    sql: Optional[str] = None
    params: Optional[List[Any]] = None
    chart: Optional[Dict[str, Any]] = None
    message: str = ""

# -----------------------------
# SMALL NLP HELPERS (RULE-BASED)
# -----------------------------
RE_TOP = re.compile(r"\bтоп\s*(\d{1,3})\b", re.IGNORECASE)
RE_CITY1 = re.compile(r"\bв городе\s+([А-Яа-яЁё\- ]+)", re.IGNORECASE)
RE_CITY2 = re.compile(r"\bгород(е|а)?\s+([А-Яа-яЁё\- ]+)", re.IGNORECASE)

RE_PRIORITY_GE = re.compile(r"\bприоритет\s*(>=|>|не меньше)\s*(\d{1,2})", re.IGNORECASE)
RE_PRIORITY_LE = re.compile(r"\bприоритет\s*(<=|<|не больше)\s*(\d{1,2})", re.IGNORECASE)

RE_LANG = re.compile(r"\b(ru|kz|eng|рус|каз|англ)\b", re.IGNORECASE)

def _norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def extract_top_n(q: str) -> Optional[int]:
    m = RE_TOP.search(q)
    if not m:
        return None
    try:
        n = int(m.group(1))
        return max(1, min(100, n))
    except:
        return None

def extract_city_filter(query: str) -> Optional[str]:
    m = RE_CITY1.search(query)
    if m:
        return _norm_space(m.group(1))
    m = RE_CITY2.search(query)
    if m:
        return _norm_space(m.group(2))
    return None

def extract_lang_filter(query: str) -> Optional[str]:
    m = RE_LANG.search(query or "")
    if not m:
        return None
    t = m.group(1).lower()
    if t in ["ru", "рус"]:
        return "RU"
    if t in ["kz", "каз"]:
        return "KZ"
    if t in ["eng", "англ"]:
        return "ENG"
    return None

def extract_priority_filters(query: str) -> Tuple[Optional[int], Optional[int]]:
    ge = None
    le = None
    m = RE_PRIORITY_GE.search(query or "")
    if m:
        try: ge = int(m.group(2))
        except: pass
    m = RE_PRIORITY_LE.search(query or "")
    if m:
        try: le = int(m.group(2))
        except: pass
    if ge is not None: ge = max(0, min(10, ge))
    if le is not None: le = max(0, min(10, le))
    return ge, le

def detect_metric(query: str) -> str:
    q = (query or "").lower()
    if "средн" in q and "приоритет" in q:
        return "avg_priority"
    return "count"

def detect_chart_kind(query: str, dims: List[str]) -> str:
    q = (query or "").lower()

    # если явно просит таблицу
    if any(w in q for w in ["таблица", "table", "список", "list"]):
        return "table"

    # pie / доли
    if any(w in q for w in ["круг", "pie", "доля", "процент", "share"]):
        return "pie"

    # line / тренд
    if any(w in q for w in ["тренд", "динамик", "по дат", "line", "график линии"]):
        return "line"

    # stacked если 2 измерения или явно просит “разбивка/стек”
    if len(dims) >= 2 and any(w in q for w in ["разбив", "стек", "stack", "сегментирован"]):
        return "stacked_bar"

    # дефолт
    return "stacked_bar" if len(dims) >= 2 else "bar"

def extract_dims_from_query(query: str) -> List[str]:
    q = (query or "").lower()

    # поддержим “по X”, “по X и Y”, “по X по Y”
    found: List[str] = []
    for d in list(DIMS.keys()) + list(TIME_DIMS.keys()):
        if d in q:
            found.append(d)

    # паттерн "по X по Y"
    m = re.search(r"по\s+(\w+)\s+по\s+(\w+)", q)
    if m:
        w1, w2 = m.group(1), m.group(2)
        for d in list(DIMS.keys()) + list(TIME_DIMS.keys()):
            if d in w1 and d not in found:
                found.insert(0, d)
            if d in w2 and d not in found:
                found.insert(1, d)

    # уникализируем и максимум 2 измерения (для простоты графиков)
    uniq = []
    for x in found:
        if x not in uniq:
            uniq.append(x)

    if not uniq:
        # умный дефолт: если есть "тренд" — дата
        if any(w in q for w in ["тренд", "динамик", "по дат"]):
            return ["дата", "тип"]
        return ["город", "тип"]

    return uniq[:2]

# -----------------------------
# MAIN ENTRY
# -----------------------------
def nl_to_intent(query: str) -> ParsedNLQ:
    if not query or not query.strip():
        return ParsedNLQ(False, message="Пустой запрос")

    dims = extract_dims_from_query(query)
    metric = detect_metric(query)
    top_n = extract_top_n(query)

    # filters
    city_filter = extract_city_filter(query)
    lang_filter = extract_lang_filter(query)
    pr_ge, pr_le = extract_priority_filters(query)

    sql, params, columns, chart = build_sql(
        dims=dims,
        metric=metric,
        city_filter=city_filter,
        lang_filter=lang_filter,
        pr_ge=pr_ge,
        pr_le=pr_le,
        top_n=top_n,
        chart_kind=detect_chart_kind(query, dims),
    )

    msg_bits = [f"По: {', '.join(dims)}", f"метрика: {metric}"]
    if city_filter: msg_bits.append(f"город={city_filter}")
    if lang_filter: msg_bits.append(f"язык={lang_filter}")
    if pr_ge is not None: msg_bits.append(f"priority>={pr_ge}")
    if pr_le is not None: msg_bits.append(f"priority<={pr_le}")
    if top_n: msg_bits.append(f"top={top_n}")

    return ParsedNLQ(
        ok=True,
        intent="analytics",
        sql=sql,
        params=params,
        chart=chart,
        message="; ".join(msg_bits),
    )

# -----------------------------
# SQL BUILDER
# -----------------------------
def build_sql(
    dims: List[str],
    metric: str,
    city_filter: Optional[str],
    lang_filter: Optional[str],
    pr_ge: Optional[int],
    pr_le: Optional[int],
    top_n: Optional[int],
    chart_kind: str,
):
    select_parts = []
    group_parts = []
    columns = []

    # dims
    for d in dims:
        if d in TIME_DIMS:
            tbl, col = TIME_DIMS[d]
            # группируем по дням (можешь сменить на 'month' по ключевым словам, если нужно)
            expr = f"DATE_TRUNC('day', {tbl}.{col})"
            select_parts.append(f"{expr} AS {d}")
            group_parts.append(d)
            columns.append(d)
            continue

        tbl, col = DIMS[d]
        select_parts.append(
            f"COALESCE(NULLIF(TRIM(({tbl}.{col})::text),''),'UNKNOWN') AS {d}"
        )
        group_parts.append(d)
        columns.append(d)

    # metric
    mconf = METRICS.get(metric) or METRICS["count"]
    select_parts.append(f"{mconf['sql']} AS {mconf['alias']}")
    columns.append(mconf["alias"])

    where = []
    params: List[Any] = []

    if city_filter:
        where.append("COALESCE(NULLIF(TRIM(tickets.city),''),'UNKNOWN') = %s")
        params.append(city_filter)

    if lang_filter:
        where.append("COALESCE(ticket_ai.language,'RU') = %s")
        params.append(lang_filter)

    if pr_ge is not None:
        where.append("COALESCE(ticket_ai.priority,0) >= %s")
        params.append(pr_ge)

    if pr_le is not None:
        where.append("COALESCE(ticket_ai.priority,0) <= %s")
        params.append(pr_le)

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    limit_sql = f"LIMIT {int(top_n)}" if top_n else ""

    sql = f"""
        SELECT {", ".join(select_parts)}
        FROM tickets
        LEFT JOIN ticket_ai ON ticket_ai.ticket_id = tickets.id
        LEFT JOIN assignments ON assignments.ticket_id = tickets.id
        LEFT JOIN managers ON managers.id = assignments.manager_id
        {where_sql}
        GROUP BY {", ".join(group_parts)}
        ORDER BY {mconf['alias']} DESC
        {limit_sql};
    """

    # chart schema for frontend
    if chart_kind == "table":
        chart = {"kind": "table"}
    elif chart_kind == "pie":
        # pie имеет смысл только при 1 dim
        label = dims[0]
        chart = {"kind": "pie", "label": label, "value": mconf["alias"]}
    elif chart_kind == "line":
        # line только если ось X = дата
        chart = {"kind": "line", "x": dims[0], "y": mconf["alias"], "series": (dims[1] if len(dims) > 1 else None)}
    elif len(dims) == 1:
        chart = {"kind": "bar", "x": dims[0], "y": mconf["alias"]}
    else:
        chart = {"kind": "stacked_bar", "x": dims[0], "y": mconf["alias"], "series": dims[1]}

    return sql, params, columns, chart