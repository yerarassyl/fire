import re
from typing import Dict, Any, Optional, Tuple
from app.db import get_conn
from app.llm import llm_template


# ============================
# Конфиг классов (твои)
# ============================
ISSUE_TYPES = [
    "Жалоба",
    "Смена данных",
    "Консультация",
    "Претензия",
    "Неработоспособность приложения",
    "Мошеннические действия",
    "Спам",
]
SENTIMENTS = {"positive", "neutral", "negative"}
LANGS = {"RU", "KZ", "ENG"}


# ============================
# Language detection (очень простой)
# ============================
RE_KZ = re.compile(r"[әқңөүұіһ]", re.IGNORECASE)
RE_LATIN = re.compile(r"[a-zA-Z]")

def detect_language(text: str) -> str:
    if not text:
        return "RU"
    if RE_KZ.search(text):
        return "KZ"
    latin = len(RE_LATIN.findall(text))
    cyr = len(re.findall(r"[А-Яа-яЁё]", text))
    if latin > cyr:
        return "ENG"
    return "RU"


# ============================
# Rule-based issue detection (только для типа + базового приоритета)
# ============================
RE_URL = re.compile(r"(https?://|www\.)", re.IGNORECASE)

PAT_SPAM = re.compile(r"(выгодн|предложен|реклам|скидк|акци[яи]|купит|продам|заработ|инвестируй|подписывай|телеграм|whatsapp|write me|dm me|заказать сейчас)", re.IGNORECASE)
PAT_FRAUD = re.compile(r"(мошен|взлом|украл|краж|fraud|scam|phish|подозрит|несанкц|неизвестн.*операц|unknown transaction)", re.IGNORECASE)
PAT_APP_DOWN = re.compile(r"(не работ|ошибк|краш|вылет|лагает|завис|не груз|не открыв|не запуска|bug|error|crash|login failed|не могу войти|пароль|смс.*не приход|otp.*не приходит)", re.IGNORECASE)
PAT_DATA_CHANGE = re.compile(r"(смен(а|ить)\s+(данн|телефон|почт|email|паспорт|удост|иин)|измен(ить|ение)\s+(данн|телефон|почт|email)|обнов(ить|ление)\s+(данн|паспорт)|не могу изменить данные)", re.IGNORECASE)
PAT_CLAIM = re.compile(r"(компенсац|вернит(е|ь)\s+деньги|возмест|refund|chargeback|списал(и|ось)|удержал(и|ось)|комисси[яи].*(списал|удерж)|не пришл[оа]\s*\$|\bпретензи)", re.IGNORECASE)
PAT_QUESTION = re.compile(r"(\?|можно ли|как (мне|нам)|подскажите|уточните|что значит|когда|почему|сколько|какая|какие|how to|could you|please explain)", re.IGNORECASE)
PAT_COMPLAINT = re.compile(r"(плох|ужас|недовол|возмут|не устраива|жалоб|проблем|срочно|блокир|заблокир|не могу|не получается)", re.IGNORECASE)

def rule_based_issue_and_priority(text: str) -> Tuple[Optional[str], Optional[int], str]:
    """
    Возвращает (issue_type, priority, reason).
    Если issue_type=None => отправим на LLM.
    """
    t = (text or "").strip()
    if not t:
        return None, None, "empty"

    if RE_URL.search(t) or PAT_SPAM.search(t):
        return "Спам", 1, "rule:spam"

    if PAT_FRAUD.search(t):
        return "Мошеннические действия", 10, "rule:fraud"

    if PAT_APP_DOWN.search(t):
        pr = 9 if re.search(r"(не могу войти|смс.*не приход|otp.*не приход|пароль)", t, re.IGNORECASE) else 7
        return "Неработоспособность приложения", pr, "rule:app_down"

    if PAT_DATA_CHANGE.search(t):
        return "Смена данных", 6, "rule:data_change"

    if PAT_CLAIM.search(t):
        return "Претензия", 9, "rule:claim"

    if PAT_QUESTION.search(t):
        return "Консультация", 4, "rule:question"

    if PAT_COMPLAINT.search(t):
        return "Жалоба", 7, "rule:complaint"

    return None, None, "rule:uncertain"


# ============================
# Проф стиль: summary/action (RU)
# ============================
FORBIDDEN_CLIENT_PHRASES = [
    "свяжитесь", "обратитесь", "пожалуйста", "мы рекомендуем", "вам нужно", "вам следует",
    "закажите", "заказать", "напишите", "write me", "dm me", "hello", "здравствуйте", "добрый день"
]

def make_crm_summary_ru(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"^(здравствуйте|добрый день|привет)[,!\s]*", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s+", " ", s).strip()
    if not s:
        return "Клиент обращается с запросом; требуется уточнение деталей."
    # убираем 1 лицо по возможности
    s = re.sub(r"\b(я|мне|мой|хочу|прошу)\b", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s+", " ", s).strip()
    if not re.match(r"^(клиент|пользователь)", s, flags=re.IGNORECASE):
        s = f"Клиент сообщает, что {s[0].lower() + s[1:]}" if len(s) > 2 else f"Клиент сообщает: {s}"
    return s[:220]

def make_internal_action_ru(a: str, issue_type: str, priority: int) -> str:
    a = (a or "").strip()
    low = a.lower()

    # если модель дала "клиентский" текст — сбрасываем
    if any(p in low for p in FORBIDDEN_CLIENT_PHRASES):
        a = ""

    if not a:
        if issue_type == "Мошеннические действия":
            a = "Эскалировать в антифрод/безопасность, зафиксировать детали и при необходимости инициировать блокировку."
        elif issue_type == "Неработоспособность приложения":
            a = "Проверить статус сервиса и логи, воспроизвести проблему и при необходимости эскалировать в техническую линию."
        elif issue_type == "Смена данных":
            a = "Запросить подтверждение личности, проверить регламент и оформить изменение данных в системе."
        elif issue_type == "Претензия":
            a = "Проверить транзакции/комиссии, собрать факты и подготовить ответ по возврату/компенсации либо эскалировать в финансовую линию."
        elif issue_type == "Жалоба":
            a = "Уточнить детали, зафиксировать причину недовольства и предложить решение либо эскалировать в профильную линию."
        else:  # Консультация
            a = "Проверить справочные материалы/условия продукта и подготовить корректный ответ; при необходимости запросить уточнения."

    # добавить “срочность” мягко, но без лишнего
    if priority >= 9 and not a.lower().startswith(("эскалировать", "проверить")):
        a = "Эскалировать в приоритетную линию и " + a[0].lower() + a[1:]

    # действие должно начинаться с глагола
    if not re.match(r"^(Проверить|Уточнить|Создать|Запросить|Эскалировать|Сверить|Разблокировать|Оформить|Отклонить)", a):
        a = "Проверить детали обращения и выполнить дальнейшие действия по регламенту."

    # чистим "пожалуйста"
    a = re.sub(r"\bпожалуйста\b[, ]*", "", a, flags=re.IGNORECASE).strip()
    return a[:240]


# ============================
# Усиленный prompt для LLM (только если не Спам)
# ============================
SYSTEM_PROMPT = f"""
Ты AI-модуль для внутренней системы контакт-центра.

Верни строго 6 строк формата key: value (без markdown, без лишних строк):

issue_type: <{ " | ".join(ISSUE_TYPES) }>
sentiment: <positive|neutral|negative>
language: <RU|KZ|ENG>
priority: <1-10 integer>
summary: <1 предложение НА РУССКОМ в стиле CRM (третье лицо)>
recommended_action: <1 предложение НА РУССКОМ — инструкция сотруднику поддержки>

Стиль:
- summary: "Клиент сообщает/запрашивает/указывает, что ..."
- recommended_action: начинается с глагола ("Проверить/Уточнить/Запросить/Эскалировать/Оформить/Отклонить").
- Запрещено писать клиенту: "Свяжитесь/Обратитесь/Пожалуйста/Мы рекомендуем".
- Запрещено: "в суд/в полицию" — вместо этого "Эскалировать в комплаенс/юридический/антифрод".

Важно: summary и recommended_action всегда на русском, даже если обращение на KZ/ENG.
""".strip()

TEMPLATE = f"""issue_type: <{'|'.join(ISSUE_TYPES)}>
sentiment: <positive|neutral|negative>
language: <RU|KZ|ENG>
priority: <1-10 integer>
summary: <one sentence in Russian CRM style>
recommended_action: <one sentence in Russian for support agent>"""


def _norm_key(raw: str) -> str:
    return re.sub(r"[^a-zA-Z_]", "", (raw or "").strip().lower())


def _safe_int(s: str, default: int) -> int:
    try:
        return int(re.findall(r"-?\d+", s)[0])
    except:
        return default


def parse_lenient(text: str) -> Optional[Dict[str, Any]]:
    if not text or not text.strip():
        return None
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    kv: Dict[str, str] = {}
    for ln in lines:
        if ":" not in ln:
            continue
        k, v = ln.split(":", 1)
        kv[_norm_key(k)] = v.strip()

    required = ["issue_type", "sentiment", "language", "priority", "summary", "recommended_action"]
    if not all(k in kv for k in required):
        return None

    issue_type = kv["issue_type"].strip()
    if issue_type not in ISSUE_TYPES:
        # fallback
        low = issue_type.lower()
        if "мош" in low:
            issue_type = "Мошеннические действия"
        elif "спам" in low:
            issue_type = "Спам"
        elif "неработ" in low or "прилож" in low or "ошиб" in low or "войти" in low:
            issue_type = "Неработоспособность приложения"
        elif "смен" in low or "данн" in low:
            issue_type = "Смена данных"
        elif "прет" in low or "компен" in low or "вернит" in low:
            issue_type = "Претензия"
        elif "жалоб" in low:
            issue_type = "Жалоба"
        else:
            issue_type = "Консультация"

    sentiment = kv["sentiment"].strip().lower()
    if sentiment not in SENTIMENTS:
        sentiment = "neutral"

    language = kv["language"].strip().upper()
    if language not in LANGS:
        language = "RU"

    priority = _safe_int(kv["priority"], 5)
    priority = max(1, min(10, priority))

    summary = (kv["summary"].strip() or "")[:220]
    recommended_action = (kv["recommended_action"].strip() or "")[:240]

    return {
        "issue_type": issue_type,
        "sentiment": sentiment,
        "language": language,
        "priority": priority,
        "summary": summary,
        "recommended_action": recommended_action,
    }


def main():
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT t.id, COALESCE(t.description,''), COALESCE(t.attachment_ref,'')
            FROM tickets t
            LEFT JOIN ticket_ai ai ON ai.ticket_id=t.id
            WHERE ai.ticket_id IS NULL
            ORDER BY t.id ASC;
        """)
        rows = cur.fetchall()

        if not rows:
            print("✅ ticket_ai уже заполнен")
            return

        print(f"🧠 Enrich (prof RU + no spam text): {len(rows)} tickets")
        done = 0
        failed = 0

        for idx, (ticket_id, description, attach) in enumerate(rows, start=1):
            text = f"{description}\n{attach}".strip()
            print(f"➡️ [{idx}/{len(rows)}] ticket_id={ticket_id} ...", flush=True)

            issue_r, pr_r, reason = rule_based_issue_and_priority(text)
            lang_r = detect_language(text)

            # Если СПАМ — вообще ничего не пишем в summary/action и LLM не вызываем
            if issue_r == "Спам":
                payload = {
                    "issue_type": "Спам",
                    "sentiment": "neutral",
                    "language": lang_r,
                    "priority": 1,
                    "summary": "",
                    "recommended_action": "",
                }
            else:
                user_text = f"""Описание обращения:
{description}

Вложения:
{attach}""".strip()

                try:
                    raw = llm_template(SYSTEM_PROMPT, user_text, TEMPLATE)
                    payload = parse_lenient(raw)
                    if not payload:
                        raise RuntimeError(f"Bad format. Raw: {raw[:260]}")
                except Exception as e:
                    failed += 1
                    print(f"❌ ticket_id={ticket_id} LLM failed: {e}", flush=True)
                    continue

                # override тип/приоритет из rules, если rules уверены
                if issue_r:
                    payload["issue_type"] = issue_r
                    payload["priority"] = int(pr_r or payload["priority"])

                payload["language"] = lang_r

                # принудительно делаем проф стиль и internal action (RU)
                payload["summary"] = make_crm_summary_ru(payload.get("summary", ""))
                payload["recommended_action"] = make_internal_action_ru(
                    payload.get("recommended_action", ""),
                    payload["issue_type"],
                    int(payload["priority"]),
                )

            cur.execute(
                """
                INSERT INTO ticket_ai(ticket_id, issue_type, sentiment, priority, language, summary, recommended_action)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    ticket_id,
                    payload["issue_type"],
                    payload["sentiment"],
                    int(payload["priority"]),
                    payload["language"],
                    payload["summary"],
                    payload["recommended_action"],
                ),
            )
            conn.commit()
            done += 1
            print(f"✅ ticket_id={ticket_id} OK (reason={reason})", flush=True)

        print(f"✅ Done: {done}, failed: {failed}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()