import re
from typing import List, Dict, Any

ISSUE_TYPES = [
    "Жалоба",
    "Смена данных",
    "Консультация",
    "Претензия",
    "Неработоспособность приложения",
    "Мошеннические действия",
    "Спам",
]

def detect_language(text: str) -> str:
    if not text:
        return "RU"
    t = text.lower()
    # супер-лайт эвристика
    if re.search(r"\b(the|and|please|account|app)\b", t):
        return "ENG"
    if re.search(r"[әғқңөұүһі]", t):
        return "KZ"
    return "RU"

def stub_issue_type(text: str) -> str:
    t = (text or "").lower()
    if any(w in t for w in ["мошен", "fraud", "обман", "скам"]):
        return "Мошеннические действия"
    if any(w in t for w in ["не работает", "ошибка", "crash", "лагает", "вылет"]):
        return "Неработоспособность приложения"
    if any(w in t for w in ["спам", "реклама", "spam"]):
        return "Спам"
    if any(w in t for w in ["измен", "смен", "данн", "паспорт", "телефон", "email"]):
        return "Смена данных"
    if any(w in t for w in ["жалоб", "плохо", "ужас", "недоволен"]):
        return "Жалоба"
    if any(w in t for w in ["претенз", "компенсац", "верните", "возврат"]):
        return "Претензия"
    return "Консультация"

def stub_sentiment(text: str) -> str:
    t = (text or "").lower()
    neg = sum(1 for w in ["ужас", "плохо", "ненавижу", "обман", "верните", "кошмар", "не работает"] if w in t)
    pos = sum(1 for w in ["спасибо", "класс", "хорошо", "отлично"] if w in t)
    if neg > pos and neg >= 1:
        return "Негативный"
    if pos > neg and pos >= 1:
        return "Позитивный"
    return "Нейтральный"

def stub_priority(segment: str, issue_type: str, sentiment: str) -> int:
    # 1..10
    s = (segment or "").upper()
    base = 3
    if s == "VIP":
        base += 3
    if s == "PRIORITY":
        base += 2
    if issue_type in ["Мошеннические действия", "Неработоспособность приложения"]:
        base += 2
    if sentiment == "Негативный":
        base += 1
    return max(1, min(10, base))

def make_summary(text: str, issue_type: str) -> str:
    t = (text or "").strip()
    short = (t[:160] + "…") if len(t) > 160 else t
    action = {
        "Мошеннические действия": "Рекомендация: проверить транзакции/доступ, инициировать блокировку и эскалацию.",
        "Смена данных": "Рекомендация: запросить подтверждающие документы и выполнить смену данных по регламенту.",
        "Неработоспособность приложения": "Рекомендация: собрать детали (устройство/версия/скрин), проверить инциденты, предложить workaround.",
        "Жалоба": "Рекомендация: зафиксировать причину, предложить решение/компенсацию по политике сервиса.",
        "Претензия": "Рекомендация: проверить историю, подготовить официальный ответ и сроки решения.",
        "Спам": "Рекомендация: пометить как спам/закрыть, при необходимости — блокировка отправителя.",
        "Консультация": "Рекомендация: дать пошаговую инструкцию и ссылку на FAQ/регламент.",
    }.get(issue_type, "Рекомендация: уточнить детали и предложить стандартное решение.")
    return f"{issue_type}: {short}\n{action}"

def rr_key(office: str, need_vip: bool, lang: str, need_chief: bool) -> str:
    return f"{office}|{'VIP' if need_vip else 'NOVIP'}|{lang}|{'CHIEF' if need_chief else 'NORMAL'}"

def build_explanation(filters: Dict[str, Any], candidates: List[Dict[str, Any]], rr: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "filters": filters,
        "candidates": candidates,
        "round_robin": rr,
    }