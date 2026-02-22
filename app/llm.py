import os
import re
import json
import time
import requests
from typing import Any, Dict, Optional


def _base_url() -> str:
    return os.getenv("OLLAMA_BASE_URL", "http://localhost:11434").rstrip("/")


def _model() -> str:
    return os.getenv("OLLAMA_MODEL", "qwen3:8b")


def _timeout() -> int:
    try:
        return int(os.getenv("OLLAMA_TIMEOUT", "40"))
    except:
        return 40


def _temperature() -> float:
    try:
        return float(os.getenv("OLLAMA_TEMPERATURE", "0.2"))
    except:
        return 0.2


def _retries() -> int:
    try:
        return int(os.getenv("OLLAMA_RETRIES", "1"))
    except:
        return 1


def _post(url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    # connect timeout 5s, read timeout _timeout()
    r = requests.post(url, json=payload, timeout=(5, _timeout()))
    r.raise_for_status()
    return r.json()


def _call_ollama_chat(system: str, user: str) -> str:
    url = f"{_base_url()}/api/chat"
    payload = {
        "model": _model(),
        "stream": False,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "options": {"temperature": _temperature()},
    }
    data = _post(url, payload)
    msg = (data.get("message") or {}).get("content")
    if msg is None:
        msg = data.get("response")
    return (msg or "").strip()


def llm_text(system: str, user: str) -> str:
    last_err = ""
    for attempt in range(_retries() + 1):
        try:
            out = _call_ollama_chat(system, user)
            if out:
                return out
            last_err = "empty response"
        except Exception as e:
            last_err = str(e)
            time.sleep(0.2 * (attempt + 1))
    raise RuntimeError(f"Ollama call failed: {last_err}")


def llm_template(system: str, user: str, template: str) -> str:
    hard_user = (
        f"{user}\n\n"
        f"Return ONLY in this exact format (no markdown, no extra lines):\n{template}\n"
    )
    return llm_text(system, hard_user)