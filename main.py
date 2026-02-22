#!/usr/bin/env python3
import argparse
import os
import sys
import time
import signal
import subprocess
import importlib
from pathlib import Path
from typing import Optional, List


ROOT = Path(__file__).resolve().parent
DEFAULT_API_HOST = "127.0.0.1"
DEFAULT_API_PORT = 8000
DEFAULT_UI_PORT = 8501


def _import_any(*names):
    last_err = None
    for n in names:
        try:
            return importlib.import_module(n)
        except Exception as e:
            last_err = e
    raise last_err


def run_pipeline() -> None:
    """
    Запускает:
      load_data.main()
      enrich_stub.main()
      route.main()
    """
    print("[main] Running pipeline: load_data -> enrich_stub -> route", flush=True)

    load_data = _import_any("load_data", "app.load_data")
    enrich_stub = _import_any("enrich_stub", "app.enrich_stub")
    route = _import_any("route", "app.route")

    # Если у тебя функции называются иначе — просто поменяй здесь.
    load_data.main()
    enrich_stub.main()
    route.main()

    print("[main] Pipeline finished ✅", flush=True)


def _start_subprocess(cmd: List[str], name: str, cwd: Optional[Path] = None) -> subprocess.Popen:
    """
    Запускает процесс и печатает команду.
    """
    print(f"[main] Starting {name}: {' '.join(cmd)}", flush=True)
    return subprocess.Popen(
        cmd,
        cwd=str(cwd or ROOT),
        env=os.environ.copy(),
        stdout=None,   # пусть вывод идёт в консоль
        stderr=None,
        shell=False,
    )


def run_api(host: str, port: int) -> None:
    """
    Запускает только API (блокирующе).
    """
    try:
        import uvicorn
    except Exception:
        print("[main] uvicorn not installed. Install: pip install uvicorn", flush=True)
        sys.exit(1)

    print(f"[main] Starting API on http://{host}:{port}", flush=True)
    uvicorn.run("app.api:app", host=host, port=port, reload=True)


def run_ui(api_url: str, ui_port: int) -> None:
    """
    Запускает только UI (streamlit) как subprocess (блокирующе).
    """
    # UI будет читать API_URL из переменной окружения, если ты её используешь.
    # Если в dashboard.py API_URL жёстко прописан — оставь как есть.
    env = os.environ.copy()
    env["FIRE_API_URL"] = api_url

    cmd = [
        sys.executable, "-m", "streamlit", "run",
        str(ROOT / "ui" / "dashboard.py"),
        "--server.port", str(ui_port),
    ]
    print(f"[main] Starting UI on http://127.0.0.1:{ui_port}", flush=True)
    subprocess.run(cmd, cwd=str(ROOT), env=env, check=False)


def run_all(host: str, port: int, ui_port: int, do_pipeline: bool) -> None:
    """
    Запускает pipeline (опционально) + API + UI одновременно.
    API и UI поднимаются как два процесса.
    """
    if do_pipeline:
        run_pipeline()

    api_cmd = [
        sys.executable, "-m", "uvicorn",
        "app.api:app",
        "--host", host,
        "--port", str(port),
        "--reload",
    ]
    ui_cmd = [
        sys.executable, "-m", "streamlit", "run",
        str(ROOT / "ui" / "dashboard.py"),
        "--server.port", str(ui_port),
    ]

    api_proc = _start_subprocess(api_cmd, "API")
    # дадим API пару секунд подняться
    time.sleep(2.0)
    ui_proc = _start_subprocess(ui_cmd, "UI")

    procs = [api_proc, ui_proc]

    def shutdown(*_):
        print("\n[main] Shutting down...", flush=True)
        for p in procs:
            try:
                if p.poll() is None:
                    p.terminate()
            except Exception:
                pass

        # ждём чуть-чуть, потом kill
        time.sleep(2.0)
        for p in procs:
            try:
                if p.poll() is None:
                    p.kill()
            except Exception:
                pass

        print("[main] Done ✅", flush=True)
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    print("[main] Running. Press Ctrl+C to stop.", flush=True)

    # держим main живым, и если один из процессов упал — завершаем всё
    while True:
        for p in procs:
            code = p.poll()
            if code is not None:
                print(f"[main] Process exited with code {code}. Stopping all.", flush=True)
                shutdown()
        time.sleep(0.5)


def main():
    parser = argparse.ArgumentParser(description="FIRE launcher (pipeline / api / ui / all)")
    parser.add_argument(
        "mode",
        choices=["pipeline", "api", "ui", "all"],
        help="What to run",
    )
    parser.add_argument("--host", default=DEFAULT_API_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_API_PORT)
    parser.add_argument("--ui-port", type=int, default=DEFAULT_UI_PORT)
    parser.add_argument("--no-pipeline", action="store_true", help="Skip pipeline when mode=all")

    args = parser.parse_args()

    api_url = f"http://{args.host}:{args.port}"

    if args.mode == "pipeline":
        run_pipeline()
    elif args.mode == "api":
        run_api(args.host, args.port)
    elif args.mode == "ui":
        run_ui(api_url=api_url, ui_port=args.ui_port)
    elif args.mode == "all":
        run_all(args.host, args.port, args.ui_port, do_pipeline=(not args.no_pipeline))


if __name__ == "__main__":
    main()