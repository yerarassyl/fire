import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import json
import math

API_URL = "http://127.0.0.1:8000"

st.set_page_config(page_title="FIRE Dashboard", layout="wide")

# ✅ Start + твои 3 вкладки сохранены
tabs = st.tabs(["🚀 Start", "📊 NL Analytics", "🧠 Explain Routing", "🗺 Geo Heatmap"])


def safe_int(x, default=0) -> int:
    try:
        if x is None:
            return default
        if isinstance(x, float) and math.isnan(x):
            return default
        return int(float(x))
    except Exception:
        return default


def pick_first_existing(row_or_df, candidates, default=None):
    """Берет первое существующее поле из списка."""
    if isinstance(row_or_df, pd.DataFrame):
        for c in candidates:
            if c in row_or_df.columns:
                return c
        return None
    else:
        for c in candidates:
            if c in row_or_df and row_or_df.get(c) not in [None, ""]:
                return row_or_df.get(c)
        return default


# ---------------- TAB 0: START (Pipeline) ----------------
with tabs[0]:
    st.title("🚀 FIRE: загрузка tickets.csv и запуск pipeline")
    st.write("Выбери **tickets.csv** и нажми кнопку.")

    tickets_file = st.file_uploader("tickets.csv (обязательно)", type=["csv"])
    run = st.button("▶️ Обработать всё", type="primary")


    if run:
        if not tickets_file:
            st.error("Нужно загрузить tickets.csv")
        else:
            files = {"tickets": tickets_file}
            with st.spinner("Запускаю обработку..."):
                try:
                    r = requests.post(f"{API_URL}/pipeline/run", files=files, timeout=600)
                except Exception as e:
                    st.error(f"API не отвечает: {e}")
                    st.stop()

            if r.status_code != 200:
                st.error(f"Ошибка API: {r.status_code}")
                st.write(r.text)
            else:
                data = r.json()
                if data.get("ok"):
                    st.success("✅ Готово! Pipeline завершён.")
                    if data.get("message"):
                        st.caption(data["message"])
                else:
                    st.error(data.get("message", "Pipeline error"))


# ---------------- TAB 1: NL Analytics ----------------
with tabs[1]:
    st.title("📊 NL Analytics")

    # ✅ Примеры НЕ скрыты, только те что ты попросил
    st.markdown("""
### Примеры запросов
""")

    examples = [
        "Покажи таблицу по менеджерам в городе Караганда",
        "Покажи доли типов обращений в городе Алматы",
        "Покажи распределение по городам с разбивкой по типам",
        "топ 10 городов по типам",
        "Покажи распределение по офисам где приоритет >= 8",
        "Покажи доли типов обращений язык ENG",
    ]

    if "nlq_query" not in st.session_state:
        st.session_state["nlq_query"] = "Покажи распределение типов обращений по городам"

    b1, b2, b3 = st.columns(3)
    btn_cols = [b1, b2, b3]
    for i, ex in enumerate(examples):
        if btn_cols[i % 3].button(ex, key=f"ex_btn_{i}"):
            st.session_state["nlq_query"] = ex

    q = st.text_input(
        "Запрос",
        key="nlq_query",
        placeholder="Например: Покажи таблицу по менеджерам в городе Караганда",
    )

    if st.button("Построить", type="primary"):
        try:
            data = requests.post(f"{API_URL}/nlq", json={"query": q}, timeout=30).json()
        except Exception as e:
            st.error(f"API не отвечает: {e}")
            st.stop()

        if not data.get("ok"):
            st.warning(data.get("message", "Не понял запрос"))
            st.stop()

        st.success(data.get("message", "OK"))

        # ✅ совместимость: columns может быть, а может не быть
        if "columns" in data and data.get("columns"):
            df = pd.DataFrame(data.get("rows", []), columns=data["columns"])
        else:
            df = pd.DataFrame(data.get("rows", []))

        st.dataframe(df, use_container_width=True)

        chart = data.get("chart") or {}
        kind = chart.get("kind")

        if kind == "table":
            st.info("Показана таблица (без графика).")
        elif kind == "bar":
            st.plotly_chart(px.bar(df, x=chart["x"], y=chart["y"]), use_container_width=True)
        elif kind == "pie":
            st.plotly_chart(px.pie(df, names=chart["label"], values=chart["value"]), use_container_width=True)
        elif kind == "stacked_bar":
            st.plotly_chart(px.bar(df, x=chart["x"], y=chart["y"], color=chart["series"]), use_container_width=True)
        elif kind == "line":
            if chart.get("series"):
                st.plotly_chart(px.line(df, x=chart["x"], y=chart["y"], color=chart["series"]), use_container_width=True)
            else:
                st.plotly_chart(px.line(df, x=chart["x"], y=chart["y"]), use_container_width=True)
        else:
            st.warning(f"Неизвестный тип графика: {kind}")


# ---------------- TAB 2: Explain Routing ----------------
with tabs[2]:
    st.title("🧠 Explainable Routing Viewer")
    st.caption("Выбери тикет — увидишь причины назначения (JSON) + детали.")

    try:
        assignments = requests.get(f"{API_URL}/assignments", timeout=20).json()
    except Exception as e:
        st.error(f"API не отвечает: {e}")
        st.stop()

    # ✅ совместимость: если API вернёт {"ok": True, "rows": [...]}
    if isinstance(assignments, dict) and "rows" in assignments:
        if not assignments.get("ok", True):
            st.error(assignments.get("message", "Ошибка /assignments"))
            st.stop()
        assignments = assignments.get("rows", [])

    if not assignments:
        st.warning("Нет назначений. Запусти pipeline на вкладке 🚀 Start или: python -m app.load_data → enrich_stub → route")
        st.stop()

    df = pd.DataFrame(assignments)

    # ✅ НОРМАЛИЗАЦИЯ ИМЁН КОЛОНОК, чтобы не падало
    # manager_name
    if "manager_name" not in df.columns:
        src = pick_first_existing(df, ["manager_name", "manager", "full_name", "manager_full_name"])
        if src:
            df["manager_name"] = df[src]
        else:
            df["manager_name"] = "UNKNOWN"

    # algorithm
    if "algorithm" not in df.columns:
        src = pick_first_existing(df, ["algorithm", "algo", "routing_algo", "routing_algorithm"])
        if src:
            df["algorithm"] = df[src]
        else:
            df["algorithm"] = "UNKNOWN"

    # базовые поля на всякий случай
    for col, default in [
        ("office_name", "UNKNOWN"),
        ("language", "UNKNOWN"),
        ("issue_type", "UNKNOWN"),
        ("city", "UNKNOWN"),
        ("priority", 0),
    ]:
        if col not in df.columns:
            df[col] = default

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        office_f = st.selectbox("Офис", ["ALL"] + sorted(df["office_name"].dropna().unique().tolist()))
    with c2:
        lang_f = st.selectbox("Язык", ["ALL"] + sorted(df["language"].dropna().unique().tolist()))
    with c3:
        issue_f = st.selectbox("Тип", ["ALL"] + sorted(df["issue_type"].dropna().unique().tolist()))
    with c4:
        min_pr = st.slider("Min priority", 0, 10, 0)

    view = df.copy()
    if office_f != "ALL":
        view = view[view["office_name"] == office_f]
    if lang_f != "ALL":
        view = view[view["language"] == lang_f]
    if issue_f != "ALL":
        view = view[view["issue_type"] == issue_f]
    view = view[view["priority"].fillna(0).astype(int) >= min_pr]

    st.markdown("### Назначения")

    desired_cols = ["ticket_id", "assigned_at", "office_name", "manager_name", "issue_type", "language", "priority", "city", "algorithm"]
    existing_cols = [c for c in desired_cols if c in view.columns]

    st.dataframe(
        view[existing_cols],
        use_container_width=True,
        height=260
    )

    if view.empty:
        st.info("По фильтрам ничего не найдено.")
        st.stop()

    selected_id = st.selectbox("ticket_id", view["ticket_id"].tolist())
    row = view[view["ticket_id"] == selected_id].iloc[0]

    st.subheader("📌 Детали")
    a, b, c = st.columns(3)
    with a:
        st.write("**ticket_id:**", safe_int(row.get("ticket_id")))
        st.write("**city:**", row.get("city", ""))
        st.write("**segment:**", row.get("segment", ""))
    with b:
        st.write("**issue_type:**", row.get("issue_type", ""))
        st.write("**sentiment:**", row.get("sentiment", ""))
        st.write("**priority:**", safe_int(row.get("priority")))
        st.write("**language:**", row.get("language", ""))
    with c:
        st.write("**office:**", row.get("office_name", ""))
        st.write("**manager:**", row.get("manager_name", ""))
        st.write("**position:**", row.get("position", ""))
        st.write("**manager_load:**", safe_int(row.get("current_load", 0)))
        st.write("**algorithm:**", row.get("algorithm", "UNKNOWN"))

    # ✅ AI summary + рекомендации в деталях
    st.subheader("🤖 AI Summary & Recommendations")
    ai_summary = row.get("ai_summary") or row.get("summary") or ""
    ai_reco = row.get("ai_recommendation") or row.get("recommended_action") or row.get("recommendation") or ""

    if not ai_summary and not ai_reco:
        st.info("AI summary/рекомендации пока нет (появится после enrich_stub или если поля есть в ticket_ai).")
    else:
        if ai_summary:
            st.write("**AI Summary:**")
            st.write(ai_summary)
        if ai_reco:
            st.write("**AI Recommendation:**")
            st.write(ai_reco)

    st.subheader("🔍 Explanation JSON")
    try:
        explanation = json.loads(row.get("explanation") or "{}")
    except Exception:
        explanation = {"raw": row.get("explanation")}
    st.json(explanation)


# ---------------- TAB 3: Geo Heatmap ----------------
with tabs[3]:
    st.title("🗺 Geo Map")

    try:
        pts = requests.get(f"{API_URL}/geo_points", timeout=25).json()
        offices = requests.get(f"{API_URL}/office_points", timeout=25).json()
    except Exception as e:
        st.error(f"API не отвечает: {e}")
        st.stop()

    if not pts:
        st.warning("Нет geo-точек. Проверь что ticket_geo заполнен.")
        st.stop()

    df = pd.DataFrame(pts)
    df_off = pd.DataFrame(offices) if offices else pd.DataFrame(columns=["office_name", "lon", "lat"])

    # ВАЖНО: приводим координаты офисов к float, иначе Plotly иногда молча не рисует
    if not df_off.empty:
        df_off["lon"] = pd.to_numeric(df_off["lon"], errors="coerce")
        df_off["lat"] = pd.to_numeric(df_off["lat"], errors="coerce")
        df_off = df_off.dropna(subset=["lon", "lat"])

    f1, f2, f3, f4 = st.columns(4)
    with f1:
        office = st.selectbox("Office", ["ALL"] + sorted(df["office_name"].dropna().unique().tolist()))
    with f2:
        issue = st.selectbox("Issue type", ["ALL"] + sorted(df["issue_type"].dropna().unique().tolist()))
    with f3:
        min_pr = st.slider("Min priority", 0, 10, 0, key="geo_min_pr")
    with f4:
        show_lines = st.checkbox("Show lines to office", value=True)

    view = df.copy()
    if office != "ALL":
        view = view[view["office_name"] == office]
    if issue != "ALL":
        view = view[view["issue_type"] == issue]
    view = view[view["priority"] >= min_pr]

    st.dataframe(
        view[["ticket_id", "city", "issue_type", "priority", "office_name", "geocode_status", "distance_m"]],
        use_container_width=True,
        height=240
    )

    fig = px.scatter_mapbox(
        view,
        lat="lat",
        lon="lon",
        color="office_name",
        size="priority",
        hover_data=["ticket_id", "city", "issue_type", "priority", "geocode_status", "distance_m"],
        zoom=4,
        height=700,
    )
    fig.update_layout(mapbox_style="open-street-map")

    # ✅ Офисы “иголочками”
    if df_off.empty:
        st.warning("⚠️ Офисы не отрисованы: API вернул 0 офисов. Проверь что business_units.location заполнен.")
    else:
        fig.add_scattermapbox(
            lat=df_off["lat"],
            lon=df_off["lon"],
            mode="markers+text",
            marker=dict(size=5, color="black"),
            text=df_off["office_name"],
            textfont=dict(color="black"), 
            textposition="top center",
            hoverinfo="text",
            name="Offices",
        )

    # Линии
    if show_lines:
        lines = view.dropna(subset=["office_lon", "office_lat"]).copy()

        MAX_LINES = 300
        if len(lines) > MAX_LINES:
            lines = lines.sample(MAX_LINES, random_state=42)

        def bucket(dist_m):
            if dist_m is None:
                return "UNKNOWN"
            try:
                d = float(dist_m)
                if math.isnan(d):
                    return "UNKNOWN"
            except Exception:
                return "UNKNOWN"
            if d < 5000:
                return "near_<5km"
            if d <= 200000:
                return "mid_5-200km"
            return "far_>200km"

        lines["dist_bucket"] = lines["distance_m"].apply(bucket)

        bucket_colors = {
            "near_<5km": "green",
            "mid_5-200km": "orange",
            "far_>200km": "red",
            "UNKNOWN": "gray",
        }

        for bname, color in bucket_colors.items():
            chunk = lines[lines["dist_bucket"] == bname]
            if chunk.empty:
                continue

            lat_list, lon_list, text_list = [], [], []
            for _, r in chunk.iterrows():
                lat_list += [r["lat"], r["office_lat"], None]
                lon_list += [r["lon"], r["office_lon"], None]
                dist = safe_int(r.get("distance_m"), 0)
                text_list += [f"ticket {r['ticket_id']} → {r['office_name']} ({dist}m)", "", None]

            fig.add_scattermapbox(
                lat=lat_list,
                lon=lon_list,
                mode="lines",
                line=dict(width=2, color=color),
                hoverinfo="text",
                text=text_list,
                name=f"Route {bname}",
                opacity=0.55,
            )

    st.plotly_chart(fig, use_container_width=True)