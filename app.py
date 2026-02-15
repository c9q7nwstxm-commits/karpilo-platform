import streamlit as st
import os
import re
import math
import datetime as dt
import pandas as pd
from sqlalchemy import create_engine, text
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

st.set_page_config(
    page_title="Karpilo Trucking Controls",
    page_icon="logo.png",
    layout="wide"
)

# --- DATABASE CONFIG ---
DB_PATH = "karpilo.db"
UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

engine = create_engine(f"sqlite:///{DB_PATH}", future=True)



# --- DATABASE CONFIG ---
DB_PATH = "karpilo.db"
UPLOAD_DIR = "uploads"

import os
os.makedirs(UPLOAD_DIR, exist_ok=True)

from sqlalchemy import create_engine

DB_PATH = "karpilo.db"
UPLOAD_DIR = "uploads"

import os
os.makedirs(UPLOAD_DIR, exist_ok=True)

engine = create_engine(f"sqlite:///{DB_PATH}", future=True)
st.set_page_config(layout="wide")

# --- HIDE SIDEBAR COLLAPSE ARROW + STYLE ---
st.markdown("""
<style>
[data-testid="collapsedControl"] {display: none;}
section[data-testid="stSidebar"] h1 {
    font-size: 26px;
    color: #ff2b2b;
    font-weight: 800;
}
</style>
""", unsafe_allow_html=True)


# ----------------------------
# STYLES (sidebar red text nav)
# ----------------------------
st.markdown(
    """
<style>
/* Sidebar buttons look like red text tabs (no bullets) */
section[data-testid="stSidebar"] button {
  background: transparent !important;
  border: none !important;
  padding: 0.35rem 0 !important;
  margin: 0 !important;
}
section[data-testid="stSidebar"] button p {
  color: #ff3b30 !important;
  font-weight: 800 !important;
  font-size: 1.05rem !important;
}
section[data-testid="stSidebar"] button:hover p {
  text-decoration: underline !important;
}
</style>
""",
    unsafe_allow_html=True,
)

# ----------------------------
# HELPERS
# ----------------------------
def now_iso():
    return dt.datetime.now().isoformat(timespec="seconds")


def money(x):
    try:
        x = 0.0 if x is None or (isinstance(x, float) and math.isnan(x)) else float(x)
    except Exception:
        x = 0.0
    return f"${x:,.2f}"


def safe_filename(name: str) -> str:
    name = (name or "").strip()
    name = re.sub(r"[^a-zA-Z0-9._-]+", "_", name)
    return name[:120] if name else "upload"


def df_from_query(sql: str, params: dict = None) -> pd.DataFrame:
    params = params or {}
    with engine.begin() as conn:
        rows = conn.execute(text(sql), params).mappings().all()
    return pd.DataFrame(rows)


def write_pdf_summary(summary: dict, out_path: str):
    c = canvas.Canvas(out_path, pagesize=letter)
    width, height = letter
    y = height - 50

    c.setFont("Helvetica-Bold", 14)
    c.drawString(50, y, "Karpilo Platform — Summary Export")
    y -= 22

    c.setFont("Helvetica", 10)
    c.drawString(50, y, f"Generated: {dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    y -= 18

    c.setFont("Helvetica-Bold", 11)
    c.drawString(50, y, "Key Totals")
    y -= 16

    c.setFont("Helvetica", 10)
    for k, v in summary.items():
        c.drawString(55, y, f"{k}: {v}")
        y -= 14
        if y < 60:
            c.showPage()
            y = height - 50
            c.setFont("Helvetica", 10)

    c.showPage()
    c.save()


def tier_pct(gross_week: float) -> float:
    if gross_week <= 3999.99:
        return 0.33
    elif gross_week <= 5999.99:
        return 0.35
    return 0.37


def week_end_sunday(d: pd.Timestamp) -> pd.Timestamp:
    # Monday=0 ... Sunday=6
    return d + pd.to_timedelta((6 - d.weekday()), unit="D")


# ----------------------------
# DB INIT
# ----------------------------
def init_db():
    with engine.begin() as conn:
        # Loads: delivery_date drives pay-week (Sat–Sun deliveries)
        conn.execute(
            text(
                """
CREATE TABLE IF NOT EXISTS loads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    delivery_date TEXT NOT NULL,

    order_number TEXT,
    trip_number TEXT,
    dispatched_miles REAL,

    empty_hub_start REAL,
    empty_hub_end REAL,
    loaded_hub_start REAL,
    loaded_hub_end REAL,
    empty_miles REAL,
    loaded_miles REAL,
    total_hub_miles REAL,

    linehaul_dollars REAL,
    fsc_dollars REAL,
    accessorials_dollars REAL,

    notes TEXT,
    created_at TEXT NOT NULL
);
"""
            )
        )

        conn.execute(
            text(
                """
CREATE TABLE IF NOT EXISTS receipts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    receipt_date TEXT NOT NULL,
    vendor TEXT,
    category TEXT,
    amount REAL NOT NULL,
    deductible_pct INTEGER NOT NULL,
    deductible_amount REAL NOT NULL,
    filename TEXT,
    linked_load_id INTEGER,
    notes TEXT,
    created_at TEXT NOT NULL
);
"""
            )
        )

        conn.execute(
            text(
                """
CREATE TABLE IF NOT EXISTS fuel (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fuel_date TEXT NOT NULL,
    location TEXT,
    gallons REAL NOT NULL,
    price_per_gal REAL NOT NULL,
    total_dollars REAL NOT NULL,
    odometer REAL,
    linked_load_id INTEGER,
    notes TEXT,
    created_at TEXT NOT NULL
);
"""
            )
        )

        conn.execute(
            text(
                """
CREATE TABLE IF NOT EXISTS maintenance (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    maint_date TEXT NOT NULL,
    item TEXT NOT NULL,
    vendor TEXT,
    category TEXT,
    cost REAL NOT NULL,
    odometer REAL,
    linked_load_id INTEGER,
    notes TEXT,
    created_at TEXT NOT NULL
);
"""
            )
        )

        # Non-driving pay not tied to dispatch
        conn.execute(
            text(
                """
CREATE TABLE IF NOT EXISTS non_driving_pay (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pay_date TEXT NOT NULL,
    description TEXT NOT NULL,
    amount REAL NOT NULL,
    notes TEXT,
    created_at TEXT NOT NULL
);
"""
            )
        )

        # Per-diem entries (fixed: $69/day, 80% deductible)
        conn.execute(
            text(
                """
CREATE TABLE IF NOT EXISTS per_diem (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    period_start TEXT NOT NULL,
    period_end TEXT NOT NULL,
    working_days INTEGER NOT NULL,
    rate_per_day REAL NOT NULL,
    deductible_pct INTEGER NOT NULL,
    deductible_amount REAL NOT NULL,
    notes TEXT,
    created_at TEXT NOT NULL
);
"""
            )
        )

        # Pay-period deductions (reduce take-home; not automatically tax deductions)
        conn.execute(
            text(
                """
CREATE TABLE IF NOT EXISTS deductions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    deduct_date TEXT NOT NULL,
    label TEXT NOT NULL,
    amount REAL NOT NULL,
    notes TEXT,
    created_at TEXT NOT NULL
);
"""
            )
        )

        # Uploads
        conn.execute(
            text(
                """
CREATE TABLE IF NOT EXISTS bol_uploads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    linked_load_id INTEGER,
    filename TEXT NOT NULL,
    uploaded_at TEXT NOT NULL
);
"""
            )
        )
        conn.execute(
            text(
                """
CREATE TABLE IF NOT EXISTS log_uploads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    linked_load_id INTEGER,
    filename TEXT NOT NULL,
    uploaded_at TEXT NOT NULL
);
"""
            )
        )


init_db()

# ----------------------------
# SIDEBAR (global date range + red text nav)
# ----------------------------
st.sidebar.markdown("## Karpilo Trucking Control Panel")
st.sidebar.caption("Loads • FSC • Receipts • Per Diem • Taxes • Maintenance")
st.sidebar.divider()

default_start = dt.date.today().replace(day=1)
default_end = dt.date.today()

start_date = st.sidebar.date_input("Start date", value=default_start, key="global_start")
end_date = st.sidebar.date_input("End date", value=default_end, key="global_end")

date_start_s = start_date.strftime("%Y-%m-%d")
date_end_s = end_date.strftime("%Y-%m-%d")

st.sidebar.divider()

if "page" not in st.session_state:
    st.session_state.page = "Loads"


def go(p):
    st.session_state.page = p


st.sidebar.button("Loads", on_click=go, args=("Loads",), key="nav_loads")
st.sidebar.button("Receipts", on_click=go, args=("Receipts",), key="nav_receipts")
st.sidebar.button("Fuel", on_click=go, args=("Fuel",), key="nav_fuel")
st.sidebar.button("Maintenance", on_click=go, args=("Maintenance",), key="nav_maint")
st.sidebar.button("Per-Diem", on_click=go, args=("Per-Diem",), key="nav_pd")
st.sidebar.button("Deductions", on_click=go, args=("Deductions",), key="nav_ded")
st.sidebar.button("Dashboard", on_click=go, args=("Dashboard",), key="nav_dash")
st.sidebar.button("Taxes", on_click=go, args=("Taxes",), key="nav_taxes")
st.sidebar.button("Export/Backup", on_click=go, args=("Export/Backup",), key="nav_export")

page = st.session_state.page

# ----------------------------
# MAIN HEADER (logo top, not sidebar)
# ----------------------------
if os.path.exists("logo.png"):
    st.image("logo.png", width=520)
st.divider()

today = dt.date.today()

# ======================================================================
# PAGE: LOADS
# ======================================================================
if page == "Loads":
    st.subheader("Loads (Delivery-Based — Weekly Pay Uses Delivery Date Sat–Sun)")

    load_tabs = st.tabs(["Entry", "Upload BOL", "Upload Logs"])

    # ---------- Entry ----------
    with load_tabs[0]:
        left, right = st.columns([1, 1], gap="large")

        with left:
            st.markdown("### Add / Save a Load")

            delivery_date = st.date_input(
                "Delivery date (counts for weekly pay)",
                value=today,
                key="delivery_date",
            )

            order_number = st.text_input("Order #", key="order_number")
            trip_number = st.text_input("Trip #", key="trip_number")

            dispatched_miles = st.number_input(
                "Dispatched miles",
                min_value=0.0,
                step=1.0,
                format="%.1f",
                key="dispatched_miles",
            )

            st.markdown("#### Hub Miles (manual inputs)")
            h1, h2 = st.columns(2)
            with h1:
                empty_hub_start = st.number_input("Empty Hub Start", min_value=0.0, step=1.0, format="%.1f", key="ehs")
                empty_hub_end = st.number_input("Empty Hub End", min_value=0.0, step=1.0, format="%.1f", key="ehe")
            with h2:
                loaded_hub_start = st.number_input("Loaded Hub Start", min_value=0.0, step=1.0, format="%.1f", key="lhs")
                loaded_hub_end = st.number_input("Loaded Hub End", min_value=0.0, step=1.0, format="%.1f", key="lhe")

            empty_miles = max(0.0, float(empty_hub_end) - float(empty_hub_start))
            loaded_miles = max(0.0, float(loaded_hub_end) - float(loaded_hub_start))
            total_hub_miles = empty_miles + loaded_miles

            deadhead_miles = empty_miles
            deadhead_pct = (deadhead_miles / total_hub_miles) if total_hub_miles > 0 else 0.0
            variance_vs_dispatch = (total_hub_miles - float(dispatched_miles)) if dispatched_miles is not None else 0.0

            st.info(
                f"Empty: **{empty_miles:.1f}** | Loaded: **{loaded_miles:.1f}** | Total Hub: **{total_hub_miles:.1f}**\n\n"
                f"Deadhead: **{deadhead_miles:.1f}** ({deadhead_pct:.1%})  |  Variance vs Dispatched: **{variance_vs_dispatch:+.1f}** miles"
            )

            st.markdown("#### Pay (per load)")
            linehaul = st.number_input("Line Haul $", min_value=0.0, step=10.0, format="%.2f", key="lh")
            fsc = st.number_input("FSC $ (whole dollars per load)", min_value=0.0, step=1.0, format="%.2f", key="fsc")
            access = st.number_input("Accessorials $", min_value=0.0, step=1.0, format="%.2f", key="acc")

            gross = float(linehaul + fsc + access)
            rpm_hub = ((linehaul + access) / total_hub_miles) if total_hub_miles > 0 else 0.0
            rpm_dispatch = ((linehaul + access) / dispatched_miles) if dispatched_miles > 0 else 0.0

            st.info(
                f"Gross (LH+FSC+Accessorials): **{money(gross)}**  |  "
                f"RPM Hub ((LH+Acc)/Hub): **${rpm_hub:,.3f}**  |  "
                f"RPM Dispatch ((LH+Acc)/Dispatched): **${rpm_dispatch:,.3f}**"
            )

            notes = st.text_area("Notes (optional)", key="load_notes")

            if st.button("Save Load", type="primary"):
                with engine.begin() as conn:
                    conn.execute(
                        text(
                            """
INSERT INTO loads (
    delivery_date,
    order_number, trip_number, dispatched_miles,
    empty_hub_start, empty_hub_end, loaded_hub_start, loaded_hub_end,
    empty_miles, loaded_miles, total_hub_miles,
    linehaul_dollars, fsc_dollars, accessorials_dollars,
    notes, created_at
) VALUES (
    :dd,
    :ord,:trip,:dm,
    :ehs,:ehe,:lhs,:lhe,
    :em,:lm,:thm,
    :lh,:fsc,:acc,
    :n,:ts
)
"""
                        ),
                        dict(
                            dd=delivery_date.strftime("%Y-%m-%d"),
                            ord=order_number.strip() if order_number else None,
                            trip=trip_number.strip() if trip_number else None,
                            dm=float(dispatched_miles),
                            ehs=float(empty_hub_start),
                            ehe=float(empty_hub_end),
                            lhs=float(loaded_hub_start),
                            lhe=float(loaded_hub_end),
                            em=float(empty_miles),
                            lm=float(loaded_miles),
                            thm=float(total_hub_miles),
                            lh=float(linehaul),
                            fsc=float(fsc),
                            acc=float(access),
                            n=notes.strip() if notes else None,
                            ts=now_iso(),
                        ),
                    )
                st.success("Load saved.")

        with right:
            st.markdown("### Loads in Date Range (by Delivery Date)")

            df_loads = df_from_query(
                """
SELECT *,
       (COALESCE(linehaul_dollars,0)+COALESCE(fsc_dollars,0)+COALESCE(accessorials_dollars,0)) AS gross,
       CASE WHEN COALESCE(total_hub_miles,0) > 0
            THEN (COALESCE(linehaul_dollars,0)+COALESCE(accessorials_dollars,0))/total_hub_miles
            ELSE 0 END AS rpm_hub,
       CASE WHEN COALESCE(dispatched_miles,0) > 0
            THEN (COALESCE(linehaul_dollars,0)+COALESCE(accessorials_dollars,0))/dispatched_miles
            ELSE 0 END AS rpm_dispatch,
       (COALESCE(total_hub_miles,0) - COALESCE(dispatched_miles,0)) AS variance_vs_dispatch,
       CASE WHEN COALESCE(total_hub_miles,0) > 0
            THEN COALESCE(empty_miles,0)/total_hub_miles
            ELSE 0 END AS deadhead_pct
FROM loads
WHERE delivery_date BETWEEN :a AND :b
ORDER BY delivery_date DESC, id DESC
""",
                {"a": date_start_s, "b": date_end_s},
            )

            if df_loads.empty:
                st.info("No loads saved in this date range yet.")
            else:
                show_cols = [
                    "id",
                    "delivery_date",
                    "order_number",
                    "trip_number",
                    "dispatched_miles",
                    "empty_miles",
                    "loaded_miles",
                    "total_hub_miles",
                    "deadhead_pct",
                    "variance_vs_dispatch",
                    "linehaul_dollars",
                    "fsc_dollars",
                    "accessorials_dollars",
                    "gross",
                    "rpm_hub",
                    "rpm_dispatch",
                    "notes",
                ]
                df_show = df_loads[show_cols].copy()
                df_show["deadhead_pct"] = df_show["deadhead_pct"].apply(lambda x: f"{float(x):.2%}")
                st.dataframe(df_show, use_container_width=True)

            st.markdown("#### Delete a Load (careful)")
            del_id = st.number_input("Load ID to delete", min_value=0, step=1, value=0, key="del_load_id")
            if st.button("Delete Load"):
                if del_id <= 0:
                    st.error("Enter a valid load id.")
                else:
                    with engine.begin() as conn:
                        conn.execute(text("DELETE FROM loads WHERE id = :i"), {"i": int(del_id)})
                    st.warning(f"Deleted load id {int(del_id)} (if it existed).")

    # ---------- Upload BOL ----------
    with load_tabs[1]:
        st.markdown("### Upload BOL")
        bol_link = st.number_input("Link to Load ID (optional)", min_value=0, step=1, value=0, key="bol_link")
        bol_up = st.file_uploader("Upload BOL (pdf/jpg/png)", type=["pdf", "jpg", "jpeg", "png"], key="bol_file")

        if st.button("Save BOL Upload"):
            if bol_up is None:
                st.error("Choose a file first.")
            else:
                safe = safe_filename(bol_up.name)
                stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"bol_{stamp}_{safe}"
                path = os.path.join(UPLOAD_DIR, filename)
                with open(path, "wb") as f:
                    f.write(bol_up.getbuffer())

                with engine.begin() as conn:
                    conn.execute(
                        text("INSERT INTO bol_uploads (linked_load_id, filename, uploaded_at) VALUES (:lid,:fn,:ts)"),
                        {"lid": int(bol_link) if bol_link > 0 else None, "fn": filename, "ts": now_iso()},
                    )
                st.success("BOL uploaded.")

        df_bol = df_from_query("SELECT * FROM bol_uploads ORDER BY id DESC LIMIT 50")
        if not df_bol.empty:
            st.dataframe(df_bol, use_container_width=True)

    # ---------- Upload Logs ----------
    with load_tabs[2]:
        st.markdown("### Upload Logs")
        log_link = st.number_input("Link to Load ID (optional)", min_value=0, step=1, value=0, key="log_link")
        log_up = st.file_uploader("Upload Logs (pdf/jpg/png)", type=["pdf", "jpg", "jpeg", "png"], key="log_file")

        if st.button("Save Logs Upload"):
            if log_up is None:
                st.error("Choose a file first.")
            else:
                safe = safe_filename(log_up.name)
                stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"logs_{stamp}_{safe}"
                path = os.path.join(UPLOAD_DIR, filename)
                with open(path, "wb") as f:
                    f.write(log_up.getbuffer())

                with engine.begin() as conn:
                    conn.execute(
                        text("INSERT INTO log_uploads (linked_load_id, filename, uploaded_at) VALUES (:lid,:fn,:ts)"),
                        {"lid": int(log_link) if log_link > 0 else None, "fn": filename, "ts": now_iso()},
                    )
                st.success("Logs uploaded.")

        df_logs = df_from_query("SELECT * FROM log_uploads ORDER BY id DESC LIMIT 50")
        if not df_logs.empty:
            st.dataframe(df_logs, use_container_width=True)

# ======================================================================
# PAGE: RECEIPTS
# ======================================================================
elif page == "Receipts":
    st.subheader("Receipts (Deductible % in 5% increments — reduces taxable income)")

    left, right = st.columns([1, 1], gap="large")

    with left:
        r_date = st.date_input("Receipt date", value=today, key="r_date")
        vendor = st.text_input("Vendor / Store", key="r_vendor")
        category = st.text_input("Category", key="r_cat")
        amount = st.number_input("Amount $", min_value=0.0, step=0.01, format="%.2f", key="r_amt")

        pct = st.slider("Deductible %", min_value=0, max_value=100, step=5, value=100, key="r_pct")
        ded = round(amount * (pct / 100.0), 2)
        st.info(f"Deductible amount: **{money(ded)}**")

        link_load = st.number_input("Link to Load ID (optional)", min_value=0, step=1, value=0, key="r_link")
        notes = st.text_area("Notes (optional)", key="r_notes")
        up = st.file_uploader("Upload receipt (jpg/png/pdf)", type=["jpg", "jpeg", "png", "pdf"], key="r_file")

        if st.button("Save Receipt", type="primary"):
            if amount <= 0:
                st.error("Enter amount > 0.")
            else:
                filename = None
                if up is not None:
                    safe = safe_filename(up.name)
                    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
                    filename = f"receipt_{stamp}_{safe}"
                    path = os.path.join(UPLOAD_DIR, filename)
                    with open(path, "wb") as f:
                        f.write(up.getbuffer())

                with engine.begin() as conn:
                    conn.execute(
                        text(
                            """
INSERT INTO receipts (
    receipt_date, vendor, category, amount,
    deductible_pct, deductible_amount,
    filename, linked_load_id, notes,
    created_at
) VALUES (
    :d, :v, :c, :a,
    :p, :da,
    :fn, :lid, :n,
    :ts
)
"""
                        ),
                        dict(
                            d=r_date.strftime("%Y-%m-%d"),
                            v=vendor.strip() if vendor else None,
                            c=category.strip() if category else None,
                            a=float(amount),
                            p=int(pct),
                            da=float(ded),
                            fn=filename,
                            lid=int(link_load) if link_load > 0 else None,
                            n=notes.strip() if notes else None,
                            ts=now_iso(),
                        ),
                    )
                st.success("Receipt saved.")

    with right:
        df_receipts = df_from_query(
            """
SELECT *
FROM receipts
WHERE receipt_date BETWEEN :a AND :b
ORDER BY receipt_date DESC, id DESC
""",
            {"a": date_start_s, "b": date_end_s},
        )

        if df_receipts.empty:
            st.info("No receipts in this date range.")
        else:
            st.dataframe(df_receipts, use_container_width=True)

# ======================================================================
# PAGE: FUEL
# ======================================================================
elif page == "Fuel":
    st.subheader("Fuel Tracking")

    left, right = st.columns([1, 1], gap="large")
    with left:
        f_date = st.date_input("Fuel date", value=today, key="f_date")
        location = st.text_input("Location", key="f_loc")
        gallons = st.number_input("Gallons", min_value=0.0, step=0.1, format="%.2f", key="f_gal")
        ppg = st.number_input("Price per gallon", min_value=0.0, step=0.01, format="%.3f", key="f_ppg")
        total = round(gallons * ppg, 2)
        st.write("Total $:", money(total))
        odo = st.number_input("Odometer (optional)", min_value=0.0, step=1.0, format="%.0f", key="f_odo")
        link_load = st.number_input("Link to Load ID (optional)", min_value=0, step=1, value=0, key="f_link")
        notes = st.text_area("Notes (optional)", key="f_notes")

        if st.button("Save Fuel", type="primary"):
            if gallons <= 0 or ppg <= 0:
                st.error("Enter gallons and price per gallon.")
            else:
                with engine.begin() as conn:
                    conn.execute(
                        text(
                            """
INSERT INTO fuel (
    fuel_date, location, gallons, price_per_gal, total_dollars,
    odometer, linked_load_id, notes, created_at
) VALUES (:d,:l,:g,:p,:t,:o,:lid,:n,:ts)
"""
                        ),
                        dict(
                            d=f_date.strftime("%Y-%m-%d"),
                            l=location.strip() if location else None,
                            g=float(gallons),
                            p=float(ppg),
                            t=float(total),
                            o=float(odo) if odo else None,
                            lid=int(link_load) if link_load > 0 else None,
                            n=notes.strip() if notes else None,
                            ts=now_iso(),
                        ),
                    )
                st.success("Fuel saved.")

    with right:
        df_fuel = df_from_query(
            """
SELECT *
FROM fuel
WHERE fuel_date BETWEEN :a AND :b
ORDER BY fuel_date DESC, id DESC
""",
            {"a": date_start_s, "b": date_end_s},
        )
        if df_fuel.empty:
            st.info("No fuel entries in this range.")
        else:
            st.dataframe(df_fuel, use_container_width=True)

# ======================================================================
# PAGE: MAINTENANCE
# ======================================================================
elif page == "Maintenance":
    st.subheader("Maintenance Log")

    left, right = st.columns([1, 1], gap="large")
    with left:
        m_date = st.date_input("Maintenance date", value=today, key="m_date")
        item = st.text_input("Item / Work performed", key="m_item")
        vendor = st.text_input("Vendor", key="m_vendor")
        category = st.text_input("Category (PM, Tires, Repair, APU, Trailer, etc.)", key="m_cat")
        cost = st.number_input("Cost $", min_value=0.0, step=1.0, format="%.2f", key="m_cost")
        odo = st.number_input("Odometer (optional)", min_value=0.0, step=1.0, format="%.0f", key="m_odo")
        link_load = st.number_input("Link to Load ID (optional)", min_value=0, step=1, value=0, key="m_link")
        notes = st.text_area("Notes (optional)", key="m_notes")

        if st.button("Save Maintenance", type="primary"):
            if not item.strip():
                st.error("Item / work performed is required.")
            elif cost <= 0:
                st.error("Cost must be > 0.")
            else:
                with engine.begin() as conn:
                    conn.execute(
                        text(
                            """
INSERT INTO maintenance (
    maint_date, item, vendor, category, cost,
    odometer, linked_load_id, notes, created_at
) VALUES (:d,:i,:v,:c,:cost,:o,:lid,:n,:ts)
"""
                        ),
                        dict(
                            d=m_date.strftime("%Y-%m-%d"),
                            i=item.strip(),
                            v=vendor.strip() if vendor else None,
                            c=category.strip() if category else None,
                            cost=float(cost),
                            o=float(odo) if odo else None,
                            lid=int(link_load) if link_load > 0 else None,
                            n=notes.strip() if notes else None,
                            ts=now_iso(),
                        ),
                    )
                st.success("Maintenance saved.")

    with right:
        df_m = df_from_query(
            """
SELECT *
FROM maintenance
WHERE maint_date BETWEEN :a AND :b
ORDER BY maint_date DESC, id DESC
""",
            {"a": date_start_s, "b": date_end_s},
        )
        if df_m.empty:
            st.info("No maintenance in this range.")
        else:
            st.dataframe(df_m, use_container_width=True)

# ======================================================================
# PAGE: PER-DIEM (fixed $69/day, 80% deductible)
# ======================================================================
elif page == "Per-Diem":
    st.subheader("Per-Diem (Fixed) — $69.00/day, 80% deductible")

    st.markdown(
        """
**Rule (your setting):**
- Per-diem rate: **$69.00 per working day**
- Deductible portion: **80%**
- Pay-period deduction = **Working days × 69 × 0.80**
"""
    )

    c1, c2 = st.columns(2)
    with c1:
        p_start = st.date_input("Pay period start", value=start_date, key="pd_start")
        p_end = st.date_input("Pay period end", value=end_date, key="pd_end")
        working_days = st.number_input("Working days (qualifying)", min_value=0, step=1, value=0, key="pd_days")
    with c2:
        notes = st.text_area("Notes (optional)", key="pd_notes")

    rate = 69.00
    pct = 80
    deductible = round(float(working_days) * rate * (pct / 100.0), 2)

    st.success(f"Per-Diem deduction for this pay period: **{money(deductible)}**")

    if st.button("Save Per-Diem Entry", type="primary"):
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
INSERT INTO per_diem (
    period_start, period_end, working_days, rate_per_day,
    deductible_pct, deductible_amount, notes, created_at
) VALUES (:a,:b,:wd,:r,:p,:d,:n,:ts)
"""
                ),
                dict(
                    a=p_start.strftime("%Y-%m-%d"),
                    b=p_end.strftime("%Y-%m-%d"),
                    wd=int(working_days),
                    r=float(rate),
                    p=int(pct),
                    d=float(deductible),
                    n=notes.strip() if notes else None,
                    ts=now_iso(),
                ),
            )
        st.success("Per-diem saved.")

    st.divider()
    st.subheader("Per-Diem History")
    df_pd = df_from_query("SELECT * FROM per_diem ORDER BY period_end DESC, id DESC")
    if df_pd.empty:
        st.info("No per-diem entries yet.")
    else:
        st.dataframe(df_pd, use_container_width=True)

# ======================================================================
# PAGE: DEDUCTIONS (reduce net pay; not tax-only by default)
# ======================================================================
elif page == "Deductions":
    st.subheader("Deductions (Pay Period / Week)")

    left, right = st.columns(2)
    with left:
        d_date = st.date_input("Deduction date", value=today, key="ded_date")
        label = st.text_input("Deduction label (escrow, fees, insurance, etc.)", key="ded_label")
        amt = st.number_input("Amount ($)", min_value=0.0, step=1.0, format="%.2f", key="ded_amt")
        notes = st.text_area("Notes (optional)", key="ded_notes")

        if st.button("Save Deduction", type="primary"):
            if not label.strip():
                st.error("Label is required.")
            elif amt <= 0:
                st.error("Amount must be > 0.")
            else:
                with engine.begin() as conn:
                    conn.execute(
                        text(
                            """
INSERT INTO deductions (deduct_date, label, amount, notes, created_at)
VALUES (:d,:l,:a,:n,:ts)
"""
                        ),
                        dict(
                            d=d_date.strftime("%Y-%m-%d"),
                            l=label.strip(),
                            a=float(amt),
                            n=notes.strip() if notes else None,
                            ts=now_iso(),
                        ),
                    )
                st.success("Deduction saved.")

    with right:
        st.subheader("Deductions in Date Range")
        df_ded = df_from_query(
            """
SELECT * FROM deductions
WHERE deduct_date BETWEEN :a AND :b
ORDER BY deduct_date DESC, id DESC
""",
            {"a": date_start_s, "b": date_end_s},
        )
        if df_ded.empty:
            st.info("No deductions in this date range.")
        else:
            st.dataframe(df_ded, use_container_width=True)
            st.metric("Total Deductions (range)", money(float(df_ded["amount"].fillna(0).sum())))

# ======================================================================
# PAGE: DASHBOARD (weekly pay by delivery date Sat–Sun + non-driving pay)
# ======================================================================
elif page == "Dashboard":
    st.subheader("Dashboard — Weekly Driver Pay (Sat–Sun by Delivery Date)")

    st.markdown("### Add Non-Driving Pay (No Dispatch)")
    nd_c1, nd_c2, nd_c3 = st.columns([1, 2, 1])
    with nd_c1:
        nd_date = st.date_input("Pay date", value=today, key="nd_date")
    with nd_c2:
        nd_desc = st.text_input("Description", key="nd_desc", placeholder="Detention, layover, breakdown, etc.")
    with nd_c3:
        nd_amt = st.number_input("Amount $", min_value=0.0, step=1.0, format="%.2f", key="nd_amt")
    nd_notes = st.text_area("Notes (optional)", key="nd_notes")

    if st.button("Save Non-Driving Pay"):
        if nd_amt <= 0 or not (nd_desc or "").strip():
            st.error("Enter a description and amount.")
        else:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
INSERT INTO non_driving_pay (pay_date, description, amount, notes, created_at)
VALUES (:d,:desc,:a,:n,:ts)
"""
                    ),
                    dict(
                        d=nd_date.strftime("%Y-%m-%d"),
                        desc=nd_desc.strip(),
                        a=float(nd_amt),
                        n=nd_notes.strip() if nd_notes else None,
                        ts=now_iso(),
                    ),
                )
            st.success("Saved non-driving pay.")

    st.divider()

    df_loads = df_from_query(
        """
SELECT delivery_date,
       COALESCE(linehaul_dollars,0) AS linehaul,
       COALESCE(fsc_dollars,0) AS fsc,
       COALESCE(accessorials_dollars,0) AS accessorials,
       COALESCE(total_hub_miles,0) AS hub_miles,
       COALESCE(empty_miles,0) AS deadhead_miles
FROM loads
WHERE delivery_date BETWEEN :a AND :b
""",
        {"a": date_start_s, "b": date_end_s},
    )

    df_nd = df_from_query(
        """
SELECT pay_date,
       COALESCE(amount,0) AS amount
FROM non_driving_pay
WHERE pay_date BETWEEN :a AND :b
""",
        {"a": date_start_s, "b": date_end_s},
    )

    if df_loads.empty and df_nd.empty:
        st.info("No deliveries or non-driving pay in this date range.")
    else:
        # Normalize dates
        if not df_loads.empty:
            df_loads["delivery_date"] = pd.to_datetime(df_loads["delivery_date"])
            df_loads["gross"] = df_loads["linehaul"] + df_loads["fsc"] + df_loads["accessorials"]
            df_loads["week_end_sun"] = df_loads["delivery_date"].apply(week_end_sunday)
        else:
            df_loads = pd.DataFrame(columns=["week_end_sun", "gross", "fsc", "hub_miles", "deadhead_miles", "linehaul", "accessorials"])

        if not df_nd.empty:
            df_nd["pay_date"] = pd.to_datetime(df_nd["pay_date"])
            df_nd["week_end_sun"] = df_nd["pay_date"].apply(week_end_sunday)
        else:
            df_nd = pd.DataFrame(columns=["week_end_sun", "amount"])

        wk_loads = (
            df_loads.groupby("week_end_sun", as_index=False)
            .agg(
                gross=("gross", "sum"),
                fsc=("fsc", "sum"),
                hub_miles=("hub_miles", "sum"),
                deadhead_miles=("deadhead_miles", "sum"),
                linehaul=("linehaul", "sum"),
                accessorials=("accessorials", "sum"),
            )
            if not df_loads.empty
            else pd.DataFrame(columns=["week_end_sun", "gross", "fsc", "hub_miles", "deadhead_miles", "linehaul", "accessorials"])
        )

        wk_nd = (
            df_nd.groupby("week_end_sun", as_index=False).agg(non_driving_pay=("amount", "sum"))
            if not df_nd.empty
            else pd.DataFrame(columns=["week_end_sun", "non_driving_pay"])
        )

        wk = wk_loads.merge(wk_nd, on="week_end_sun", how="outer").fillna(0.0)

        # Gross includes non-driving pay, tier based on this weekly gross
        wk["gross_total"] = wk["gross"] + wk["non_driving_pay"]

        # Pay base excludes FSC (your rule), but includes non-driving pay and accessorials
        wk["pay_base_after_fsc"] = (wk["linehaul"] + wk["accessorials"] + wk["non_driving_pay"])

        wk["tier_pct"] = wk["gross_total"].apply(tier_pct)
        wk["driver_pay"] = wk["pay_base_after_fsc"] * wk["tier_pct"]

        wk["deadhead_pct"] = wk.apply(lambda r: (r["deadhead_miles"] / r["hub_miles"]) if r["hub_miles"] > 0 else 0.0, axis=1)

        wk = wk.sort_values("week_end_sun", ascending=False)
        wk_show = wk.copy()
        wk_show["week_end_sun"] = wk_show["week_end_sun"].dt.strftime("%Y-%m-%d")

        st.markdown("### Weekly Driver Pay (Tiered)")
        st.caption("Tier from weekly gross. Pay = tier % × (pay base after FSC).")
        show = wk_show[["week_end_sun", "gross_total", "fsc", "pay_base_after_fsc", "tier_pct", "driver_pay", "non_driving_pay", "hub_miles", "deadhead_miles", "deadhead_pct"]].copy()
        show["gross_total"] = show["gross_total"].apply(money)
        show["fsc"] = show["fsc"].apply(money)
        show["pay_base_after_fsc"] = show["pay_base_after_fsc"].apply(money)
        show["tier_pct"] = show["tier_pct"].apply(lambda x: f"{float(x):.0%}")
        show["driver_pay"] = show["driver_pay"].apply(money)
        show["non_driving_pay"] = show["non_driving_pay"].apply(money)
        show["deadhead_pct"] = show["deadhead_pct"].apply(lambda x: f"{float(x):.2%}")
        st.dataframe(show, use_container_width=True)

# ======================================================================
# PAGE: TAXES (SE + CO + FED + NET after deductions/taxes)
# Uses weekly-tier pay from Dashboard logic (exact rule).
# ======================================================================
elif page == "Taxes":
    st.subheader("Taxes + Net Pay (Pay Period)")

    # Weekly tier pay table for the range (same logic as Dashboard)
    df_loads = df_from_query(
        """
SELECT delivery_date,
       COALESCE(linehaul_dollars,0) AS linehaul,
       COALESCE(fsc_dollars,0) AS fsc,
       COALESCE(accessorials_dollars,0) AS accessorials
FROM loads
WHERE delivery_date BETWEEN :a AND :b
""",
        {"a": date_start_s, "b": date_end_s},
    )
    df_nd = df_from_query(
        """
SELECT pay_date, COALESCE(amount,0) AS amount
FROM non_driving_pay
WHERE pay_date BETWEEN :a AND :b
""",
        {"a": date_start_s, "b": date_end_s},
    )

    if not df_loads.empty:
        df_loads["delivery_date"] = pd.to_datetime(df_loads["delivery_date"])
        df_loads["gross"] = df_loads["linehaul"] + df_loads["fsc"] + df_loads["accessorials"]
        df_loads["week_end_sun"] = df_loads["delivery_date"].apply(week_end_sunday)
    else:
        df_loads = pd.DataFrame(columns=["week_end_sun", "gross", "fsc", "linehaul", "accessorials"])

    if not df_nd.empty:
        df_nd["pay_date"] = pd.to_datetime(df_nd["pay_date"])
        df_nd["week_end_sun"] = df_nd["pay_date"].apply(week_end_sunday)
    else:
        df_nd = pd.DataFrame(columns=["week_end_sun", "amount"])

    wk_loads = (
        df_loads.groupby("week_end_sun", as_index=False)
        .agg(gross=("gross", "sum"), fsc=("fsc", "sum"), linehaul=("linehaul", "sum"), accessorials=("accessorials", "sum"))
        if not df_loads.empty
        else pd.DataFrame(columns=["week_end_sun", "gross", "fsc", "linehaul", "accessorials"])
    )
    wk_nd = (
        df_nd.groupby("week_end_sun", as_index=False).agg(non_driving_pay=("amount", "sum"))
        if not df_nd.empty
        else pd.DataFrame(columns=["week_end_sun", "non_driving_pay"])
    )

    wk = wk_loads.merge(wk_nd, on="week_end_sun", how="outer").fillna(0.0)
    wk["gross_total"] = wk["gross"] + wk["non_driving_pay"]
    wk["pay_base_after_fsc"] = (wk["linehaul"] + wk["accessorials"] + wk["non_driving_pay"])
    wk["tier_pct"] = wk["gross_total"].apply(tier_pct)
    wk["driver_pay"] = wk["pay_base_after_fsc"] * wk["tier_pct"]

    driver_pay_before_deductions = float(wk["driver_pay"].sum()) if not wk.empty else 0.0

    # Deductions (reduce take-home)
    df_ded = df_from_query(
        """
SELECT COALESCE(amount,0) AS amount
FROM deductions
WHERE deduct_date BETWEEN :a AND :b
""",
        {"a": date_start_s, "b": date_end_s},
    )
    deductions_total = float(df_ded["amount"].sum()) if not df_ded.empty else 0.0
    net_before_taxes = driver_pay_before_deductions - deductions_total

    # Tax-only deductions: receipts + per diem
    df_receipts = df_from_query(
        """
SELECT COALESCE(deductible_amount,0) AS deductible_amount
FROM receipts
WHERE receipt_date BETWEEN :a AND :b
""",
        {"a": date_start_s, "b": date_end_s},
    )
    receipts_ded = float(df_receipts["deductible_amount"].sum()) if not df_receipts.empty else 0.0

    df_pd = df_from_query(
        """
SELECT COALESCE(deductible_amount,0) AS deductible_amount
FROM per_diem
WHERE period_start >= :a AND period_end <= :b
""",
        {"a": date_start_s, "b": date_end_s},
    )
    per_diem_ded = float(df_pd["deductible_amount"].sum()) if not df_pd.empty else 0.0

    tax_deductions_total = receipts_ded + per_diem_ded
    taxable_income = max(0.0, net_before_taxes - tax_deductions_total)

    st.markdown("### Pay + Taxable Income Summary")
    a1, a2, a3, a4 = st.columns(4)
    a1.metric("Driver Pay (tiered, sum of weeks)", money(driver_pay_before_deductions))
    a2.metric("Deductions (reduce net)", money(deductions_total))
    a3.metric("Net Before Taxes", money(net_before_taxes))
    a4.metric("Taxable Income (est.)", money(taxable_income))

    b1, b2 = st.columns(2)
    b1.metric("Receipts Deductible (tax-only)", money(receipts_ded))
    b2.metric("Per-Diem Deductible (tax-only)", money(per_diem_ded))

    st.divider()

    st.markdown("### Tax Withholding Settings")
    c1, c2, c3 = st.columns(3)
    with c1:
        se_tax_rate = st.number_input("SE Tax rate", value=0.153, step=0.001, format="%.3f", key="se_rate")
        apply_se = st.checkbox("Apply SE tax", value=True, key="apply_se")
    with c2:
        co_tax_rate = st.number_input("Colorado tax rate", value=0.044, step=0.001, format="%.3f", key="co_rate")
        apply_co = st.checkbox("Apply CO tax", value=True, key="apply_co")
    with c3:
        fed_tax_rate = st.number_input("Federal tax rate (estimate)", value=0.120, step=0.005, format="%.3f", key="fed_rate")
        apply_fed = st.checkbox("Apply Federal tax", value=True, key="apply_fed")

    se_tax = taxable_income * se_tax_rate if apply_se else 0.0
    co_tax = taxable_income * co_tax_rate if apply_co else 0.0
    fed_tax = taxable_income * fed_tax_rate if apply_fed else 0.0
    total_tax = se_tax + co_tax + fed_tax

    net_after_taxes = net_before_taxes - total_tax

    d1, d2, d3, d4 = st.columns(4)
    d1.metric("SE Tax", money(se_tax))
    d2.metric("CO Tax", money(co_tax))
    d3.metric("Federal Tax", money(fed_tax))
    d4.metric("Total Taxes", money(total_tax))

    st.metric("NET PAY After Deductions + Taxes", money(net_after_taxes))
    st.caption("Cash advances are NOT used as tax deductions here. Receipts + per-diem reduce taxable income.")

# ======================================================================
# PAGE: EXPORT / BACKUP
# ======================================================================
elif page == "Export/Backup":
    st.subheader("Export / Backup")

    st.markdown("### CSV Exports (Date Range)")
    df_loads = df_from_query("SELECT * FROM loads WHERE delivery_date BETWEEN :a AND :b", {"a": date_start_s, "b": date_end_s})
    df_receipts = df_from_query("SELECT * FROM receipts WHERE receipt_date BETWEEN :a AND :b", {"a": date_start_s, "b": date_end_s})
    df_fuel = df_from_query("SELECT * FROM fuel WHERE fuel_date BETWEEN :a AND :b", {"a": date_start_s, "b": date_end_s})
    df_m = df_from_query("SELECT * FROM maintenance WHERE maint_date BETWEEN :a AND :b", {"a": date_start_s, "b": date_end_s})

    def dl_csv(df, name):
        if df.empty:
            st.write(f"{name}: no rows")
            return
        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button(f"Download {name}.csv", data=csv, file_name=f"{name}.csv", mime="text/csv")

    dl_csv(df_loads, "loads")
    dl_csv(df_receipts, "receipts")
    dl_csv(df_fuel, "fuel")
    dl_csv(df_m, "maintenance")

    st.divider()
    st.markdown("### PDF Summary (Date Range)")
    if st.button("Generate PDF Summary"):
        # Loads totals
        if df_loads.empty:
            total_linehaul = total_fsc = total_access = total_gross = 0.0
        else:
            total_linehaul = float(df_loads["linehaul_dollars"].fillna(0).sum())
            total_fsc = float(df_loads["fsc_dollars"].fillna(0).sum())
            total_access = float(df_loads["accessorials_dollars"].fillna(0).sum())
            total_gross = total_linehaul + total_fsc + total_access

        receipts_ded = float(df_receipts["deductible_amount"].fillna(0).sum()) if not df_receipts.empty else 0.0
        fuel_total = float(df_fuel["total_dollars"].fillna(0).sum()) if not df_fuel.empty else 0.0
        maint_total = float(df_m["cost"].fillna(0).sum()) if not df_m.empty else 0.0

        summary = {
            "Date Range": f"{date_start_s} to {date_end_s}",
            "Total Linehaul": money(total_linehaul),
            "Total FSC": money(total_fsc),
            "Total Accessorials": money(total_access),
            "Total Gross": money(total_gross),
            "Receipts Deductible (tax-only)": money(receipts_ded),
            "Fuel Total": money(fuel_total),
            "Maintenance Total": money(maint_total),
        }

        out_path = os.path.join(UPLOAD_DIR, f"summary_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf")
        write_pdf_summary(summary, out_path)
        st.success("PDF created.")
        with open(out_path, "rb") as f:
            st.download_button("Download PDF Summary", data=f.read(), file_name=os.path.basename(out_path), mime="application/pdf")

else:
    st.info("Pick a page from the left menu.")
