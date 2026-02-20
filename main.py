import os
from typing import Optional
from fastapi import FastAPI, Request, Form, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from fastapi.exceptions import RequestValidationError
import pandas as pd
import io
from datetime import datetime, date
print("🔥 ROOT main.py LOADED")


from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from app.services.services import (
    build_rows,
    calculate_kpis,
    day_wise_performance,
    mtd_chart,
    week_wise,
    filter_by_date_range,
    asin_target_vs_actual,
    category_target_vs_actual,
    validation_summary,
)

# ==================================================
# CONFIG
# ==================================================
ADMIN_UPLOAD_KEY = "1234"

DATABASE_URL = os.getenv("DATABASE_URL")

# Local fallback
if not DATABASE_URL:
    DATABASE_URL = "sqlite:///./local.db"

engine: Engine = create_engine(DATABASE_URL, pool_pre_ping=True)

app = FastAPI(title="Audio Array Sales Dashboard")
templates = Jinja2Templates(directory="templates")

# ==================================================
# DB SETUP
# ==================================================
def init_db():
    with engine.begin() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS ledger (
            id SERIAL PRIMARY KEY,
            date DATE NOT NULL,
            account TEXT NOT NULL,
            asin TEXT NOT NULL,
            sales NUMERIC NOT NULL,
            net_sales NUMERIC NOT NULL,
            UNIQUE(date, account, asin)
        );
        """))

init_db()

# ==================================================
# LEDGER IO
# ==================================================
LEDGER_COLUMNS = ["date", "account", "ASIN", "sales", "net_sales"]

def load_ledger() -> pd.DataFrame:
    with engine.begin() as conn:
        df = pd.read_sql(
            text("SELECT date, account, asin AS \"ASIN\", sales, net_sales FROM ledger"),
            conn,
            parse_dates=["date"]
        )
    return df[LEDGER_COLUMNS] if not df.empty else pd.DataFrame(columns=LEDGER_COLUMNS)

def save_ledger(rows: pd.DataFrame):
    if rows.empty:
        return

    rows = rows.copy()
    rows.columns = ["date", "account", "asin", "sales", "net_sales"]

 # FIX: convert pandas Timestamp to Python date
    rows["date"] = pd.to_datetime(rows["date"]).dt.date

    with engine.begin() as conn:
        for _, r in rows.iterrows():
            conn.execute(
                text("""
                INSERT INTO ledger (date, account, asin, sales, net_sales)
                VALUES (:date, :account, :asin, :sales, :net_sales)
                ON CONFLICT (date, account, asin)
                DO UPDATE SET
                    sales = EXCLUDED.sales,
                    net_sales = EXCLUDED.net_sales
                """),
                r.to_dict()
            )

# ==================================================
# MONTH HELPERS
# ==================================================
def get_month_bounds(month_str: str):
    start = datetime.strptime(month_str, "%b %Y")
    end = (start + pd.offsets.MonthEnd(1)).to_pydatetime()
    return start, end

def available_months_from_ledger(ledger: pd.DataFrame):
    if ledger.empty:
        return []
    return sorted(
        ledger["date"]
        .dt.to_period("M")
        .astype(str)
        .apply(lambda x: datetime.strptime(x, "%Y-%m").strftime("%b %Y"))
        .unique()
    )

# ==================================================
# DASHBOARD RENDER
# ==================================================
print("🚀 render_dashboard CALLED")
print("UPLOAD ENABLED:", bool(ADMIN_UPLOAD_KEY))

def render_dashboard(
    request: Request,
    error: str | None = None,
    selected_month: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
):
    ledger = load_ledger()
    months = available_months_from_ledger(ledger)

    f = t = None
    ledger_filtered = ledger

    # ---- DATE RANGE (TOP PRIORITY) ----
    if from_date and to_date and from_date != "" and to_date != "":
        f = pd.to_datetime(from_date, format="%Y-%m-%d", errors="coerce")
        t = pd.to_datetime(to_date, format="%Y-%m-%d", errors="coerce")
        if (pd.notna(f) and pd.notna(t)) or selected_month:
            ledger_filtered = filter_by_date_range(ledger, f, t)

    # ---- MONTH FALLBACK ----
    elif selected_month:
        f, t = get_month_bounds(selected_month)
        ledger_filtered = filter_by_date_range(ledger, f, t)

    context = {
        "request": request,
        "upload_enabled": bool(ADMIN_UPLOAD_KEY),
        "error": error,
        "months": months,
        "selected_month": selected_month or "",
        "from_date": from_date or "",
        "to_date": to_date or "",
    }

    # ---------------- EMPTY LEDGER SAFETY ----------------
    if ledger_filtered.empty:
        context.update({
            "monthly_target": 0,
            "target_till_date": 0,
            "actual_sales": 0,
            "achievement_pct": 0,
            "pace_index": 0,
            "daywise": [],
            "chart": {"labels": [], "actual": [], "target": []},
            "weekwise": "<p>No data</p>",
            "asin_target_table": "<p>No data</p>",
            "category_target_table": "<p>No data</p>",
        })
        return templates.TemplateResponse("index.html", context)

    # ---------------- REF DATE ----------------
    if t is not None:
        ref_date = t
    elif f is not None:
        ref_date = f
    else:
        ref_date = ledger_filtered["date"].dropna().max()

    if pd.isna(ref_date):
        ref_date = pd.Timestamp.today()

    # ---------------- KPIs ----------------
    context.update({
        **calculate_kpis(ledger_filtered, ref_date),
        "daywise": day_wise_performance(ledger_filtered, ref_date),
        "chart": mtd_chart(ledger_filtered, ref_date),
        "weekwise": week_wise(ledger_filtered),
    })

    # ---------------- DATA VALIDATION (READ-ONLY) ----------------
    context["validation"] = validation_summary(ledger, f, t)


    # ---------------- OPTION 2: AUTO-CLAMP TARGETS ----------------
    if (pd.notna(f) and pd.notna(t)) or selected_month:
        tf, tt = f, t
        if f.month != t.month or f.year != t.year:
            tf = ref_date.replace(day=1)
            tt = ref_date
        context["asin_target_table"] = asin_target_vs_actual(ledger, tf, tt)
        context["category_target_table"] = category_target_vs_actual(ledger, tf, tt)
    else:
        context["asin_target_table"] = "<p>Select date range or month</p>"
        context["category_target_table"] = "<p>Select date range or month</p>"

    # ---------------- KPI SAFETY DEFAULTS (UI ONLY) ----------------
    context.setdefault("monthly_target", 0)
    context.setdefault("target_till_date", 0)
    context.setdefault("actual_sales", 0)
    context.setdefault("achievement_pct", 0)
    context.setdefault("pace_index", 0)

    return templates.TemplateResponse("index.html", context)
# ==================================================
# ROUTES
# ==================================================
@app.api_route("/", methods=["GET", "POST", "HEAD"])
async def dashboard(
    request: Request,
    month: Optional[str] = Form(None),
    from_date: Optional[str] = Form(None),
    to_date: Optional[str] = Form(None),
):
    return render_dashboard(
        request,
        selected_month=month,
        from_date=from_date,
        to_date=to_date,
    )

@app.get("/log-sales")
def block_get_log_sales():
    return RedirectResponse("/", status_code=303)

# ==================================================
# LOG SALES (ADMIN)
# ==================================================
@app.post("/log-sales", response_class=HTMLResponse)
async def log_sales(
    request: Request,
    upload_key: str = Form(...),
    sales_date: str = Form(...),
    replace_day: Optional[str] = Form(None),
    aa_file: Optional[UploadFile] = File(None),
    cr_file: Optional[UploadFile] = File(None),
    vi_file: Optional[UploadFile] = File(None),
    vc_file: Optional[UploadFile] = File(None),
):
    if not ADMIN_UPLOAD_KEY:
        return render_dashboard(request, "Admin key missing on server.")

    if upload_key.strip() != ADMIN_UPLOAD_KEY:
        return render_dashboard(request, "Invalid admin upload key.")

    sales_date_parsed = pd.to_datetime(sales_date, errors="coerce")
    if pd.isna(sales_date_parsed):
        return render_dashboard(request, "Invalid sales date.")

    uploads = [
        (aa_file, "Nexlev", False),
        (cr_file, "Cambium Retail", False),
        (vi_file, "Viomi By Cambium", False),
        (vc_file, "Vendor Central", True),
    ]

    all_rows = []

    for file, account, is_vendor in uploads:
        print("Processing:", account)

        if not file or not file.filename:
            print("No file for:", account)
            continue

        print("Filename:", file.filename)

        rows = build_rows(
            file=file,
            account=account,
            sales_date=sales_date_parsed,
            is_vendor=is_vendor,
        )

        print("Rows returned:", len(rows))

        if not rows.empty:
            all_rows.append(rows)

    if not all_rows:
        return render_dashboard(request, "No valid sales rows found.")

    final_df = pd.concat(all_rows, ignore_index=True)

    # ---------------- REPLACE DAY MODE ----------------
    if replace_day in ("1", "true", "True", "on"):
        with engine.begin() as conn:
            for _, acct, _ in uploads:
                conn.execute(
                    text("DELETE FROM ledger WHERE date = :d AND account = :a"),
                    {"d": sales_date_parsed.date(), "a": acct},
                )

    save_ledger(final_df)

    return render_dashboard(request, "✅ Sales uploaded successfully.")


# ==================================================
# DOWNLOAD LEDGER (READ-ONLY)
# ==================================================
@app.get("/download-ledger")
def download_ledger(
    month: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
):
    ledger = load_ledger()

    if ledger.empty:
        return {"error": "No ledger data"}

    # Apply SAME filter logic as dashboard (DEFENSIVE, NO LOGIC CHANGE)
    if (
        from_date
        and to_date
        and from_date != ""
        and to_date != ""
    ):
        f = pd.to_datetime(from_date, format="%Y-%m-%d", errors="coerce")
        t = pd.to_datetime(to_date, format="%Y-%m-%d", errors="coerce")

        if (pd.notna(f) and pd.notna(t)) or selected_month:
            ledger = filter_by_date_range(ledger, f, t)

    elif month:
        f, t = get_month_bounds(month)
        ledger = filter_by_date_range(ledger, f, t)

    if ledger.empty:
        return {"error": "No data for selected filters"}

    buffer = io.StringIO()
    ledger.sort_values(["date", "account", "ASIN"]).to_csv(buffer, index=False)
    buffer.seek(0)

    return StreamingResponse(
        buffer,
        media_type="text/csv",
        headers={
            "Content-Disposition": "attachment; filename=sales_ledger.csv"
        },
    )


# ==================================================
# ERROR HANDLERS
# ==================================================
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc):
    return render_dashboard(request, "Invalid input.")

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc):
    print("UNHANDLED ERROR:", repr(exc))
    return render_dashboard(request, "Unexpected server error.")
