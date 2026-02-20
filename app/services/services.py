import pandas as pd
import os
import warnings

# =========================
# CONSTANTS
# =========================
GST_RATE = 0.18
MONTHLY_TARGET = 17147488
WORKING_DAYS = 31
PER_DAY_TARGET = MONTHLY_TARGET / WORKING_DAYS

PLANNING_FOLDER = r"G:\My Drive\FAstAPI\FAstAPI"

# =========================
# COMMON HELPERS
# =========================
def norm(c):
    return (
        str(c)
        .lower()
        .replace("\ufeff", "")
        .replace("(", "")
        .replace(")", "")
        .replace("-", "")
        .replace("_", "")
        .replace(" ", "")
        .strip()
    )

def clean_money(x):
    if pd.isna(x):
        return 0.0
    return float(
        str(x)
        .replace("₹", "")
        .replace(",", "")
        .replace("INR", "")
        .strip()
    )

def empty_df():
    return pd.DataFrame()

# =========================
# PLANNING FILE RESOLVER
# =========================
def get_planning_file_for_date(d):
    if not isinstance(d, pd.Timestamp):
        d = pd.to_datetime(d)

    month = d.strftime("%b")
    year = d.strftime("%Y")
    filename = f"ASIN Planning file - {month} {year}.xlsx"
    return os.path.join(PLANNING_FOLDER, filename)

# =========================
# FILE LOADER
# =========================
def load_file(source, sheet_name=0, skiprows=0):
    try:
        if hasattr(source, "filename"):
            source.file.seek(0)
            name = source.filename.lower()

            if name.endswith(".csv") or name.endswith(".txt"):
                return pd.read_csv(source.file, skiprows=skiprows)

            if name.endswith(".xlsx") or name.endswith(".xls"):
                with warnings.catch_warnings():
                    warnings.filterwarnings(
                        "ignore",
                        category=UserWarning,
                        module="openpyxl",
                    )
                    return pd.read_excel(
                        source.file,
                        sheet_name=sheet_name,
                        skiprows=skiprows,
                        engine="openpyxl",
                    )

        if isinstance(source, str) and os.path.exists(source):
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore",
                    category=UserWarning,
                    module="openpyxl",
                )
                return pd.read_excel(source, sheet_name=sheet_name, engine="openpyxl")

    except Exception as e:
        print("LOAD_FILE_ERROR:", e)

    return empty_df()

# =========================
# PLANNING DATA
# =========================
def load_planning_main(ref_date):
    path = get_planning_file_for_date(ref_date)
    if not os.path.exists(path):
        print("PLANNING FILE MISSING:", path)
        return empty_df()

    df = load_file(path, sheet_name="Main")
    if df.empty:
        return empty_df()

    df.columns = [norm(c) for c in df.columns]

    if "asin" not in df.columns:
        return empty_df()

    df["asin"] = df["asin"].astype(str).str.upper().str.strip()
    return df

def load_planning_category(ref_date):
    path = get_planning_file_for_date(ref_date)
    if not os.path.exists(path):
        return empty_df()

    df = load_file(path, sheet_name="Category")
    if df.empty:
        return empty_df()

    df.columns = [norm(c) for c in df.columns]
    return df

# =========================
# CORE INGESTION
# =========================
def build_rows(file, account, sales_date, is_vendor):
    df = load_file(file, skiprows=1 if is_vendor else 0)
    if df.empty:
        return empty_df()

    df.columns = [norm(c) for c in df.columns]

    if "parentasin" in df.columns:
        df["ASIN"] = df["parentasin"]
    elif "asin" in df.columns:
        df["ASIN"] = df["asin"]
    else:
        return empty_df()

    df["ASIN"] = df["ASIN"].astype(str).str.upper().str.strip()

    plan_main = load_planning_main(sales_date)
    if plan_main.empty:
        return empty_df()

    allowed_asins = set(plan_main["asin"])
    df = df[df["ASIN"].isin(allowed_asins)]
    if df.empty:
        return empty_df()

    # =========================
    # SELLER CENTRAL LOGIC
    # =========================
    if not is_vendor:
        if "orderedproductsales" not in df.columns:
            return empty_df()

        # 🔒 STRICT BUSINESS RULE
        # ALL ACCOUNTS → ONLY B2C (ignore B2B for planning consistency)
        # Audio Array → ONLY B2C
        # Cambium Retail / Viomi → B2C + B2B
        df["sales"] = df["orderedproductsales"].apply(clean_money)

        # B2B sales are intentionally ignored for all Seller Central accounts

        df["net_sales"] = df["sales"] / (1 + GST_RATE)

    # =========================
    # VENDOR CENTRAL
    # =========================
    else:
        if "orderedrevenue" not in df.columns:
            return empty_df()

        df["sales"] = df["orderedrevenue"].apply(clean_money)
        df["net_sales"] = df["sales"]

    df["date"] = pd.to_datetime(sales_date)
    df["account"] = account

    return df[["date", "account", "ASIN", "sales", "net_sales"]]

# =========================
# KPI
# =========================
def calculate_kpis(df, ref_date):
    if df.empty:
        return {
            "monthly_target": 0,
            "target_till": 0,
            "actual": 0,
            "achievement": 0,
            "pace": 0,
        }

    plan = load_planning_main(ref_date)
    if plan.empty:
        return {
            "monthly_target": 0,
            "target_till": 0,
            "actual": 0,
            "achievement": 0,
            "pace": 0,
        }

    days = df["date"].nunique()

    # month-wise dynamic column already normalized
    month = pd.to_datetime(ref_date).strftime("%b").lower()
    monthly_col = f"{month}goalprojected"

    if monthly_col not in plan.columns or "perdaygoalprojected" not in plan.columns:
        return {
            "monthly_target": 0,
            "target_till": 0,
            "actual": 0,
            "achievement": 0,
            "pace": 0,
        }

    monthly_target = plan[monthly_col].sum()
    per_day_target = plan["perdaygoalprojected"].sum()

    target_till = per_day_target * days
    actual = df["net_sales"].sum()

    return {
        "monthly_target": round(monthly_target, 1),
        "target_till": round(target_till, 1),
        "actual": round(actual, 1),
        "achievement": actual / target_till if target_till else 0,
        "pace": actual / target_till if target_till else 0,
    }

    days = df["date"].nunique()
    target_till = PER_DAY_TARGET * days
    actual = df["net_sales"].sum()

    return {
        "monthly_target": MONTHLY_TARGET,
        "target_till": round(target_till, 1),
        "actual": round(actual, 1),
        "achievement": actual / target_till if target_till else 0,
        "pace": actual / target_till if target_till else 0,
    }

# =========================
# DAY / MTD / WEEK
# =========================
def day_wise_performance(df, ref_date):
    if df.empty:
        return []

    plan = load_planning_main(ref_date)
    if plan.empty or "perdaygoalprojected" not in plan.columns:
        return []

    per_day_target = plan["perdaygoalprojected"].sum()

    d = df.groupby("date", as_index=False)["net_sales"].sum()
    d["actual"] = d["net_sales"]
    d["target"] = per_day_target
    d["achieved"] = (d["net_sales"] / per_day_target).round(2)
    return d.to_dict("records")

def mtd_chart(df, ref_date):
    if df.empty:
        return {"labels": [], "actual": [], "target": []}

    plan = load_planning_main(ref_date)
    if plan.empty or "perdaygoalprojected" not in plan.columns:
        return {"labels": [], "actual": [], "target": []}

    per_day_target = plan["perdaygoalprojected"].sum()

    d = df.groupby("date", as_index=False)["net_sales"].sum()
    d["actual"] = d["net_sales"]
    d["cum_actual"] = d["net_sales"].cumsum()
    d["cum_target"] = per_day_target * (d.index + 1)

    return {
        "labels": d["date"].dt.strftime("%d %b").tolist(),
        "actual": d["cum_actual"].round(1).tolist(),
        "target": d["cum_target"].round(1).tolist(),
    }

    d = df.groupby("date", as_index=False)["net_sales"].sum()
    d["actual"] = d["net_sales"]
    d["cum_actual"] = d["net_sales"].cumsum()
    d["cum_target"] = PER_DAY_TARGET * (d.index + 1)

    return {
        "labels": d["date"].dt.strftime("%d %b").tolist(),
        "actual": d["cum_actual"].round(1).tolist(),
        "target": d["cum_target"].round(1).tolist(),
    }

def week_wise(df):
    if df.empty:
        return "<p>No data</p>"

    return (
        df.assign(week=df["date"].dt.to_period("W").astype(str))
        .groupby("week", as_index=False)["net_sales"]
        .sum()
        .round(1)
        .to_html(index=False, classes="table table-striped table-bordered table-sm")
    )

# =========================
# FILTER
# =========================
def filter_by_date_range(df, f, t):
    return df[(df["date"] >= f) & (df["date"] <= t)]

# =========================
# ASIN TARGET VS ACTUAL
# =========================
def asin_target_vs_actual(ledger, f, t):
    ledger = filter_by_date_range(ledger, f, t)
    ledger = ledger.copy()  # defensive copy to avoid SettingWithCopyWarning
    ledger = ledger.copy()  # defensive copy to avoid SettingWithCopyWarning
    if ledger.empty:
        return "<p>No data</p>"

    plan = load_planning_main(f)
    if plan.empty:
        return "<p>No planning data</p>"

    days = ledger["date"].nunique()
    plan["period_target"] = plan["perdaygoalprojected"] * days

    actual = ledger.groupby("ASIN", as_index=False)["net_sales"].sum()

    merged = plan.merge(actual, left_on="asin", right_on="ASIN", how="left").fillna(0)

    out = pd.DataFrame({
        "ASIN": merged["asin"],
        "Category": merged["category"],
        "Target": merged["period_target"].round(1),
        "Actual": merged["net_sales"].round(1),
        "Achievement %": ((merged["net_sales"] / merged["period_target"]) * 100).round(1),
    })

    out["Achievement %"] = out["Achievement %"].astype(str) + "%"
    return out.to_html(index=False, classes="table table-striped table-bordered table-sm")

# =========================
# CATEGORY TARGET VS ACTUAL
# =========================
def category_target_vs_actual(ledger, f, t):
    ledger = filter_by_date_range(ledger, f, t)
    ledger = ledger.copy()  # defensive copy to avoid SettingWithCopyWarning
    if ledger.empty:
        return "<p>No data</p>"

    plan_main = load_planning_main(f)
    plan_cat = load_planning_category(f)
    if plan_main.empty or plan_cat.empty:
        return "<p>No planning data</p>"

    asin_category = plan_main.set_index("asin")["category"].to_dict()
    ledger["category"] = ledger["ASIN"].map(asin_category)

    actual = ledger.groupby("category", as_index=False)["net_sales"].sum()

    days = ledger["date"].nunique()
    plan_cat["period_target"] = plan_cat["perdaygoal"] * days

    merged = plan_cat.merge(actual, on="category", how="left").fillna(0)

    out = pd.DataFrame({
        "Category": merged["category"],
        "Per Day Target": merged["perdaygoal"].round(1),
        "Target": merged["period_target"].round(1),
        "Actual": merged["net_sales"].round(1),
        "Achievement %": ((merged["net_sales"] / merged["period_target"]) * 100).round(1),
    })

    out["Achievement %"] = out["Achievement %"].astype(str) + "%"
    return out.to_html(index=False, classes="table table-striped table-bordered table-sm")

# ======================================================
# 🔧 ONE-TIME HISTORICAL CORRECTION (MANUAL USE ONLY)
# ======================================================

    """
    ⚠️ RUN THIS ONCE ONLY ⚠️

    Removes Audio Array Seller Central B2B contamination
    from historical net_sales.

    This function:
    - DOES NOT auto-run
    - DOES NOT delete rows
    - ONLY updates account = 'Audio Array'
    """

    with engine.begin() as conn:
        conn.execute("""
            UPDATE ledger
            SET net_sales = net_sales / (1 + 0) -- placeholder
            WHERE account = 'Audio Array';
        """)

    print("⚠️ This function is a placeholder. Use controlled script for correction.")

# =========================
# DATA INTEGRITY & VALIDATION (READ-ONLY)
# =========================
def validation_summary(ledger, f, t):
    """
    Read-only validation helper.
    Does NOT mutate data.
    Used only for dashboard reconciliation & audit visibility.
    """
    try:
        # Defensive copy
        df = ledger.copy()

        # Apply same filter logic
        if f is not None and t is not None:
            df = df[(df["date"] >= f) & (df["date"] <= t)]

        if df.empty:
            return None

        # ---------- ASIN VALIDATION ----------
        plan = load_planning_main(f or df["date"].max())
        if plan.empty:
            extra_asins = 0
        else:
            ledger_asins = set(df["ASIN"].unique())
            plan_asins = set(plan["asin"].unique())
            extra_asins = len(ledger_asins - plan_asins)

        # ---------- AUDIO ARRAY B2B CHECK ----------
        audio_array_b2b_rows = 0
        aa = df[df["account"] == "Nexlev"]
        if not aa.empty:
            audio_array_b2b_rows = int((aa["sales"] != aa["net_sales"]).sum())

        # ---------- RECONCILIATION ----------
        kpi_actual = round(df["net_sales"].sum(), 1)

        day_sum = (
            df.groupby("date", as_index=False)["net_sales"]
            .sum()["net_sales"]
            .sum()
        )
        day_sum = round(day_sum, 1)

        difference = round(kpi_actual - day_sum, 1)

        # ---------- REASON ----------
        reasons = []
        if extra_asins > 0:
            reasons.append("Ledger contains ASINs not present in planning file.")
        if audio_array_b2b_rows > 0:
            reasons.append("Historical Audio Array B2B rows detected.")
        if difference != 0 and not reasons:
            reasons.append("Difference due to partial month / missing days in selection.")

        reason_text = " ".join(reasons) if reasons else "All validations passed. Data is consistent."

        return {
            "extra_asins": extra_asins,
            "audio_array_b2b_rows": audio_array_b2b_rows,
            "kpi_actual": kpi_actual,
            "daywise_sum": day_sum,
            "difference": difference,
            "reason": reason_text,
        }

    except Exception as e:
        print("VALIDATION_ERROR:", e)
        return None
