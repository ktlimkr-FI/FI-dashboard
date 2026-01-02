import os
import json
import time
import requests
import pandas as pd
from datetime import datetime, timedelta

from fredapi import Fred
import gspread
from google.oauth2.service_account import Credentials


# =========================
# ENV (GitHub Secrets)
# =========================
FRED_API_KEY = os.environ["FRED_API_KEY"]
GSHEET_ID = os.environ["GSHEET_ID"]
SERVICE_ACCOUNT_JSON = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]


# =========================
# CONFIG: What to store
# =========================

# 1) Daily: store levels (native daily)
DAILY_FRED_SERIES = {
    # tab1
    "RPONTTLD": "Repo_Volume",
    "SOFR": "SOFR",
    "SOFR99": "SOFR_99th",
    "DFEDTARU": "Fed_Target_Upper",
    "DFEDTARL": "Fed_Target_Lower",

    # add more daily series you actually use (examples)
    "OBFRVOL": "OBFRVOL",
    "DTWEXBGS": "DTWEXBGS",
    "DTWEXAFEGS": "DTWEXAFEGS",
    "DTWEXEMEGS": "DTWEXEMEGS",

    "DGS3MO": "US_3M",
    "DGS1": "US_1Y",
    "DGS2": "US_2Y",
    "DGS3": "US_3Y",
    "DGS5": "US_5Y",
    "DGS10": "US_10Y",
    "DGS30": "US_30Y",
}

# 2) Weekly: OFR (native weekly)
WEEKLY_OFR_MNEMONICS = {
    "NYPD-PD_AFtD_T-A": "UST_fails_to_deliver",
    "NYPD-PD_AFtD_AG-A": "AgencyGSE_fails_to_deliver",
    "NYPD-PD_AFtD_CORS-A": "Corporate_fails_to_deliver",
    "NYPD-PD_AFtD_OMBS-A": "OtherMBS_fails_to_deliver",
}

# 3) Monthly macro: store ONLY YoY/MoM (no levels)
#    Define each item as:
#      - series_id: FRED series
#      - prefix: column prefix (country/metric)
MONTHLY_LEVEL_SERIES = [
    {"series_id": "CPIAUCSL", "prefix": "US_CPI"},                 # monthly
    {"series_id": "CPILFESL", "prefix": "US_CoreCPI"},             # monthly
    {"series_id": "CP0000EZ19M086NEST", "prefix": "EZ_HICP"},      # monthly (as used in your dashboard)
    {"series_id": "CP0000DEM086NEST", "prefix": "DE_HICP"},
    {"series_id": "CP0000GBM086NEST", "prefix": "UK_HICP"},
    {"series_id": "JPNCPICORMINMEI", "prefix": "JP_CoreCPI"},
    {"series_id": "CHNCPIALLMINMEI", "prefix": "CN_CPI"},
    # unemployment: if you want "level", add separately; otherwise omit
]

# 4) Quarterly macro: store ONLY YoY/QoQ (no levels)
QUARTERLY_LEVEL_SERIES = [
    {"series_id": "GDPC1", "prefix": "US_RealGDP"},
    {"series_id": "CLVMNACSCAB1GQEA", "prefix": "EZ_RealGDP"},
    {"series_id": "CLVMNACSCAB1GQDE", "prefix": "DE_RealGDP"},
    {"series_id": "CHNGDPNQDSMEI", "prefix": "CN_RealGDP"},
]


# =========================
# Helpers: Google Sheets
# =========================
def get_gspread_client(json_str: str):
    info = json.loads(json_str)
    creds = Credentials.from_service_account_info(
        info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)


def ensure_worksheet(sh, tab_name: str, rows="5000", cols="50"):
    try:
        return sh.worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound:
        return sh.add_worksheet(title=tab_name, rows=rows, cols=cols)


def get_header_and_last_date(ws):
    """
    Returns (header_list or None, last_date_str or None)
    last_date_str expected format: YYYY-MM-DD
    """
    values = ws.get_all_values()
    if not values or not values[0] or values[0][0] != "Date":
        return None, None

    header = values[0]
    if len(values) >= 2 and values[-1] and values[-1][0]:
        return header, values[-1][0]
    return header, None


def write_header(ws, headers: list[str]):
    ws.update("A1", [headers])


def append_rows(ws, rows: list[list]):
    if not rows:
        return 0
    ws.append_rows(rows, value_input_option="USER_ENTERED")
    return len(rows)


def pick_start_date(last_date_str: str | None, default_start: str) -> str:
    if not last_date_str:
        return default_start
    try:
        d = datetime.strptime(last_date_str, "%Y-%m-%d") + timedelta(days=1)
        return d.strftime("%Y-%m-%d")
    except ValueError:
        return default_start


# =========================
# Helpers: FRED transforms
# =========================
def fred_series(fred: Fred, series_id: str, start_date: str) -> pd.Series:
    s = fred.get_series(series_id, observation_start=start_date)
    if s is None or len(s) == 0:
        return pd.Series(dtype="float64")
    s.index = pd.to_datetime(s.index)
    return s.sort_index()


def pct_change(series: pd.Series, periods: int) -> pd.Series:
    return series.pct_change(periods) * 100.0


def build_monthly_rates(level: pd.Series, prefix: str) -> pd.DataFrame:
    """
    monthly level -> YoY/MoM
    """
    df = pd.DataFrame(index=level.index)
    df[f"{prefix}_YoY"] = pct_change(level, 12)
    df[f"{prefix}_MoM"] = pct_change(level, 1)
    return df


def build_quarterly_rates(level: pd.Series, prefix: str) -> pd.DataFrame:
    """
    quarterly level -> YoY/QoQ
    """
    df = pd.DataFrame(index=level.index)
    df[f"{prefix}_YoY"] = pct_change(level, 4)
    df[f"{prefix}_QoQ"] = pct_change(level, 1)
    return df


# =========================
# OFR loader (weekly)
# =========================
def load_ofr_multifull(mnemonics: list[str], start_date: str) -> pd.DataFrame:
    url = "https://data.financialresearch.gov/v1/series/multifull"
    params = {"mnemonics": ",".join(mnemonics)}
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    raw = resp.json()

    frames = []
    for mnem, entry in raw.items():
        ts = entry.get("timeseries", {})
        agg = ts.get("aggregation")
        if not agg:
            continue
        tmp = pd.DataFrame(agg, columns=["Date", mnem])
        tmp["Date"] = pd.to_datetime(tmp["Date"])
        tmp = tmp.set_index("Date").sort_index()
        frames.append(tmp)

    if not frames:
        return pd.DataFrame()

    out = pd.concat(frames, axis=1).sort_index()
    out = out[out.index >= pd.to_datetime(start_date)]
    return out


# =========================
# Update routines per tab
# =========================
def update_daily(fred: Fred, sh):
    tab = "data-daily"
    ws = ensure_worksheet(sh, tab)

    target_headers = ["Date"] + list(DAILY_FRED_SERIES.values())
    header, last_date = get_header_and_last_date(ws)

    if header != target_headers:
        write_header(ws, target_headers)

    start_date = pick_start_date(last_date, default_start="2006-01-01")
    print(f"📌 {tab}: start_date={start_date}")

    combined = pd.DataFrame()
    for sid, col in DAILY_FRED_SERIES.items():
        try:
            s = fred_series(fred, sid, start_date)
            if s.empty:
                continue
            tmp = s.to_frame(name=col)
            combined = tmp if combined.empty else combined.join(tmp, how="outer")
            time.sleep(0.15)
        except Exception as e:
            print(f"⚠️ {tab} FRED load failed {sid}: {e}")

    if combined.empty:
        print(f"ℹ️ {tab}: no new rows")
        return

    combined.index.name = "Date"
    combined = combined.reset_index()
    combined["Date"] = pd.to_datetime(combined["Date"]).dt.strftime("%Y-%m-%d")
    combined = combined[["Date"] + list(DAILY_FRED_SERIES.values())].fillna("")
    n = append_rows(ws, combined.values.tolist())
    print(f"✅ {tab}: appended {n} rows")


def update_weekly_ofr(sh):
    tab = "data-weekly"
    ws = ensure_worksheet(sh, tab)

    target_headers = ["Date"] + list(WEEKLY_OFR_MNEMONICS.values())
    header, last_date = get_header_and_last_date(ws)

    if header != target_headers:
        write_header(ws, target_headers)

    start_date = pick_start_date(last_date, default_start="2012-01-01")
    print(f"📌 {tab}: start_date={start_date}")

    try:
        df = load_ofr_multifull(list(WEEKLY_OFR_MNEMONICS.keys()), start_date)
        if df.empty:
            print(f"ℹ️ {tab}: no new rows")
            return
        df = df.rename(columns=WEEKLY_OFR_MNEMONICS)

        df.index.name = "Date"
        df = df.reset_index()
        df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")
        df = df[["Date"] + list(WEEKLY_OFR_MNEMONICS.values())].fillna("")
        n = append_rows(ws, df.values.tolist())
        print(f"✅ {tab}: appended {n} rows")
    except Exception as e:
        print(f"⚠️ {tab}: OFR update failed: {e}")


def update_monthly_rates(fred: Fred, sh):
    tab = "data-monthly"
    ws = ensure_worksheet(sh, tab)

    # target headers based on configured series
    cols = []
    for item in MONTHLY_LEVEL_SERIES:
        p = item["prefix"]
        cols += [f"{p}_YoY", f"{p}_MoM"]
    target_headers = ["Date"] + cols

    header, last_date = get_header_and_last_date(ws)
    if header != target_headers:
        write_header(ws, target_headers)

    start_date = pick_start_date(last_date, default_start="2000-01-01")
    print(f"📌 {tab}: start_date={start_date}")

    combined = pd.DataFrame()
    for item in MONTHLY_LEVEL_SERIES:
        sid = item["series_id"]
        prefix = item["prefix"]
        try:
            lvl = fred_series(fred, sid, "1990-01-01")  # compute rates needs history
            if lvl.empty:
                continue
            rates = build_monthly_rates(lvl, prefix)
            combined = rates if combined.empty else combined.join(rates, how="outer")
            time.sleep(0.15)
        except Exception as e:
            print(f"⚠️ {tab} load/transform failed {sid}: {e}")

    if combined.empty:
        print(f"ℹ️ {tab}: nothing to write")
        return

    # keep only rows >= start_date (incremental write)
    combined = combined[combined.index >= pd.to_datetime(start_date)]
    if combined.empty:
        print(f"ℹ️ {tab}: no new rows after filtering")
        return

    combined.index.name = "Date"
    combined = combined.reset_index()
    combined["Date"] = pd.to_datetime(combined["Date"]).dt.strftime("%Y-%m-%d")
    combined = combined[["Date"] + cols].fillna("")
    n = append_rows(ws, combined.values.tolist())
    print(f"✅ {tab}: appended {n} rows")


def update_quarterly_rates(fred: Fred, sh):
    tab = "data-quarterly"
    ws = ensure_worksheet(sh, tab)

    cols = []
    for item in QUARTERLY_LEVEL_SERIES:
        p = item["prefix"]
        cols += [f"{p}_YoY", f"{p}_QoQ"]
    target_headers = ["Date"] + cols

    header, last_date = get_header_and_last_date(ws)
    if header != target_headers:
        write_header(ws, target_headers)

    start_date = pick_start_date(last_date, default_start="1990-01-01")
    print(f"📌 {tab}: start_date={start_date}")

    combined = pd.DataFrame()
    for item in QUARTERLY_LEVEL_SERIES:
        sid = item["series_id"]
        prefix = item["prefix"]
        try:
            lvl = fred_series(fred, sid, "1970-01-01")  # compute rates needs history
            if lvl.empty:
                continue
            rates = build_quarterly_rates(lvl, prefix)
            combined = rates if combined.empty else combined.join(rates, how="outer")
            time.sleep(0.15)
        except Exception as e:
            print(f"⚠️ {tab} load/transform failed {sid}: {e}")

    if combined.empty:
        print(f"ℹ️ {tab}: nothing to write")
        return

    combined = combined[combined.index >= pd.to_datetime(start_date)]
    if combined.empty:
        print(f"ℹ️ {tab}: no new rows after filtering")
        return

    combined.index.name = "Date"
    combined = combined.reset_index()
    combined["Date"] = pd.to_datetime(combined["Date"]).dt.strftime("%Y-%m-%d")
    combined = combined[["Date"] + cols].fillna("")
    n = append_rows(ws, combined.values.tolist())
    print(f"✅ {tab}: appended {n} rows")


# =========================
# Main
# =========================
def main():
    fred = Fred(api_key=FRED_API_KEY)
    gc = get_gspread_client(SERVICE_ACCOUNT_JSON)
    sh = gc.open_by_key(GSHEET_ID)

    # Run updates
    update_daily(fred, sh)
    update_weekly_ofr(sh)
    update_monthly_rates(fred, sh)
    update_quarterly_rates(fred, sh)

    print("\n🎉 All updates completed.")


if __name__ == "__main__":
    main()
