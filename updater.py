import os
import json
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional

from fredapi import Fred
import gspread
from google.oauth2.service_account import Credentials


# =========================
# ENV (GitHub Secrets)
# =========================
FRED_API_KEY = os.environ["FRED_API_KEY"]
GSHEET_ID = os.environ["GSHEET_ID"]
SERVICE_ACCOUNT_JSON = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]

# BoK ECOS
BOK_API_KEY = os.environ["BOK_API_KEY"]


# =========================
# CONFIG: Daily / Weekly (KEEP)
# =========================
DAILY_FRED_SERIES = {
    "RPONTTLD": "Repo_Volume",
    "SOFR": "SOFR",
    "SOFR99": "SOFR_99th",
    "DFEDTARU": "Fed_Target_Upper",
    "DFEDTARL": "Fed_Target_Lower",
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

WEEKLY_OFR_MNEMONICS = {
    "NYPD-PD_AFtD_T-A": "UST_fails_to_deliver",
    "NYPD-PD_AFtD_AG-A": "AgencyGSE_fails_to_deliver",
    "NYPD-PD_AFtD_CORS-A": "Corporate_fails_to_deliver",
    "NYPD-PD_AFtD_OMBS-A": "OtherMBS_fails_to_deliver",
}


# =========================
# CONFIG: Monthly/Quarterly (ALL BoK)
# =========================
# Drop HK and DE as you decided. Use Euro area via XM.
CCY_LIST = ["US", "CA", "XM", "CH", "JP", "CN", "KR"]

# ECOS table codes
ECOS_POLICY = "902Y006"  # 주요국 정책금리 [M]
ECOS_CPI    = "902Y008"  # 주요국 소비자물가 지수 [M]  (index)
ECOS_UNEMP  = "902Y021"  # 주요국 실업률(계절조정변동) [M]
ECOS_GROWTH = "902Y015"  # 주요국 경제성장률 [Q]

# =========================
# Google Sheets Helpers
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


def pick_start_date(last_date_str: Optional[str], default_start: str) -> str:
    if not last_date_str:
        return default_start
    try:
        d = datetime.strptime(last_date_str, "%Y-%m-%d") + timedelta(days=1)
        return d.strftime("%Y-%m-%d")
    except ValueError:
        return default_start


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
# ECOS helpers
# =========================
def _tp_to_timestamp(tp: str) -> pd.Timestamp:
    tp = str(tp).strip()
    # Monthly YYYYMM
    if len(tp) == 6 and tp.isdigit():
        return pd.Timestamp(int(tp[:4]), int(tp[4:6]), 1)
    # Daily YYYYMMDD
    if len(tp) == 8 and tp.isdigit():
        return pd.Timestamp(int(tp[:4]), int(tp[4:6]), int(tp[6:8]))
    # Quarterly like 2024Q3 or 2024-Q3 (be permissive)
    if "Q" in tp:
        s = tp.replace("-", "")
        # 2024Q3
        if len(s) == 6 and s[:4].isdigit() and s[4] == "Q" and s[5].isdigit():
            y = int(s[:4]); q = int(s[5])
            month = q * 3
            return pd.Timestamp(y, month, 1) + pd.offsets.MonthEnd(0)
    return pd.to_datetime(tp, errors="coerce")


def ecos_stat_search(
    api_key: str,
    stat_code: str,
    cycle: str,       # "M" or "Q"
    start: str,       # "YYYYMM" or "YYYYQn" 형태를 ECOS가 받는 범위 내에서
    end: str,
    item_code1: str = "?",
    item_code2: str = "?",
    item_code3: str = "?",
    item_code4: str = "?",
    lang: str = "kr",
    timeout: int = 30,
) -> pd.Series:
    """
    ECOS StatisticSearch
    https://ecos.bok.or.kr/api/{KEY}/json/{lang}/1/100000/StatisticSearch/{STAT}/{CYCLE}/{START}/{END}/{ITEM1}/{ITEM2}/{ITEM3}/{ITEM4}
    """
    url = (
        f"https://ecos.bok.or.kr/api/{api_key}/json/{lang}/1/100000/"
        f"StatisticSearch/{stat_code}/{cycle}/{start}/{end}/"
        f"{item_code1}/{item_code2}/{item_code3}/{item_code4}"
    )
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    js = r.json()

    rows = js.get("StatisticSearch", {}).get("row", [])
    if not rows:
        return pd.Series(dtype="float64")

    out = []
    for row in rows:
        t = row.get("TIME")
        v = row.get("DATA_VALUE")
        if not t or v is None:
            continue
        dt = _tp_to_timestamp(t)
        if pd.isna(dt):
            continue
        try:
            fv = float(v)
        except Exception:
            continue
        out.append((dt, fv))

    if not out:
        return pd.Series(dtype="float64")

    s = pd.Series({d: v for d, v in out}).sort_index()
    s.index = pd.to_datetime(s.index)
    s.index.name = "Date"
    return s


def to_period_index(s: pd.Series, freq: str) -> pd.Series:
    """
    Normalize index to:
      - freq="M" => month-start (YYYY-MM-01)
      - freq="Q" => quarter-end (consistent)
    """
    if s is None or s.empty:
        return pd.Series(dtype="float64")
    idx = pd.to_datetime(s.index, errors="coerce")
    out = pd.Series(s.values, index=idx).dropna()
    if out.empty:
        return pd.Series(dtype="float64")

    if freq == "M":
        out.index = out.index.to_period("M").to_timestamp("MS")
        out = out.groupby(out.index).last().sort_index()
    elif freq == "Q":
        out.index = out.index.to_period("Q").to_timestamp("Q")  # quarter end
        out = out.groupby(out.index).last().sort_index()
    else:
        out = out.sort_index()

    out.index.name = "Date"
    return out


def build_cpi_yoy_from_index(cpi_index: pd.Series) -> pd.Series:
    """
    CPI index (monthly) -> YoY% (12m pct change)
    """
    if cpi_index is None or cpi_index.empty:
        return pd.Series(dtype="float64")
    cpi_index = cpi_index.sort_index()
    yoy = cpi_index.pct_change(12) * 100.0
    yoy.name = "CPI_YoY"
    return yoy


# =========================
# Update routines
# =========================
def update_daily(fred, sh):
    FULL_BACKFILL = False
    FULL_START_DATE = "2006-01-01"
    TAB_NAME = "data-daily"
    LOOKBACK_DAYS = 30

    ws = ensure_worksheet(sh, TAB_NAME)

    headers = ["Date"] + list(DAILY_FRED_SERIES.values())
    header, _ = get_header_and_last_date(ws)
    if header != headers:
        ws.clear()
        ws.append_row(headers, value_input_option="USER_ENTERED")

    records = ws.get_all_records()
    if records:
        df_existing = pd.DataFrame(records)
        if "Date" in df_existing.columns:
            df_existing["Date"] = pd.to_datetime(df_existing["Date"], errors="coerce")
            df_existing = df_existing.dropna(subset=["Date"]).set_index("Date").sort_index()
        else:
            df_existing = pd.DataFrame(columns=headers[1:])
            df_existing.index.name = "Date"
    else:
        df_existing = pd.DataFrame(columns=headers[1:])
        df_existing.index.name = "Date"

    if FULL_BACKFILL:
        df_existing = pd.DataFrame(columns=headers[1:])
        df_existing.index.name = "Date"
        pull_start = FULL_START_DATE
        print(f"🚨 {TAB_NAME}: FULL_BACKFILL from {pull_start}")
    else:
        pull_start = (datetime.utcnow() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")
        print(f"📌 {TAB_NAME}: pulling from {pull_start} (UTC)")

    df_pulled = pd.DataFrame()
    for sid, col in DAILY_FRED_SERIES.items():
        try:
            s = fred.get_series(sid, observation_start=pull_start)
            if s is None or len(s) == 0:
                continue
            s = s.sort_index()
            s.index = pd.to_datetime(s.index)
            df_pulled = s.to_frame(name=col) if df_pulled.empty else df_pulled.join(s.to_frame(name=col), how="outer")
            time.sleep(0.12)
        except Exception as e:
            print(f"⚠️ DAILY load failed: {sid} ({e})")

    if df_pulled.empty:
        print(f"ℹ️ {TAB_NAME}: no data pulled from FRED")
        return

    df_existing_clean = df_existing.copy()
    for c in df_existing_clean.columns:
        df_existing_clean[c] = df_existing_clean[c].replace("", pd.NA)

    df_merged = df_existing_clean.combine_first(df_pulled)
    for c in headers[1:]:
        if c not in df_merged.columns:
            df_merged[c] = pd.NA
    df_merged = df_merged[headers[1:]]

    df_out = df_merged.reset_index()
    df_out["Date"] = pd.to_datetime(df_out["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df_out = df_out.fillna("")

    ws.clear()
    ws.update([headers] + df_out.values.tolist(), value_input_option="USER_ENTERED")
    print(f"✅ {TAB_NAME}: rewritten rows={len(df_out)} cols={len(headers)}")


def update_weekly_ofr(sh):
    tab = "data-weekly"
    ws = ensure_worksheet(sh, tab)

    headers = ["Date"] + list(WEEKLY_OFR_MNEMONICS.values())
    header, last_date = get_header_and_last_date(ws)
    if header != headers:
        ws.clear()
        write_header(ws, headers)

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


def update_monthly_bok_only(sh):
    """
    Monthly ONLY headers:
      - {CCY}_CPI_YoY
      - {CCY}_Unemployment
      - {CCY}_PolicyRate
    Source: BoK ECOS
    Note: CPI is provided as index (902Y008); YoY% computed in code.
    """
    tab = "data-monthly"
    ws = ensure_worksheet(sh, tab)

    cols = []
    for ccy in CCY_LIST:
        cols += [f"{ccy}_CPI_YoY", f"{ccy}_Unemployment", f"{ccy}_PolicyRate"]
    headers = ["Date"] + cols

    header, last_date = get_header_and_last_date(ws)
    if header != headers:
        ws.clear()
        write_header(ws, headers)

    # Pull window: long enough to compute YoY reliably
    # If you already have data, look back 15 months to avoid missing revisions and to compute YoY.
    if last_date:
        d_last = pd.to_datetime(last_date, errors="coerce")
        if pd.isna(d_last):
            start_dt = pd.Timestamp("2000-01-01")
        else:
            start_dt = (d_last - pd.DateOffset(months=15)).to_period("M").to_timestamp("MS")
    else:
        start_dt = pd.Timestamp("2000-01-01")

    start_ym = start_dt.strftime("%Y%m")  # ECOS wants YYYYMM
    end_ym = pd.Timestamp.today().strftime("%Y%m")
    print(f"📌 {tab}: ECOS window {start_ym} ~ {end_ym}")

    combined = pd.DataFrame()

    for ccy in CCY_LIST:
        try:
            # CPI index -> YoY%
            cpi_ix = ecos_stat_search(BOK_API_KEY, ECOS_CPI, "M", start_ym, end_ym, item_code1=ccy)
            cpi_ix = to_period_index(cpi_ix, "M")
            cpi_yoy = build_cpi_yoy_from_index(cpi_ix)

            # Unemployment level %
            un = ecos_stat_search(BOK_API_KEY, ECOS_UNEMP, "M", start_ym, end_ym, item_code1=ccy)
            un = to_period_index(un, "M")

            # Policy rate level %
            pr = ecos_stat_search(BOK_API_KEY, ECOS_POLICY, "M", start_ym, end_ym, item_code1=ccy)
            pr = to_period_index(pr, "M")

            tmp = pd.DataFrame(index=cpi_yoy.index.union(un.index).union(pr.index).sort_values())
            if not cpi_yoy.empty:
                tmp[f"{ccy}_CPI_YoY"] = cpi_yoy
            if not un.empty:
                tmp[f"{ccy}_Unemployment"] = un
            if not pr.empty:
                tmp[f"{ccy}_PolicyRate"] = pr

            combined = tmp if combined.empty else combined.join(tmp, how="outer")

            time.sleep(0.08)

        except Exception as e:
            print(f"⚠️ {tab}: ECOS monthly failed for {ccy}: {e}")

    if combined.empty:
        print(f"❌ {tab}: combined empty (ECOS failed).")
        return

    # Normalize monthly index to month-start and keep window
    combined.index = pd.to_datetime(combined.index, errors="coerce")
    combined = combined[~combined.index.isna()]
    combined = combined.groupby(combined.index.to_period("M").to_timestamp("MS")).last().sort_index()
    combined.index.name = "Date"

    # cut from start_dt
    combined = combined[combined.index >= start_dt]
    combined = combined.dropna(how="all")

    # ensure requested cols only
    for c in cols:
        if c not in combined.columns:
            combined[c] = pd.NA
    combined = combined[cols]

    # rewrite whole sheet (stable)
    out = combined.reset_index()
    out["Date"] = pd.to_datetime(out["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    out = out.fillna("")

    ws.clear()
    ws.update([headers] + out.values.tolist(), value_input_option="USER_ENTERED")
    print(f"✅ {tab}: rewritten rows={len(out)} cols={len(headers)}")


def update_quarterly_bok_only(sh):
    """
    Quarterly ONLY headers:
      - {CCY}_Growth
    Source: BoK ECOS 902Y015[Q]
    """
    tab = "data-quarterly"
    ws = ensure_worksheet(sh, tab)

    cols = [f"{ccy}_Growth" for ccy in CCY_LIST]
    headers = ["Date"] + cols

    header, last_date = get_header_and_last_date(ws)
    if header != headers:
        ws.clear()
        write_header(ws, headers)

    # Pull window
    if last_date:
        d_last = pd.to_datetime(last_date, errors="coerce")
        if pd.isna(d_last):
            start_dt = pd.Timestamp("1990-01-01")
        else:
            start_dt = d_last - pd.DateOffset(months=9)  # 3 quarters lookback
    else:
        start_dt = pd.Timestamp("1990-01-01")

    # ECOS quarterly range: best-effort using YYYYQn-like numeric is inconsistent;
    # We will still pass YYYYMM bounds but cycle=Q often works with TIME parsing returned as 2024Q3 etc.
    start_q_hint = start_dt.strftime("%Y%m")
    end_q_hint = pd.Timestamp.today().strftime("%Y%m")
    print(f"📌 {tab}: ECOS window {start_q_hint} ~ {end_q_hint}")

    combined = pd.DataFrame()

    for ccy in CCY_LIST:
        try:
            s = ecos_stat_search(BOK_API_KEY, ECOS_GROWTH, "Q", start_q_hint, end_q_hint, item_code1=ccy)
            s = to_period_index(s, "Q")
            if s.empty:
                print(f"⚠️ {tab}: growth empty for {ccy}")
                continue
            tmp = s.to_frame(name=f"{ccy}_Growth")
            combined = tmp if combined.empty else combined.join(tmp, how="outer")
            time.sleep(0.08)
        except Exception as e:
            print(f"⚠️ {tab}: ECOS quarterly failed for {ccy}: {e}")

    if combined.empty:
        print(f"❌ {tab}: combined empty.")
        return

    combined = combined.sort_index()
    combined = combined[combined.index >= pd.to_datetime(start_dt)]
    combined = combined.dropna(how="all")

    for c in cols:
        if c not in combined.columns:
            combined[c] = pd.NA
    combined = combined[cols]

    out = combined.reset_index()
    out["Date"] = pd.to_datetime(out["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    out = out.fillna("")

    ws.clear()
    ws.update([headers] + out.values.tolist(), value_input_option="USER_ENTERED")
    print(f"✅ {tab}: rewritten rows={len(out)} cols={len(headers)}")


# =========================
# Main
# =========================
def main():
    fred = Fred(api_key=FRED_API_KEY)
    gc = get_gspread_client(SERVICE_ACCOUNT_JSON)
    sh = gc.open_by_key(GSHEET_ID)

    # daily / weekly keep as-is
    update_daily(fred, sh)
    update_weekly_ofr(sh)

    # simplified monthly / quarterly (ALL BoK)
    update_monthly_bok_only(sh)
    update_quarterly_bok_only(sh)

    print("\n🎉 All updates completed.")


if __name__ == "__main__":
    main()
