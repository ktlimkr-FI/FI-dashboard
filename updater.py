"""
FI-dashboard updater.py (consolidated)

- Daily: FRED (unchanged)
- Weekly: OFR repo fails (unchanged)
- Monthly:
  - Headline CPI: OECD G20_PRICES (DF_G20_PRICES) -> YoY/MoM computed
  - Unemployment rate: OECD LFS (DF_IALFS_UNE_M) -> level %
  - Policy rate:
      * Euro Area (EA) + Germany (DE): ECB Deposit Facility Rate (DFR) from ECB Data Portal API
        -> daily series resampled to month-end (last)
      * Others (US/CA/JP/CN/CH/KR): Bank of Korea ECOS table 902Y006 (monthly)
  - Core CPI: NOT collected (by decision)

- Quarterly:
  - Real GDP growth: OECD QNA expenditure growth (DF_QNA_EXPENDITURE_GROWTH_G20)
    -> attempts to map YoY/QoQ if a dimension indicates it; otherwise fills QoQ only.
"""

import os
import json
import time
import requests
import pandas as pd
from io import StringIO
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

# Optional (for policy rates outside EA/DE via BOK)
BOK_API_KEY = os.environ.get("BOK_API_KEY", "")


# =========================
# CONFIG: What to store
# =========================

# 1) Daily: store levels (native daily) - UNCHANGED
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

# 2) Weekly: OFR (native weekly) - UNCHANGED
WEEKLY_OFR_MNEMONICS = {
    "NYPD-PD_AFtD_T-A": "UST_fails_to_deliver",
    "NYPD-PD_AFtD_AG-A": "AgencyGSE_fails_to_deliver",
    "NYPD-PD_AFtD_CORS-A": "Corporate_fails_to_deliver",
    "NYPD-PD_AFtD_OMBS-A": "OtherMBS_fails_to_deliver",
}

# 3) Monthly: OECD CPI + OECD Unemployment + (ECB/ECOS) policy rate
#    Core CPI not collected.
OECD_LOC = {
    "US": "USA",
    "CA": "CAN",
    "DE": "DEU",
    "EA": "EA20",   # consistent with your QNA URL using EA20
    "CH": "CHE",
    "JP": "JPN",
    "CN": "CHN",
    "HK": "HKG",
    "KR": "KOR",
}

# ECOS 902Y006 supports (commonly): US/CA/JP/CN/CH/KR
ECOS_POLICY_STAT_CODE = "902Y006"
ECOS_POLICY_ITEM = {
    "US": "US",
    "CA": "CA",
    "JP": "JP",
    "CN": "CN",
    "CH": "CH",
    "KR": "KR",
}

# ECB DFR (Deposit Facility Rate) via ECB Data Portal API
ECB_BASE = "https://data-api.ecb.europa.eu/service/data"
ECB_DFR_FLOW = "FM"
ECB_DFR_KEY = "FM.D.U2.EUR.4F.KR.DFR.LEV"  # DFR level daily


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


def pick_start_date(last_date_str: Optional[str], default_start: str) -> str:
    """
    For daily/weekly tabs where start_date is YYYY-MM-DD
    """
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
    df = pd.DataFrame(index=level.index)
    df[f"{prefix}_YoY"] = pct_change(level, 12)
    df[f"{prefix}_MoM"] = pct_change(level, 1)
    return df


# =========================
# OFR loader (weekly) - UNCHANGED
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
# OECD / ECB / ECOS helpers
# =========================
def _find_col(df: pd.DataFrame, candidates: list[str]) -> str:
    for c in candidates:
        if c in df.columns:
            return c
    raise ValueError(f"column not found. candidates={candidates}, columns={list(df.columns)}")


def _tp_to_timestamp(tp: str) -> pd.Timestamp:
    tp = str(tp).strip()
    # monthly: YYYY-MM
    if len(tp) == 7 and tp[4] == "-":
        return pd.Timestamp(tp + "-01")
    # quarterly: YYYY-Qn
    if "-Q" in tp:
        y, q = tp.split("-Q")
        y = int(y); q = int(q)
        month = q * 3
        return pd.Timestamp(y, month, 1) + pd.offsets.MonthEnd(0)
    # full date
    return pd.to_datetime(tp, errors="coerce")


def _monthly_start_period(last_date: Optional[str], default_start: str = "2000-01") -> str:
    """
    Returns startPeriod in YYYY-MM for OECD monthly endpoints.
    We use last_date + 1 day, then take YYYY-MM.
    """
    if not last_date:
        return default_start
    d0 = pd.to_datetime(last_date, errors="coerce")
    if pd.isna(d0):
        return default_start
    d0 = d0 + pd.Timedelta(days=1)
    return d0.strftime("%Y-%m")


def _quarterly_start_period(last_date: Optional[str], default_start: str = "1990-Q1") -> str:
    """
    Returns startPeriod in YYYY-Qn.
    """
    if not last_date:
        return default_start
    d0 = pd.to_datetime(last_date, errors="coerce")
    if pd.isna(d0):
        return default_start
    d0 = d0 + pd.Timedelta(days=1)
    q = ((d0.month - 1) // 3) + 1
    return f"{d0.year}-Q{q}"


def oecd_get_csv(url: str, timeout: int = 90) -> pd.DataFrame:
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    return pd.read_csv(StringIO(resp.text))


def ecb_get_series_csv(flow: str, key: str, start_period: str, end_period: Optional[str] = None, timeout: int = 60) -> pd.Series:
    """
    ECB Data Portal SDMX API (CSV) -> Series
    """
    url = f"{ECB_BASE}/{flow}/{key}?startPeriod={start_period}&format=csvfilewithlabels"
    if end_period:
        url += f"&endPeriod={end_period}"

    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    df = pd.read_csv(StringIO(r.text))

    tp_col = _find_col(df, ["TIME_PERIOD", "Time period"])
    val_col = _find_col(df, ["OBS_VALUE", "Value"])

    df[tp_col] = df[tp_col].astype(str)
    df[val_col] = pd.to_numeric(df[val_col], errors="coerce")

    df["_dt"] = pd.to_datetime(df[tp_col], errors="coerce")
    df = df.dropna(subset=["_dt", val_col]).sort_values("_dt")

    s = pd.Series(df[val_col].values, index=pd.to_datetime(df["_dt"].values)).sort_index()
    s.index.name = "Date"
    return s


def ecos_stat_search(
    api_key: str,
    stat_code: str,
    cycle: str,           # "M"
    start: str,           # "YYYYMM"
    end: str,             # "YYYYMM"
    item_code1: str = "?",
    item_code2: str = "?",
    item_code3: str = "?",
    item_code4: str = "?",
    lang: str = "kr",
    timeout: int = 30,
) -> pd.Series:
    """
    ECOS StatisticSearch:
    https://ecos.bok.or.kr/api/{KEY}/json/{lang}/1/100000/StatisticSearch/{STAT}/{CYCLE}/{START}/{END}/{ITEM1}/{ITEM2}/{ITEM3}/{ITEM4}
    """
    if not api_key:
        return pd.Series(dtype="float64")

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
        dt = _tp_to_timestamp(t)  # "YYYYMM" also parses
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
    return s


# =========================
# Update routines per tab
# =========================
def update_daily(fred, sh):
    """
    Daily updater that BACKFILLS missing values by merging with existing sheet data.
    - Pull last N days from FRED (lookback)
    - Read existing sheet into DataFrame
    - Merge (prefer existing unless missing; fill missing from fresh pull)
    - Rewrite the whole sheet
    """
    FULL_BACKFILL = False
    FULL_START_DATE = "2006-01-01"

    TAB_NAME = "data-daily"
    LOOKBACK_DAYS = 30

    ws = ensure_worksheet(sh, TAB_NAME)

    headers = ["Date"] + list(DAILY_FRED_SERIES.values())
    header, _last_date_str = get_header_and_last_date(ws)

    if header != headers:
        ws.clear()
        ws.append_row(headers, value_input_option="USER_ENTERED")

    records = ws.get_all_records()
    if records:
        df_existing = pd.DataFrame(records)
        if "Date" not in df_existing.columns:
            df_existing = pd.DataFrame(columns=headers[1:])
            df_existing.index.name = "Date"
        else:
            df_existing["Date"] = pd.to_datetime(df_existing["Date"], errors="coerce")
            df_existing = df_existing.dropna(subset=["Date"]).set_index("Date").sort_index()
    else:
        df_existing = pd.DataFrame(columns=headers[1:])
        df_existing.index.name = "Date"

    if FULL_BACKFILL:
        print("🚨 FULL BACKFILL MODE: ignoring existing sheet data")
        df_existing = pd.DataFrame(columns=headers[1:])
        df_existing.index.name = "Date"

    if FULL_BACKFILL:
        pull_start = FULL_START_DATE
        print(f"🚨 FULL BACKFILL MODE: pulling full history from {pull_start}")
    else:
        pull_start = (datetime.utcnow() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")
        print(f"📌 {TAB_NAME}: pulling from {pull_start} (UTC)")

    df_pulled = pd.DataFrame()
    df_pulled.index.name = "Date"

    for sid, col in DAILY_FRED_SERIES.items():
        try:
            s = fred.get_series(sid, observation_start=pull_start)
            if s is None or len(s) == 0:
                continue

            s = s.sort_index()
            s.index = pd.to_datetime(s.index)

            tmp = s.to_frame(name=col)
            df_pulled = tmp if df_pulled.empty else df_pulled.join(tmp, how="outer")

            time.sleep(0.15)
        except Exception as e:
            print(f"⚠️ DAILY load failed: {sid} ({e})")

    if df_pulled.empty:
        print(f"ℹ️ {TAB_NAME}: no data pulled from FRED")
        return

    df_existing_clean = df_existing.copy()
    for c in df_existing_clean.columns:
        df_existing_clean[c] = df_existing_clean[c].replace("", pd.NA)

    df_merged = df_existing_clean.combine_first(df_pulled)
    df_merged.index.name = "Date"

    for c in headers[1:]:
        if c not in df_merged.columns:
            df_merged[c] = pd.NA
    df_merged = df_merged[headers[1:]]

    df_out = df_merged.reset_index()
    if "Date" not in df_out.columns and "index" in df_out.columns:
        df_out = df_out.rename(columns={"index": "Date"})

    if "Date" not in df_out.columns:
        raise ValueError("Internal error: Date column missing after reset_index().")

    df_out["Date"] = pd.to_datetime(df_out["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df_out = df_out.fillna("")

    values = [headers] + df_out.values.tolist()
    ws.clear()
    ws.update(values, value_input_option="USER_ENTERED")

    print(f"✅ {TAB_NAME}: rewritten rows={len(df_out)} cols={len(headers)}")


def update_weekly_ofr(sh):
    tab = "data-weekly"
    ws = ensure_worksheet(sh, tab)

    target_headers = ["Date"] + list(WEEKLY_OFR_MNEMONICS.values())
    header, last_date = get_header_and_last_date(ws)

    if header != target_headers:
        ws.clear()
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
    """
    Monthly:
      - CPI headline: OECD G20_PRICES -> YoY/MoM
      - Unemployment: OECD LFS -> level %
      - Policy rate:
          EA, DE: ECB DFR daily -> month-end last
          Others: ECOS 902Y006 (where available)
      - Core CPI: not collected
    """
    tab = "data-monthly"
    ws = ensure_worksheet(sh, tab)

    ccy_list = list(OECD_LOC.keys())

    # CoreCPI removed
    cols = []
    for ccy in ccy_list:
        cols += [
            f"{ccy}_CPI_YoY", f"{ccy}_CPI_MoM",
            f"{ccy}_Unemployment",
            f"{ccy}_PolicyRate",
        ]
    target_headers = ["Date"] + cols

    header, last_date = get_header_and_last_date(ws)
    if header != target_headers:
        ws.clear()
        write_header(ws, target_headers)

    start_period = _monthly_start_period(last_date, default_start="2000-01")
    print(f"📌 {tab}: startPeriod={start_period}")

    combined = pd.DataFrame()

    # -------------------------
    # 1) Headline CPI: OECD G20_PRICES
    # Your pattern: /CHN.M...PA...?
    # We expand to multiple locations using "+".
    # -------------------------
    try:
        locs = "+".join([OECD_LOC[c] for c in ccy_list])
        url_cpi = (
            "https://sdmx.oecd.org/public/rest/data/"
            "OECD.SDD.TPS,DSD_G20_PRICES@DF_G20_PRICES"
            f",/{locs}.M...PA...?"
            f"startPeriod={start_period}"
            "&dimensionAtObservation=AllDimensions"
            "&format=csvfilewithlabels"
        )
        df = oecd_get_csv(url_cpi, timeout=120)

        tp_col = _find_col(df, ["TIME_PERIOD", "Time period"])
        val_col = _find_col(df, ["OBS_VALUE", "Value"])
        loc_col = _find_col(df, ["LOCATION", "Location"])

        df[tp_col] = df[tp_col].astype(str)
        df[val_col] = pd.to_numeric(df[val_col], errors="coerce")
        df["_dt"] = df[tp_col].apply(_tp_to_timestamp)
        df = df.dropna(subset=["_dt"])

        piv = df.pivot_table(index="_dt", columns=loc_col, values=val_col, aggfunc="last").sort_index()

        for ccy in ccy_list:
            loc = OECD_LOC[ccy]
            if loc not in piv.columns:
                continue
            level = piv[loc].dropna()
            if level.empty:
                continue
            rates = build_monthly_rates(level, f"{ccy}_CPI")
            combined = rates if combined.empty else combined.join(rates, how="outer")

    except Exception as e:
        print(f"⚠️ {tab}: OECD CPI failed: {e}")

    # -------------------------
    # 2) Unemployment: OECD LFS
    # Your URL:
    #   .../DSD_LFS@DF_IALFS_UNE_M,/..._Z.Y._T.Y_GE15..M?startPeriod=...
    # We filter to our needed LOCATION codes after download.
    # -------------------------
    try:
        url_unemp = (
            "https://sdmx.oecd.org/public/rest/data/"
            "OECD.SDD.TPS,DSD_LFS@DF_IALFS_UNE_M"
            ",/..._Z.Y._T.Y_GE15..M?"
            f"startPeriod={start_period}"
            "&dimensionAtObservation=AllDimensions"
            "&format=csvfilewithlabels"
        )
        dfu = oecd_get_csv(url_unemp, timeout=180)

        tp_col = _find_col(dfu, ["TIME_PERIOD", "Time period"])
        val_col = _find_col(dfu, ["OBS_VALUE", "Value"])
        loc_col = _find_col(dfu, ["LOCATION", "Location"])

        dfu[tp_col] = dfu[tp_col].astype(str)
        dfu[val_col] = pd.to_numeric(dfu[val_col], errors="coerce")
        dfu["_dt"] = dfu[tp_col].apply(_tp_to_timestamp)
        dfu = dfu.dropna(subset=["_dt"])

        keep_locs = set(OECD_LOC.values())
        dfu = dfu[dfu[loc_col].isin(keep_locs)]

        piv = dfu.pivot_table(index="_dt", columns=loc_col, values=val_col, aggfunc="last").sort_index()

        tmp = pd.DataFrame(index=piv.index)
        for ccy in ccy_list:
            loc = OECD_LOC[ccy]
            if loc in piv.columns:
                tmp[f"{ccy}_Unemployment"] = piv[loc]

        if not tmp.empty:
            combined = tmp if combined.empty else combined.join(tmp, how="outer")

    except Exception as e:
        print(f"⚠️ {tab}: OECD Unemployment failed: {e}")

    # -------------------------
    # 3) Policy Rate
    # 3-1) EA & DE: ECB DFR (daily -> month-end last)
    # -------------------------
    try:
        # For ECB daily, startPeriod is safer as YYYY-MM-DD
        d0 = f"{start_period}-01" if len(start_period) == 7 else start_period
        dfr_daily = ecb_get_series_csv(ECB_DFR_FLOW, ECB_DFR_KEY, start_period=d0, timeout=60)

        if not dfr_daily.empty:
            dfr_m = dfr_daily.resample("M").last()
            tmp = pd.DataFrame(index=dfr_m.index)
            tmp["EA_PolicyRate"] = dfr_m
            tmp["DE_PolicyRate"] = dfr_m  # Germany uses euro policy rate
            combined = tmp if combined.empty else combined.join(tmp, how="outer")

    except Exception as e:
        print(f"⚠️ {tab}: ECB DFR failed: {e}")

    # 3-2) Others: ECOS 902Y006 (monthly)
    try:
        if BOK_API_KEY:
            ecos_start = start_period.replace("-", "")  # YYYYMM
            ecos_end = datetime.utcnow().strftime("%Y%m")

            tmp = pd.DataFrame()
            for ccy, item1 in ECOS_POLICY_ITEM.items():
                s = ecos_stat_search(
                    api_key=BOK_API_KEY,
                    stat_code=ECOS_POLICY_STAT_CODE,
                    cycle="M",
                    start=ecos_start,
                    end=ecos_end,
                    item_code1=item1,
                )
                if s.empty:
                    continue
                col = f"{ccy}_PolicyRate"
                tmp = s.to_frame(name=col) if tmp.empty else tmp.join(s.to_frame(name=col), how="outer")

            if not tmp.empty:
                combined = tmp if combined.empty else combined.join(tmp, how="outer")
        else:
            print(f"ℹ️ {tab}: BOK_API_KEY missing; skipping ECOS policy rates.")

    except Exception as e:
        print(f"⚠️ {tab}: ECOS PolicyRate failed: {e}")

    # -------------------------
    # Finalize / append
    # -------------------------
    if combined.empty:
        print(f"❌ {tab}: combined is empty.")
        return

    combined = combined.sort_index()

    # filter strictly after last_date to avoid duplicates
    if last_date:
        dt0 = pd.to_datetime(last_date, errors="coerce") + pd.Timedelta(days=1)
        combined = combined[combined.index >= dt0]

    if combined.empty:
        print(f"ℹ️ {tab}: no new rows")
        return

    combined.index.name = "Date"
    out = combined.reset_index()
    out["Date"] = pd.to_datetime(out["Date"], errors="coerce").dt.strftime("%Y-%m-%d")

    # ensure all columns exist
    for c in cols:
        if c not in out.columns:
            out[c] = pd.NA
    out = out[["Date"] + cols].fillna("")

    n = append_rows(ws, out.values.tolist())
    print(f"✅ {tab}: appended {n} rows")


def update_quarterly_rates(fred: Fred, sh):
    """
    Quarterly:
      - Real GDP growth from OECD DF_QNA_EXPENDITURE_GROWTH_G20
      - Attempts to map YoY/QoQ via an available dimension; otherwise fills QoQ only.
    """
    tab = "data-quarterly"
    ws = ensure_worksheet(sh, tab)

    ccy_list = list(OECD_LOC.keys())

    cols = []
    for ccy in ccy_list:
        cols += [f"{ccy}_RealGDP_YoY", f"{ccy}_RealGDP_QoQ"]
    target_headers = ["Date"] + cols

    header, last_date = get_header_and_last_date(ws)
    if header != target_headers:
        ws.clear()
        write_header(ws, target_headers)

    start_period = _quarterly_start_period(last_date, default_start="1990-Q1")
    print(f"📌 {tab}: startPeriod={start_period}")

    combined = pd.DataFrame()

    try:
        # Your URL (kept as-is)
        url = (
            "https://sdmx.oecd.org/public/rest/data/"
            "OECD.SDD.NAD,DSD_NAMAIN1@DF_QNA_EXPENDITURE_GROWTH_G20"
            "/Q..ARG+AUS+BRA+CAN+CHN+FRA+DEU+IND+IDN+ITA+JPN+KOR+MEX+RUS+SAU+ZAF+TUR+GBR+USA+OECD+G20+G7+USMCA+OECDE+EA20+EU27_2020..........?"
            f"startPeriod={start_period}"
            "&dimensionAtObservation=AllDimensions"
            "&format=csvfilewithlabels"
        )
        df = oecd_get_csv(url, timeout=180)

        tp_col = _find_col(df, ["TIME_PERIOD", "Time period"])
        val_col = _find_col(df, ["OBS_VALUE", "Value"])
        loc_col = _find_col(df, ["LOCATION", "Location"])

        df[tp_col] = df[tp_col].astype(str)
        df[val_col] = pd.to_numeric(df[val_col], errors="coerce")
        df["_dt"] = df[tp_col].apply(_tp_to_timestamp)
        df = df.dropna(subset=["_dt"])

        # Keep only our locations
        keep_locs = set(OECD_LOC.values())
        df = df[df[loc_col].isin(keep_locs)]

        # Find a dimension that might indicate YoY vs QoQ
        rate_dim = None
        for cand in ["MEASURE", "Measure", "SUBJECT", "Subject", "INDICATOR", "Indicator"]:
            if cand in df.columns:
                rate_dim = cand
                break

        def _map_kind(k: str) -> Optional[str]:
            kl = str(k).lower()
            # heuristics
            if "yoy" in kl or "year" in kl or "y/y" in kl or "a/a" in kl:
                return "YoY"
            if "qoq" in kl or "quarter" in kl or "q/q" in kl:
                return "QoQ"
            return None

        if rate_dim:
            for key, dfg in df.groupby(rate_dim):
                kind = _map_kind(key)
                if kind is None:
                    continue
                piv = dfg.pivot_table(index="_dt", columns=loc_col, values=val_col, aggfunc="last").sort_index()
                tmp = pd.DataFrame(index=piv.index)
                for ccy in ccy_list:
                    loc = OECD_LOC[ccy]
                    if loc in piv.columns:
                        tmp[f"{ccy}_RealGDP_{kind}"] = piv[loc]
                if not tmp.empty:
                    combined = tmp if combined.empty else combined.join(tmp, how="outer")
        else:
            # no dimension found -> treat as QoQ by convention
            piv = df.pivot_table(index="_dt", columns=loc_col, values=val_col, aggfunc="last").sort_index()
            tmp = pd.DataFrame(index=piv.index)
            for ccy in ccy_list:
                loc = OECD_LOC[ccy]
                if loc in piv.columns:
                    tmp[f"{ccy}_RealGDP_QoQ"] = piv[loc]
            combined = tmp

    except Exception as e:
        print(f"⚠️ {tab}: OECD GDP growth failed: {e}")

    if combined.empty:
        print(f"❌ {tab}: combined is empty.")
        return

    combined = combined.sort_index()

    if last_date:
        dt0 = pd.to_datetime(last_date, errors="coerce") + pd.Timedelta(days=1)
        combined = combined[combined.index >= dt0]

    if combined.empty:
        print(f"ℹ️ {tab}: no new rows")
        return

    combined.index.name = "Date"
    out = combined.reset_index()
    out["Date"] = pd.to_datetime(out["Date"], errors="coerce").dt.strftime("%Y-%m-%d")

    for c in cols:
        if c not in out.columns:
            out[c] = pd.NA
    out = out[["Date"] + cols].fillna("")

    n = append_rows(ws, out.values.tolist())
    print(f"✅ {tab}: appended {n} rows")


# =========================
# Main
# =========================
def main():
    fred = Fred(api_key=FRED_API_KEY)
    gc = get_gspread_client(SERVICE_ACCOUNT_JSON)
    sh = gc.open_by_key(GSHEET_ID)

    update_daily(fred, sh)
    update_weekly_ofr(sh)
    update_monthly_rates(fred, sh)
    update_quarterly_rates(fred, sh)

    print("\n🎉 All updates completed.")


if __name__ == "__main__":
    main()
