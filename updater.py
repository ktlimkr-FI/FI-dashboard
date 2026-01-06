"""
updater.py (re-written, end-to-end)

What this script does
---------------------
1) data-daily   : FRED daily series (unchanged logic)
2) data-weekly  : OFR repo fails weekly (unchanged logic)
3) data-monthly :
   - Headline CPI YoY/MoM from OECD DF_G20_PRICES (direct growth rates)
       * YoY: ...GY
       * MoM: ...GM
   - Unemployment (%) from OECD LFS (DF_IALFS_UNE_M)
   - Policy rate (%):
       * Euro Area (EA) + Germany (DE): ECB Deposit Facility Rate (DFR), daily -> month-end(last)
       * Others (US/CA/JP/CN/CH/KR): BOK ECOS table 902Y006, monthly (BOK_API_KEY)
   - Core CPI: NOT collected (by decision)

4) data-quarterly:
   - Real GDP growth from OECD DF_QNA_EXPENDITURE_GROWTH_G20
     (attempts to map YoY/QoQ if a dimension indicates it; otherwise fills QoQ only)

Key reliability fixes (vs previous version)
------------------------------------------
- OECD area column is not always "LOCATION"; it can be "REF_AREA" (now robustly detected).
- ECOS TIME often arrives as YYYYMM; parser now supports YYYYMM and YYYYMMDD.
- Monthly/Quarterly queries use a lookback window to avoid missing late revisions,
  but append filtering still prevents duplicates.

ENV (GitHub Secrets)
--------------------
Required:
- FRED_API_KEY
- GSHEET_ID
- GOOGLE_SERVICE_ACCOUNT_JSON

Optional (but recommended for policy rates outside EA/DE):
- BOK_API_KEY   (ECOS API key)
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

# ECOS (Bank of Korea)
BOK_API_KEY = os.environ.get("BOK_API_KEY", "")


# =========================
# CONFIG: What to store
# =========================

# 1) Daily: store levels (native daily)
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

# 2) Weekly: OFR (native weekly)
WEEKLY_OFR_MNEMONICS = {
    "NYPD-PD_AFtD_T-A": "UST_fails_to_deliver",
    "NYPD-PD_AFtD_AG-A": "AgencyGSE_fails_to_deliver",
    "NYPD-PD_AFtD_CORS-A": "Corporate_fails_to_deliver",
    "NYPD-PD_AFtD_OMBS-A": "OtherMBS_fails_to_deliver",
}

# 3) Monthly / Quarterly countries list
# Dashboard currency codes -> OECD codes
OECD_LOC = {
    "US": "USA",
    "CA": "CAN",
    "DE": "DEU",
    "EA": "EA20",
    "CH": "CHE",
    "JP": "JPN",
    "CN": "CHN",
    "HK": "HKG",
    "KR": "KOR",
}

# ECOS policy rate (monthly)
ECOS_POLICY_STAT_CODE = "902Y006"
ECOS_POLICY_ITEM = {
    "US": "US",
    "CA": "CA",
    "JP": "JP",
    "CN": "CN",
    "CH": "CH",
    "KR": "KR",
}

# ECB DFR via ECB Data Portal API (EDP)
ECB_BASE = "https://data-api.ecb.europa.eu/service/data"
ECB_DFR_FLOW = "FM"
ECB_DFR_KEY = "FM.D.U2.EUR.4F.KR.DFR.LEV"  # Deposit facility rate level (daily)


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
# Time period parsing
# =========================
def _tp_to_timestamp(tp: str) -> pd.Timestamp:
    tp = str(tp).strip()

    # ECOS monthly: YYYYMM
    if len(tp) == 6 and tp.isdigit():
        y = int(tp[:4]); m = int(tp[4:6])
        return pd.Timestamp(y, m, 1)

    # ECOS daily: YYYYMMDD
    if len(tp) == 8 and tp.isdigit():
        y = int(tp[:4]); m = int(tp[4:6]); d = int(tp[6:8])
        return pd.Timestamp(y, m, d)

    # monthly: YYYY-MM
    if len(tp) == 7 and tp[4] == "-":
        return pd.Timestamp(tp + "-01")

    # quarterly: YYYY-Qn
    if "-Q" in tp:
        y, q = tp.split("-Q")
        y = int(y); q = int(q)
        month = q * 3
        return pd.Timestamp(y, month, 1) + pd.offsets.MonthEnd(0)

    # full date or fallback
    return pd.to_datetime(tp, errors="coerce")


# =========================
# Query window helpers (avoid “0 rows” due to revisions / partial periods)
# =========================
def monthly_query_start(last_date: Optional[str], default_start: str = "2000-01", lookback_months: int = 3) -> str:
    """
    Returns YYYY-MM. Uses lookback to reduce the chance of missing revisions.
    """
    if not last_date:
        return default_start
    d = pd.to_datetime(last_date, errors="coerce")
    if pd.isna(d):
        return default_start
    d2 = (d - pd.DateOffset(months=lookback_months)).replace(day=1)
    return d2.strftime("%Y-%m")


def quarterly_query_start(last_date: Optional[str], default_start: str = "1990-Q1", lookback_quarters: int = 2) -> str:
    """
    Returns YYYY-Qn. Uses lookback to reduce the chance of missing revisions.
    """
    if not last_date:
        return default_start
    d = pd.to_datetime(last_date, errors="coerce")
    if pd.isna(d):
        return default_start
    d2 = d - pd.DateOffset(months=3 * lookback_quarters)
    q = ((d2.month - 1) // 3) + 1
    return f"{d2.year}-Q{q}"


# =========================
# Helpers: FRED
# =========================
def fred_series(fred: Fred, series_id: str, start_date: str) -> pd.Series:
    s = fred.get_series(series_id, observation_start=start_date)
    if s is None or len(s) == 0:
        return pd.Series(dtype="float64")
    s.index = pd.to_datetime(s.index)
    return s.sort_index()


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
# OECD / ECB / ECOS helpers
# =========================
def _find_col(df: pd.DataFrame, candidates: list[str]) -> str:
    for c in candidates:
        if c in df.columns:
            return c
    raise ValueError(f"column not found. candidates={candidates}, columns={list(df.columns)}")


def _find_area_col(df: pd.DataFrame) -> str:
    # OECD CSV: REF_AREA is common; sometimes LOCATION
    for cand in [
        "REF_AREA",
        "Reference area",
        "Ref area",
        "LOCATION",
        "Location",
        "REF_AREA:Reference area",
        "LOCATION:Location",
    ]:
        if cand in df.columns:
            return cand
    raise ValueError(f"area column not found. columns={list(df.columns)}")


def oecd_get_csv(url: str, timeout: int = 120) -> pd.DataFrame:
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    return pd.read_csv(StringIO(resp.text))


def ecb_get_series_csv(flow: str, key: str, start_period: str, end_period: Optional[str] = None, timeout: int = 60) -> pd.Series:
    """
    ECB Data Portal SDMX API (CSV) -> Series(Date index)
    """
    url = f"{ECB_BASE}/{flow}/{key}?startPeriod={start_period}&format=csvfilewithlabels"
    if end_period:
        url += f"&endPeriod={end_period}"

    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    df = pd.read_csv(StringIO(r.text))

    tp_col = _find_col(df, ["TIME_PERIOD", "Time period", "TIME_PERIOD:Time period"])
    val_col = _find_col(df, ["OBS_VALUE", "Value", "OBS_VALUE:Value"])

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
    return s


def fetch_oecd_g20_cpi_rate(
    ref_areas: list[str],
    start_period: str,
    end_period: Optional[str],
    growth_code: str,  # "GY" (YoY) or "GM" (MoM)
    timeout: int = 120,
) -> pd.DataFrame:
    """
    OECD DF_G20_PRICES direct CPI growth rates:
      - YoY: ...GY
      - MoM: ...GM
    Example key for China YoY: CHN.M.N.CPI.PA._T.N.GY
    """
    areas = "+".join(ref_areas)
    key = f"{areas}.M.N.CPI.PA._T.N.{growth_code}"

    url = (
        "https://sdmx.oecd.org/public/rest/data/"
        "OECD.SDD.TPS,DSD_G20_PRICES@DF_G20_PRICES,/"
        f"{key}"
        f"?startPeriod={start_period}"
        + (f"&endPeriod={end_period}" if end_period else "")
        + "&dimensionAtObservation=AllDimensions"
        + "&format=csvfilewithlabels"
    )

    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    df = pd.read_csv(StringIO(r.text))

    tp_col = _find_col(df, ["TIME_PERIOD", "Time period", "TIME_PERIOD:Time period"])
    val_col = _find_col(df, ["OBS_VALUE", "Value", "OBS_VALUE:Value"])
    area_col = _find_area_col(df)

    out = df[[tp_col, val_col, area_col]].copy()
    out.columns = ["TIME_PERIOD", "OBS_VALUE", "AREA"]
    out["TIME_PERIOD"] = out["TIME_PERIOD"].astype(str)
    out["OBS_VALUE"] = pd.to_numeric(out["OBS_VALUE"], errors="coerce")
    out["Date"] = pd.to_datetime(out["TIME_PERIOD"], errors="coerce")
    out = out.dropna(subset=["Date"]).sort_values("Date")
    return out
def fetch_oecd_g20_cpi_rate_one(
    area: str,                # e.g. "CHN"
    start_period: str,        # "YYYY-MM"
    end_period: Optional[str],
    growth_code: str,         # try "GY", "GM", maybe fallback later
    timeout: int = 120,
) -> pd.Series:
    key = f"{area}.M.N.CPI.PA._T.N.{growth_code}"
    url = (
        "https://sdmx.oecd.org/public/rest/data/"
        "OECD.SDD.TPS,DSD_G20_PRICES@DF_G20_PRICES,/"
        f"{key}"
        f"?startPeriod={start_period}"
        + (f"&endPeriod={end_period}" if end_period else "")
        + "&dimensionAtObservation=AllDimensions"
        + "&format=csvfilewithlabels"
    )

    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    df = pd.read_csv(StringIO(r.text))

    tp_col = _find_col(df, ["TIME_PERIOD", "Time period", "TIME_PERIOD:Time period"])
    val_col = _find_col(df, ["OBS_VALUE", "Value", "OBS_VALUE:Value"])

    s = pd.to_numeric(df[val_col], errors="coerce")
    dt = pd.to_datetime(df[tp_col].astype(str), errors="coerce")
    out = pd.Series(s.values, index=dt).dropna().sort_index()
    out.index.name = "Date"
    return out


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


def update_monthly_rates(sh):
    """
    Monthly:
      - CPI YoY/MoM: OECD DF_G20_PRICES direct growth rates (GY/GM)
      - Unemployment (%): OECD LFS
      - Policy rate (%):
          EA, DE: ECB DFR daily -> month-end last
          Others: ECOS 902Y006 monthly (if BOK_API_KEY available)
      - Core CPI: not collected
    """
    tab = "data-monthly"
    ws = ensure_worksheet(sh, tab)

    ccy_list = list(OECD_LOC.keys())

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

    start_period = monthly_query_start(last_date, default_start="2000-01", lookback_months=3)
    end_period = pd.Timestamp.today().strftime("%Y-%m")
    print(f"📌 {tab}: startPeriod={start_period} (lookback), endPeriod={end_period}")

    combined = pd.DataFrame()

    # -------------------------
    # 1) CPI YoY/MoM from OECD (country-by-country, so one failure doesn't kill all)
    # -------------------------
    try:
        tmp = pd.DataFrame()

        for ccy in ccy_list:
            area = OECD_LOC[ccy]

            # YoY: GY (works as you tested)
            try:
                yoy = fetch_oecd_g20_cpi_rate_one(area, start_period, end_period, "GY")
                if not yoy.empty:
                    tmp = yoy.to_frame(name=f"{ccy}_CPI_YoY") if tmp.empty else tmp.join(
                        yoy.to_frame(name=f"{ccy}_CPI_YoY"), how="outer"
                    )
            except Exception as e:
                print(f"⚠️ {tab}: CPI YoY failed for {ccy}/{area}: {e}")

            # MoM: first try GM; if empty/fails, try alternative codes (best-effort)
            mom = pd.Series(dtype="float64")
            for code in ["GM", "GP", "GR"]:   # GM이 비어있으면 데이터플로우에 따라 GP/GR 중 하나가 “previous period change”일 수 있음
                try:
                    s = fetch_oecd_g20_cpi_rate_one(area, start_period, end_period, code)
                    if not s.empty:
                        mom = s
                        break
                except Exception:
                    continue

            if not mom.empty:
                tmp = mom.to_frame(name=f"{ccy}_CPI_MoM") if tmp.empty else tmp.join(
                    mom.to_frame(name=f"{ccy}_CPI_MoM"), how="outer"
                )
            else:
                print(f"⚠️ {tab}: CPI MoM empty for {ccy}/{area} (tried GM/GP/GR)")

            time.sleep(0.1)

        if not tmp.empty:
            combined = tmp if combined.empty else combined.join(tmp, how="outer")

        print(f"✅ {tab}: OECD CPI ok (cols={0 if tmp.empty else tmp.shape[1]})")

    except Exception as e:
        print(f"⚠️ {tab}: OECD CPI block failed: {e}")


    # -------------------------
    # 2) Unemployment from OECD LFS (level %)
    # -------------------------
    try:
        url_unemp = (
            "https://sdmx.oecd.org/public/rest/data/"
            "OECD.SDD.TPS,DSD_LFS@DF_IALFS_UNE_M"
            ",/..._Z.Y._T.Y_GE15..M?"
            f"startPeriod={start_period}&endPeriod={end_period}"
            "&dimensionAtObservation=AllDimensions"
            "&format=csvfilewithlabels"
        )

        dfu = oecd_get_csv(url_unemp, timeout=180)

        tp_col = _find_col(dfu, ["TIME_PERIOD", "Time period", "TIME_PERIOD:Time period"])
        val_col = _find_col(dfu, ["OBS_VALUE", "Value", "OBS_VALUE:Value"])
        area_col = _find_area_col(dfu)

        dfu[tp_col] = dfu[tp_col].astype(str)
        dfu[val_col] = pd.to_numeric(dfu[val_col], errors="coerce")
        dfu["Date"] = pd.to_datetime(dfu[tp_col], errors="coerce")
        dfu = dfu.dropna(subset=["Date"])

        keep_areas = set(OECD_LOC.values())
        dfu = dfu[dfu[area_col].isin(keep_areas)]

        piv = dfu.pivot_table(index="Date", columns=area_col, values=val_col, aggfunc="last").sort_index()

        tmp = pd.DataFrame(index=piv.index)
        for ccy in ccy_list:
            area = OECD_LOC[ccy]
            if area in piv.columns:
                tmp[f"{ccy}_Unemployment"] = piv[area]

        if not tmp.empty:
            combined = tmp if combined.empty else combined.join(tmp, how="outer")

        print(f"✅ {tab}: OECD Unemployment ok (rows={len(tmp)})")

    except Exception as e:
        print(f"⚠️ {tab}: OECD Unemployment failed: {e}")

    # -------------------------
    # 3) Policy rate
    # 3-1) EA & DE: ECB DFR daily -> month-end last
    # -------------------------
    try:
        # DFR daily expects YYYY-MM-DD
        d0 = f"{start_period}-01"
        dfr_daily = ecb_get_series_csv(ECB_DFR_FLOW, ECB_DFR_KEY, start_period=d0, end_period=None, timeout=60)

        if not dfr_daily.empty:
            dfr_m = dfr_daily.resample("M").last()
            tmp = pd.DataFrame(index=dfr_m.index)
            tmp["EA_PolicyRate"] = dfr_m
            tmp["DE_PolicyRate"] = dfr_m
            combined = tmp if combined.empty else combined.join(tmp, how="outer")

        print(f"✅ {tab}: ECB DFR ok (rows={len(dfr_daily)})")

    except Exception as e:
        print(f"⚠️ {tab}: ECB DFR failed: {e}")

    # 3-2) Others: ECOS 902Y006 monthly (if BOK_API_KEY)
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
                    print(f"⚠️ {tab}: ECOS policy EMPTY for {ccy} item={item1}")
                    continue
                col = f"{ccy}_PolicyRate"
                tmp = s.to_frame(name=col) if tmp.empty else tmp.join(s.to_frame(name=col), how="outer")
                time.sleep(0.1)

            if not tmp.empty:
                combined = tmp if combined.empty else combined.join(tmp, how="outer")

            print(f"✅ {tab}: ECOS policy ok (cols={0 if tmp.empty else tmp.shape[1]})")
        else:
            print(f"ℹ️ {tab}: BOK_API_KEY missing; skipping ECOS policy rates.")

    except Exception as e:
        print(f"⚠️ {tab}: ECOS PolicyRate failed: {e}")

    # -------------------------
    # Finalize / append
    # -------------------------
    if combined.empty:
        print(f"❌ {tab}: combined is empty (no sources succeeded).")
        return

    combined = combined.sort_index()

    # Append only rows strictly after last_date
    if last_date:
        dt0 = pd.to_datetime(last_date, errors="coerce") + pd.Timedelta(days=1)
        combined = combined[combined.index >= dt0]

    if combined.empty:
        print(f"ℹ️ {tab}: no new rows after filtering (already up to date).")
        return

    combined.index.name = "Date"
    out = combined.reset_index()
    out["Date"] = pd.to_datetime(out["Date"], errors="coerce").dt.strftime("%Y-%m-%d")

    # Ensure all columns exist
    for c in cols:
        if c not in out.columns:
            out[c] = pd.NA
    out = out[["Date"] + cols].fillna("")

    n = append_rows(ws, out.values.tolist())
    print(f"✅ {tab}: appended {n} rows")


def update_quarterly_rates(sh):
    """
    Quarterly:
      - Real GDP growth from OECD DF_QNA_EXPENDITURE_GROWTH_G20
      - Attempts to map YoY/QoQ via a dimension; otherwise fills QoQ only.
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

    start_period = quarterly_query_start(last_date, default_start="1990-Q1", lookback_quarters=2)
    print(f"📌 {tab}: startPeriod={start_period} (lookback)")

    combined = pd.DataFrame()

    try:
        # Your original URL kept (broad country coverage). We'll filter down to our areas after download.
        url = (
            "https://sdmx.oecd.org/public/rest/data/"
            "OECD.SDD.NAD,DSD_NAMAIN1@DF_QNA_EXPENDITURE_GROWTH_G20"
            "/Q..ARG+AUS+BRA+CAN+CHN+FRA+DEU+IND+IDN+ITA+JPN+KOR+MEX+RUS+SAU+ZAF+TUR+GBR+USA+OECD+G20+G7+USMCA+OECDE+EA20+EU27_2020..........?"
            f"startPeriod={start_period}"
            "&dimensionAtObservation=AllDimensions"
            "&format=csvfilewithlabels"
        )

        df = oecd_get_csv(url, timeout=180)

        tp_col = _find_col(df, ["TIME_PERIOD", "Time period", "TIME_PERIOD:Time period"])
        val_col = _find_col(df, ["OBS_VALUE", "Value", "OBS_VALUE:Value"])
        area_col = _find_area_col(df)

        df[tp_col] = df[tp_col].astype(str)
        df[val_col] = pd.to_numeric(df[val_col], errors="coerce")
        df["Date"] = df[tp_col].apply(_tp_to_timestamp)
        df = df.dropna(subset=["Date"])

        keep_areas = set(OECD_LOC.values())
        df = df[df[area_col].isin(keep_areas)]

        # Find a dimension that might indicate YoY vs QoQ
        rate_dim = None
        for cand in ["MEASURE", "Measure", "SUBJECT", "Subject", "INDICATOR", "Indicator"]:
            if cand in df.columns:
                rate_dim = cand
                break

        def _map_kind(k: str) -> Optional[str]:
            kl = str(k).lower()
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
                piv = dfg.pivot_table(index="Date", columns=area_col, values=val_col, aggfunc="last").sort_index()
                tmp = pd.DataFrame(index=piv.index)
                for ccy in ccy_list:
                    area = OECD_LOC[ccy]
                    if area in piv.columns:
                        tmp[f"{ccy}_RealGDP_{kind}"] = piv[area]
                if not tmp.empty:
                    combined = tmp if combined.empty else combined.join(tmp, how="outer")
        else:
            # No dimension found -> treat as QoQ by convention (best-effort)
            piv = df.pivot_table(index="Date", columns=area_col, values=val_col, aggfunc="last").sort_index()
            tmp = pd.DataFrame(index=piv.index)
            for ccy in ccy_list:
                area = OECD_LOC[ccy]
                if area in piv.columns:
                    tmp[f"{ccy}_RealGDP_QoQ"] = piv[area]
            combined = tmp

        print(f"✅ {tab}: OECD GDP growth ok (rows={0 if combined.empty else len(combined)})")

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
        print(f"ℹ️ {tab}: no new rows after filtering (already up to date).")
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
    update_monthly_rates(sh)
    update_quarterly_rates(sh)

    print("\n🎉 All updates completed.")


if __name__ == "__main__":
    main()
