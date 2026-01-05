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
# CONFIG: IMF (IFS) codes
# =========================
IMF_DATASET = "IFS"  # using IMF SDMX_JSON CompactData/IFS  :contentReference[oaicite:1]{index=1}

IMF_MONTHLY = {
    "US": {
        "iso": "US",
        "imf": "111",
        "cpi_headline": "M.111.PCPI_IX",
        "cpi_core": "M.111.PCPIFE_IX",
        "unemployment": "M.111.LUR_PT",
        "policy_rate": "M.111.FPOLM_PA",
    },
    "CA": {
        "iso": "CA",
        "imf": "156",
        "cpi_headline": "M.156.PCPI_IX",
        "cpi_core": "M.156.PCPIFE_IX",
        "unemployment": "M.156.LUR_PT",
        "policy_rate": "M.156.FPOLM_PA",
    },
    "DE": {
        "iso": "DE",
        "imf": "134",
        "cpi_headline": "M.134.PCPI_IX",
        "cpi_core": "M.134.PCPIFE_IX",
        "unemployment": "M.134.LUR_PT",
        "policy_rate": "M.134.FPOLM_PA",  # note (1) in your table
    },
    "EA": {
        "iso": "EA",
        "imf": "U2",
        "cpi_headline": "M.U2.PCPI_IX",
        "cpi_core": "M.U2.PCPIFE_IX",
        "unemployment": "M.U2.LUR_PT",
        "policy_rate": "M.U2.FPOLM_PA",
    },
    "CH": {
        "iso": "CH",
        "imf": "146",
        "cpi_headline": "M.146.PCPI_IX",
        "cpi_core": "M.146.PCPIFE_IX",
        "unemployment": "M.146.LUR_PT",
        "policy_rate": "M.146.FPOLM_PA",
    },
    "JP": {
        "iso": "JP",
        "imf": "158",
        "cpi_headline": "M.158.PCPI_IX",
        "cpi_core": "M.158.PCPIFE_IX",
        "unemployment": "M.158.LUR_PT",
        "policy_rate": "M.158.FPOLM_PA",
    },
    "CN": {
        "iso": "CN",
        "imf": "924",
        "cpi_headline": "M.924.PCPI_IX",
        "cpi_core": "M.924.PCPIFE_IX",
        "unemployment": "M.924.LUR_PT",  # note (2) in your table
        "policy_rate": "M.924.FPOLM_PA",
    },
    "HK": {
        "iso": "HK",
        "imf": "532",
        "cpi_headline": "M.532.PCPI_IX",
        "cpi_core": "M.532.PCPIFE_IX",
        "unemployment": "M.532.LUR_PT",
        "policy_rate": "M.532.FPOLM_PA",
    },
    "KR": {
        "iso": "KR",
        "imf": "542",
        "cpi_headline": "M.542.PCPI_IX",
        "cpi_core": "M.542.PCPIFE_IX",
        "unemployment": "M.542.LUR_PT",
        "policy_rate": "M.542.FPOLM_PA",
    },
}

IMF_QUARTERLY = {
    "US": {"iso": "US", "imf": "111", "real_gdp_index": "Q.111.NGDP_R_SA_IX"},
    "CA": {"iso": "CA", "imf": "156", "real_gdp_index": "Q.156.NGDP_R_SA_IX"},
    "DE": {"iso": "DE", "imf": "134", "real_gdp_index": "Q.134.NGDP_R_SA_IX"},
    "EA": {"iso": "EA", "imf": "U2",  "real_gdp_index": "Q.U2.NGDP_R_SA_IX"},
    "CH": {"iso": "CH", "imf": "146", "real_gdp_index": "Q.146.NGDP_R_SA_IX"},
    "JP": {"iso": "JP", "imf": "158", "real_gdp_index": "Q.158.NGDP_R_SA_IX"},
    "CN": {"iso": "CN", "imf": "924", "real_gdp_index": "Q.924.NGDP_R_SA_IX"},
    "HK": {"iso": "HK", "imf": "532", "real_gdp_index": "Q.532.NGDP_R_SA_IX"},
    "KR": {"iso": "KR", "imf": "542", "real_gdp_index": "Q.542.NGDP_R_SA_IX"},
}

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

from typing import Optional

def pick_start_date(last_date_str: Optional[str], default_start: str) -> str:
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
# IMF (IFS) CompactData loader
# =========================
def _parse_imf_time_period(tp: str) -> pd.Timestamp:
    """
    IMF SDMX TIME_PERIOD examples (common in CompactData):
      - Monthly: '2025-11' or '2025-11-01'
      - Quarterly: '2025-Q3'
      - Annual: '2025'
    We normalize to a Timestamp for indexing.
    """
    tp = str(tp).strip()

    # Quarterly: YYYY-Qn
    if "Q" in tp and "-" in tp:
        y, q = tp.split("-Q")
        y = int(y)
        q = int(q)
        # Use quarter end as date index
        month = q * 3
        return pd.Timestamp(y, month, 1) + pd.offsets.MonthEnd(0)

    # Monthly: YYYY-MM
    if len(tp) == 7 and tp[4] == "-":
        return pd.Timestamp(tp + "-01")

    # Full date
    if len(tp) == 10 and tp[4] == "-" and tp[7] == "-":
        return pd.Timestamp(tp)

    # Annual: YYYY
    if len(tp) == 4 and tp.isdigit():
        return pd.Timestamp(int(tp), 12, 31)

    # Fallback
    return pd.to_datetime(tp, errors="coerce")


def imf_compact_series(
    series_key: str,
    dataset: str = IMF_DATASET,
    start_period: str = "1990",
    end_period: Optional[str] = None,
    timeout: int = 30,
) -> pd.Series:
    """
    Fetch one IMF CompactData series (SDMX-JSON) and return pd.Series indexed by Timestamp.

    Endpoint pattern:
      https://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/{dataset}/{series_key}?startPeriod=...&endPeriod=...
    """
    base = f"https://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/{dataset}/{series_key}"
    params = {"startPeriod": start_period}
    if end_period:
        params["endPeriod"] = end_period

    resp = requests.get(base, params=params, timeout=timeout)
    resp.raise_for_status()
    js = resp.json()

    c = js.get("CompactData", {})
    ds = c.get("DataSet", {})
    series = ds.get("Series", None)

    if series is None:
        return pd.Series(dtype="float64")

    if isinstance(series, list):
        series = series[0] if series else None
        if series is None:
            return pd.Series(dtype="float64")

    obs = series.get("Obs", None)
    if obs is None:
        return pd.Series(dtype="float64")

    if isinstance(obs, dict):
        obs = [obs]

    rows = []
    for o in obs:
        tp = o.get("@TIME_PERIOD")
        val = o.get("@OBS_VALUE")
        if tp is None or val is None:
            continue
        dt = _parse_imf_time_period(tp)
        if pd.isna(dt):
            continue
        try:
            v = float(val)
        except Exception:
            continue
        rows.append((dt, v))

    if not rows:
        return pd.Series(dtype="float64")

    s = pd.Series({d: v for d, v in rows}).sort_index()
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
    - Rewrite the whole sheet (safe for a few thousand rows)

    FULL_BACKFILL 모드:
    - 딱 1회 전체 히스토리(예: 2006-01-01~)를 다시 받아서 시트를 완전히 재구성
    - 이후 반드시 FULL_BACKFILL=False로 되돌려 운영 모드로 전환
    """

    # =========================
    # (1) MODE / CONSTANTS
    # =========================
    FULL_BACKFILL = False    # 🔥 딱 한 번만 True → 이후 False로 변경!
    FULL_START_DATE = "2006-01-01"

    TAB_NAME = "data-daily"
    LOOKBACK_DAYS = 30      # 누락/휴일/지연 감안 (운영 모드에서만 사용)

    ws = ensure_worksheet(sh, TAB_NAME)

    # =========================
    # (2) HEADERS / SHEET SHAPE
    # =========================
    headers = ["Date"] + list(DAILY_FRED_SERIES.values())
    header, _last_date_str = get_header_and_last_date(ws)

    # 헤더가 다르면 초기화 후 헤더 재작성
    if header != headers:
        ws.clear()
        ws.append_row(headers, value_input_option="USER_ENTERED")

    # =========================
    # (3) READ EXISTING -> df_existing
    # =========================
    records = ws.get_all_records()  # header row 기준 dict list
    if records:
        df_existing = pd.DataFrame(records)

        # 방어: Date 컬럼이 없으면 "데이터가 사실상 없음"으로 처리
        if "Date" not in df_existing.columns:
            df_existing = pd.DataFrame(columns=headers[1:])
            df_existing.index.name = "Date"
        else:
            df_existing["Date"] = pd.to_datetime(df_existing["Date"], errors="coerce")
            df_existing = df_existing.dropna(subset=["Date"]).set_index("Date").sort_index()
    else:
        df_existing = pd.DataFrame(columns=headers[1:])
        df_existing.index.name = "Date"

    # =========================
    # (4) FULL_BACKFILL이면 기존 시트 데이터 무시 (정확한 위치 1)
    # =========================
    if FULL_BACKFILL:
        print("🚨 FULL BACKFILL MODE: ignoring existing sheet data")
        df_existing = pd.DataFrame(columns=headers[1:])
        df_existing.index.name = "Date"

    # =========================
    # (5) PULL START 결정 (정확한 위치 2)
    # =========================
    if FULL_BACKFILL:
        pull_start = FULL_START_DATE
        print(f"🚨 FULL BACKFILL MODE: pulling full history from {pull_start}")
    else:
        pull_start = (datetime.utcnow() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")
        print(f"📌 {TAB_NAME}: pulling from {pull_start} (UTC)")

    # =========================
    # (6) PULL FROM FRED -> df_pulled
    # =========================
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

            time.sleep(0.15)  # FRED 요청 과다 방지
        except Exception as e:
            print(f"⚠️ DAILY load failed: {sid} ({e})")

    if df_pulled.empty:
        print(f"ℹ️ {TAB_NAME}: no data pulled from FRED")
        return

    # reset_index() 시 Date 컬럼 생성 보장
    df_pulled.index.name = "Date"

    # =========================
    # (7) MERGE (existing 우선, 결측만 pulled로 채움)
    # =========================
    df_existing_clean = df_existing.copy()
    for c in df_existing_clean.columns:
        df_existing_clean[c] = df_existing_clean[c].replace("", pd.NA)

    df_merged = df_existing_clean.combine_first(df_pulled)
    df_merged.index.name = "Date"

    # =========================
    # (8) COLUMN ENSURE + ORDER
    # =========================
    for c in headers[1:]:
        if c not in df_merged.columns:
            df_merged[c] = pd.NA

    df_merged = df_merged[headers[1:]]

    # =========================
    # (9) REWRITE WHOLE SHEET
    # =========================
    df_out = df_merged.reset_index()

    # reset_index 결과가 'index'로 나오면 Date로 rename (안전장치)
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
    IMF(IFS) monthly:
      - Headline CPI index -> YoY/MoM (%)
      - Core CPI index -> YoY/MoM (%)
      - Unemployment rate -> level (%) 그대로
      - Policy rate -> level (%) 그대로

    Writes to: data-monthly
    """
    tab = "data-monthly"
    ws = ensure_worksheet(sh, tab)

    # 1) build headers
    cols = []
    for k in IMF_MONTHLY.keys():
        cols += [
            f"{k}_CPI_YoY", f"{k}_CPI_MoM",
            f"{k}_CoreCPI_YoY", f"{k}_CoreCPI_MoM",
            f"{k}_Unemployment",
            f"{k}_PolicyRate",
        ]
    target_headers = ["Date"] + cols

    header, last_date = get_header_and_last_date(ws)
    if header != target_headers:
        ws.clear()
        write_header(ws, target_headers)

    # incremental append start_date
    start_date = pick_start_date(last_date, default_start="2000-01-01")
    print(f"📌 {tab}: start_date={start_date}")

    # 2) pull & transform (need long history for YoY/MoM)
    combined = pd.DataFrame()

    for ccy, spec in IMF_MONTHLY.items():
        try:
            # CPI Headline
            cpi = imf_compact_series(spec["cpi_headline"], start_period="1990")
            if not cpi.empty:
                cpi_rates = build_monthly_rates(cpi, f"{ccy}_CPI")  # => {ccy}_CPI_YoY/MoM
                combined = cpi_rates if combined.empty else combined.join(cpi_rates, how="outer")

            # CPI Core
            core = imf_compact_series(spec["cpi_core"], start_period="1990")
            if not core.empty:
                core_rates = build_monthly_rates(core, f"{ccy}_CoreCPI")
                combined = core_rates if combined.empty else combined.join(core_rates, how="outer")

            # Unemployment (already %)
            unrate = imf_compact_series(spec["unemployment"], start_period="1990")
            if not unrate.empty:
                tmp = unrate.to_frame(name=f"{ccy}_Unemployment")
                combined = tmp if combined.empty else combined.join(tmp, how="outer")

            # Policy rate (already %)
            pr = imf_compact_series(spec["policy_rate"], start_period="1990")
            if not pr.empty:
                tmp = pr.to_frame(name=f"{ccy}_PolicyRate")
                combined = tmp if combined.empty else combined.join(tmp, how="outer")

            time.sleep(0.2)  # IMF API 예의상 간격
        except Exception as e:
            print(f"⚠️ {tab}: IMF load failed for {ccy}: {e}")

    if combined.empty:
        print(f"ℹ️ {tab}: nothing to write")
        return

    # 3) filter rows to append
    combined = combined.sort_index()
    combined = combined[combined.index >= pd.to_datetime(start_date)]
    if combined.empty:
        print(f"ℹ️ {tab}: no new rows after filtering")
        return

    # 4) finalize dataframe for append
    combined.index.name = "Date"
    out = combined.reset_index()

    # monthly dates => YYYY-MM-DD (keep first day)
    out["Date"] = pd.to_datetime(out["Date"], errors="coerce").dt.strftime("%Y-%m-%d")

    # ensure all columns exist, order
    for c in cols:
        if c not in out.columns:
            out[c] = pd.NA
    out = out[["Date"] + cols].fillna("")

    n = append_rows(ws, out.values.tolist())
    print(f"✅ {tab}: appended {n} rows")

def update_quarterly_rates(fred: Fred, sh):
    """
    IMF(IFS) quarterly:
      - Real GDP index (seasonally adjusted index) -> YoY/QoQ (%)

    Writes to: data-quarterly
    """
    tab = "data-quarterly"
    ws = ensure_worksheet(sh, tab)

    cols = []
    for ccy in IMF_QUARTERLY.keys():
        cols += [f"{ccy}_RealGDP_YoY", f"{ccy}_RealGDP_QoQ"]
    target_headers = ["Date"] + cols

    header, last_date = get_header_and_last_date(ws)
    if header != target_headers:
        ws.clear()
        write_header(ws, target_headers)

    start_date = pick_start_date(last_date, default_start="1990-01-01")
    print(f"📌 {tab}: start_date={start_date}")

    combined = pd.DataFrame()

    for ccy, spec in IMF_QUARTERLY.items():
        try:
            gdp_ix = imf_compact_series(spec["real_gdp_index"], start_period="1970")
            if gdp_ix.empty:
                continue

            rates = build_quarterly_rates(gdp_ix, f"{ccy}_RealGDP")
            # build_quarterly_rates => {prefix}_YoY, {prefix}_QoQ
            # 여기서 prefix가 {ccy}_RealGDP 이므로 원하는 컬럼명과 일치
            combined = rates if combined.empty else combined.join(rates, how="outer")

            time.sleep(0.2)
        except Exception as e:
            print(f"⚠️ {tab}: IMF load failed for {ccy}: {e}")

    if combined.empty:
        print(f"ℹ️ {tab}: nothing to write")
        return

    combined = combined.sort_index()
    combined = combined[combined.index >= pd.to_datetime(start_date)]
    if combined.empty:
        print(f"ℹ️ {tab}: no new rows after filtering")
        return

    combined.index.name = "Date"
    out = combined.reset_index()

    # quarterly index in _parse_imf_time_period() -> quarter end date already
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

    # Run updates
    update_daily(fred, sh)
    update_weekly_ofr(sh)
    update_monthly_rates(fred, sh)
    update_quarterly_rates(fred, sh)

    print("\n🎉 All updates completed.")


if __name__ == "__main__":
    main()
