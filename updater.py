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
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =========================
# ENV
# =========================
FRED_API_KEY = os.environ.get("FRED_API_KEY")
GSHEET_ID = os.environ.get("GSHEET_ID")
SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
BOK_API_KEY = os.environ.get("BOK_API_KEY")

if not BOK_API_KEY:
    raise ValueError("❌ BOK_API_KEY가 설정되지 않았습니다.")

# =========================
# CONFIG
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

CCY_LIST = ["US", "CA", "XM", "CH", "JP", "CN", "KR"]

COUNTRY_NAME_MAP = {
    "US": ["미국", "U.S.A", "United States", "US"],
    "CA": ["캐나다", "Canada", "CA"],
    "XM": ["유로", "Euro", "유로지역", "XM", "U4", "EZ"], 
    "DE": ["독일", "Germany", "DE"], 
    "CH": ["스위스", "Switzerland", "CH"],
    "JP": ["일본", "Japan", "JP"],
    "CN": ["중국", "China", "CN"],
    "KR": ["한국", "Korea", "KR"]
}

ECOS_POLICY = "902Y006"
ECOS_CPI    = "902Y008"
ECOS_UNEMP  = "902Y021"
ECOS_GROWTH = "902Y015"

# =========================
# Google Sheets Helpers
# =========================
def get_gspread_client(json_str: str):
    info = json.loads(json_str)
    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
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
    ws.clear()
    ws.update(range_name="A1", values=[headers], value_input_option="USER_ENTERED")

def append_rows(ws, rows: list[list]):
    if not rows: return 0
    ws.append_rows(rows, value_input_option="USER_ENTERED")
    return len(rows)

def pick_start_date(last_date_str: Optional[str], default_start: str) -> str:
    if not last_date_str: return default_start
    try:
        d = datetime.strptime(last_date_str, "%Y-%m-%d") + timedelta(days=1)
        return d.strftime("%Y-%m-%d")
    except ValueError: return default_start

# =========================
# NETWORK HELPER
# =========================
def create_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    })
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    s.mount('http://', HTTPAdapter(max_retries=retries))
    s.mount('https://', HTTPAdapter(max_retries=retries))
    return s

# =========================
# OFR LOADER (Weekly) - 🟢 [복구됨]
# =========================
def load_ofr_multifull(mnemonics: list[str], start_date: str) -> pd.DataFrame:
    session = create_session()
    url = "https://data.financialresearch.gov/v1/series/multifull"
    params = {"mnemonics": ",".join(mnemonics)}
    
    try:
        resp = session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        raw = resp.json()
    except Exception as e:
        print(f"⚠️ OFR Request Failed: {e}")
        return pd.DataFrame()

    frames = []
    for mnem, entry in raw.items():
        ts = entry.get("timeseries", {})
        agg = ts.get("aggregation")
        if not agg: continue
        
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
# ECOS AUTO-DISCOVERY
# =========================
def find_ecos_meta(api_key: str, table_keywords: list, item_targets: dict) -> tuple:
    session = create_session()
    
    # 1. 테이블 검색
    stat_code = None
    table_name = ""
    url_table = f"http://ecos.bok.or.kr/api/StatisticTableList/{api_key}/json/kr/1/1000/"
    try:
        r = session.get(url_table, timeout=10)
        js = r.json()
        rows = js.get("StatisticTableList", {}).get("row", [])
        
        candidates = []
        for row in rows:
            t_name = row.get("STAT_NAME", "")
            t_code = row.get("STAT_CODE", "")
            if all(k in t_name for k in table_keywords):
                candidates.append((t_code, t_name))
        
        candidates.sort(key=lambda x: (not x[0].startswith("902Y"), x[0]))
        
        if candidates:
            stat_code, table_name = candidates[0]
            print(f"   🔎 Found Table: {stat_code} - {table_name}")
        else:
            print(f"   ⚠️ Could not find table for {table_keywords}")
            return None, {}

    except Exception as e:
        print(f"   ⚠️ Table search failed: {e}")
        return None, {}

    # 2. 아이템(국가) 코드 검색
    item_map = {}
    url_item = f"http://ecos.bok.or.kr/api/StatisticItemList/{api_key}/json/kr/1/100/{stat_code}"
    try:
        r = session.get(url_item, timeout=10)
        js = r.json()
        rows = js.get("StatisticItemList", {}).get("row", [])
        
        for ccy, keywords in item_targets.items():
            found = False
            for row in rows:
                if row["ITEM_CODE"] == ccy:
                    item_map[ccy] = ccy
                    found = True
                    break
            if not found:
                for row in rows:
                    if any(k in row["ITEM_NAME"] for k in keywords):
                        item_map[ccy] = row["ITEM_CODE"]
                        found = True
                        break
            if not found:
                item_map[ccy] = ccy

    except Exception as e:
        print(f"   ⚠️ Item search failed: {e}")

    return stat_code, item_map

# =========================
# ECOS & DATA HELPERS
# =========================
def _tp_to_timestamp(tp: str) -> pd.Timestamp:
    tp = str(tp).strip()
    if len(tp) == 6 and tp.isdigit():
        return pd.Timestamp(int(tp[:4]), int(tp[4:6]), 1)
    if len(tp) == 8 and tp.isdigit():
        return pd.Timestamp(int(tp[:4]), int(tp[4:6]), int(tp[6:8]))
    if "Q" in tp:
        s = tp.replace("-", "")
        if len(s) == 6 and s[:4].isdigit() and s[4] == "Q" and s[5].isdigit():
            y = int(s[:4]); q = int(s[5])
            month = q * 3
            return pd.Timestamp(y, month, 1) + pd.offsets.MonthEnd(0)
    return pd.to_datetime(tp, errors="coerce")

def ecos_stat_search(
    api_key: str,
    stat_code: str,
    cycle: str,
    start: str,
    end: str,
    item_code1: str = "?",
    item_code2: str = "?",
    item_code3: str = "?",
    item_code4: str = "?",
    lang: str = "kr",
    timeout: int = 20,
) -> pd.Series:
    
    session = create_session()
    base_url = "http://ecos.bok.or.kr/api/StatisticSearch"
    
    def _call_once(start_arg, end_arg):
        url = (
            f"{base_url}/{api_key}/json/{lang}/1/10000/"
            f"{stat_code}/{cycle}/{start_arg}/{end_arg}/"
            f"{item_code1}/{item_code2}/{item_code3}/{item_code4}"
        )
        try:
            r = session.get(url, timeout=timeout)
            r.raise_for_status()
        except: return None, None, url

        try: js = r.json()
        except: return None, None, url

        if "StatisticSearch" not in js: return js, [], url
        rows = js.get("StatisticSearch", {}).get("row", [])
        return js, rows, url

    js, rows, url = _call_once(start, end)
    
    if not rows and cycle.upper() == "Q":
        def to_q(fmt_ym):
            try:
                dt = pd.to_datetime(fmt_ym, format="%Y%m")
                return f"{dt.year}Q{((dt.month-1)//3)+1}"
            except: return None
        sq, eq = to_q(start), to_q(end)
        if sq and eq:
            js2, rows2, url2 = _call_once(sq, eq)
            if rows2: rows = rows2

    if not rows: return pd.Series(dtype="float64")

    out = []
    for row in rows:
        t = row.get("TIME")
        v = row.get("DATA_VALUE")
        if not t or v is None: continue
        dt = _tp_to_timestamp(t)
        if pd.isna(dt): continue
        try:
            fv = float(v)
            out.append((dt, fv))
        except: continue

    if not out: return pd.Series(dtype="float64")

    s = pd.Series({d: v for d, v in out}).sort_index()
    s.index = pd.to_datetime(s.index)
    s.index.name = "Date"
    return s

def to_period_index(s: pd.Series, freq: str) -> pd.Series:
    if s is None or s.empty: return pd.Series(dtype="float64")
    idx = pd.to_datetime(s.index, errors="coerce")
    out = pd.Series(s.values, index=idx).dropna()
    if out.empty: return pd.Series(dtype="float64")

    if freq == "M":
        out.index = out.index.to_period("M").to_timestamp()
        out = out.groupby(out.index).last().sort_index()
    elif freq == "Q":
        out.index = out.index.to_period("Q").to_timestamp("Q") 
        out = out.groupby(out.index).last().sort_index()
    else:
        out = out.sort_index()
    out.index.name = "Date"
    return out

def build_cpi_yoy_from_index(cpi_index: pd.Series) -> pd.Series:
    if cpi_index is None or cpi_index.empty: return pd.Series(dtype="float64")
    cpi_index = cpi_index.sort_index()
    yoy = cpi_index.pct_change(12) * 100.0
    yoy.name = "CPI_YoY"
    return yoy

# =========================
# Update routines
# =========================
def update_daily(fred, sh):
    TAB_NAME = "data-daily"
    ws = ensure_worksheet(sh, TAB_NAME)
    headers = ["Date"] + list(DAILY_FRED_SERIES.values())
    header, _ = get_header_and_last_date(ws)
    if header != headers: write_header(ws, headers)
        
    pull_start = (datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m-%d")
    print(f"📌 {TAB_NAME}: pulling from {pull_start}")
    
    df_pulled = pd.DataFrame()
    for sid, col in DAILY_FRED_SERIES.items():
        try:
            s = fred.get_series(sid, observation_start=pull_start)
            if s is None or s.empty: continue
            s = s.sort_index()
            s.index = pd.to_datetime(s.index)
            df_pulled = s.to_frame(name=col) if df_pulled.empty else df_pulled.join(s.to_frame(name=col), how="outer")
            time.sleep(0.1)
        except: pass

    if df_pulled.empty:
        print(f"   ℹ️ No new data found.")
        return
    print(f"✅ {TAB_NAME}: Fetched {len(df_pulled)} rows.")

# 🟢 [복구됨] Weekly OFR Update
def update_weekly_ofr(sh):
    tab = "data-weekly"
    ws = ensure_worksheet(sh, tab)
    headers = ["Date"] + list(WEEKLY_OFR_MNEMONICS.values())
    
    header, last_date = get_header_and_last_date(ws)
    if header != headers: write_header(ws, headers)
    
    start_date = pick_start_date(last_date, "2012-01-01")
    print(f"📌 {tab}: Pulling OFR from {start_date}...")

    df = load_ofr_multifull(list(WEEKLY_OFR_MNEMONICS.keys()), start_date)
    if df.empty:
        print(f"   ℹ️ No new data.")
        return
        
    df = df.rename(columns=WEEKLY_OFR_MNEMONICS)
    df = df.reset_index()
    df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")
    df = df[["Date"] + list(WEEKLY_OFR_MNEMONICS.values())].fillna("")
    
    n = append_rows(ws, df.values.tolist())
    print(f"✅ {tab}: Appended {n} rows.")

# =========================
# ECOS MONTHLY (Full Reload)
# =========================
def update_monthly_bok_only(sh):
    tab = "data-monthly"
    ws = ensure_worksheet(sh, tab)
    cols = []
    for ccy in CCY_LIST:
        cols += [f"{ccy}_CPI_YoY", f"{ccy}_Unemployment", f"{ccy}_PolicyRate"]
    headers = ["Date"] + cols

    # Full Reload: 2000년 부터
    start_dt = pd.Timestamp("2000-01-01")
    start_ym = start_dt.strftime("%Y%m")
    end_ym = pd.Timestamp.today().strftime("%Y%m")
    
    print(f"📌 {tab}: FULL RELOAD window {start_ym} ~ {end_ym}")

    print("   🔎 Auto-discovering codes...")
    found_unemp_code, unemp_item_map = find_ecos_meta(BOK_API_KEY, ["주요국", "실업률"], COUNTRY_NAME_MAP)
    unemp_code = found_unemp_code if found_unemp_code else ECOS_UNEMP

    _, cpi_item_map = find_ecos_meta(BOK_API_KEY, ["주요국", "소비자물가"], COUNTRY_NAME_MAP)
    _, policy_item_map = find_ecos_meta(BOK_API_KEY, ["주요국", "정책금리"], COUNTRY_NAME_MAP)

    combined = pd.DataFrame()

    for ccy in CCY_LIST:
        target_ccy_cpi = "DE" if ccy == "XM" else ccy
        target_ccy_unemp = "DE" if ccy == "XM" else ccy
        target_ccy_policy = ccy # 금리는 유지
        
        cpi_item = cpi_item_map.get(target_ccy_cpi, target_ccy_cpi)
        unemp_item = unemp_item_map.get(target_ccy_unemp, target_ccy_unemp)
        policy_item = policy_item_map.get(target_ccy_policy, target_ccy_policy)

        try:
            # 1. CPI
            cpi_ix = ecos_stat_search(BOK_API_KEY, ECOS_CPI, "M", start_ym, end_ym, item_code1=cpi_item)
            cpi_ix = to_period_index(cpi_ix, "M")
            cpi_yoy = build_cpi_yoy_from_index(cpi_ix)

            # 2. Unemployment (CH 제외)
            if ccy == "CH":
                un = pd.Series(dtype="float64")
            else:
                un = ecos_stat_search(BOK_API_KEY, unemp_code, "M", start_ym, end_ym, item_code1=unemp_item)
                un = to_period_index(un, "M")

            # 3. Policy Rate
            pr = ecos_stat_search(BOK_API_KEY, ECOS_POLICY, "M", start_ym, end_ym, item_code1=policy_item)
            pr = to_period_index(pr, "M")

            # JP 금리 보정
            if ccy == "JP" and not pr.empty:
                pr = pr.asfreq("MS").ffill().fillna(0)

            tmp = pd.DataFrame(index=cpi_yoy.index.union(un.index).union(pr.index).sort_values())
            if not cpi_yoy.empty: tmp[f"{ccy}_CPI_YoY"] = cpi_yoy
            if not un.empty: tmp[f"{ccy}_Unemployment"] = un
            if not pr.empty: tmp[f"{ccy}_PolicyRate"] = pr

            if not tmp.empty:
                combined = tmp if combined.empty else combined.join(tmp, how="outer")
            time.sleep(0.1)

        except Exception as e:
            print(f"   ⚠️ Error {ccy}: {e}")

    if combined.empty:
        print(f"❌ {tab}: No valid data.")
        return

    combined.index = pd.to_datetime(combined.index, errors="coerce")
    combined = combined.groupby(combined.index.to_period("M").to_timestamp()).last().sort_index()
    combined = combined[combined.index >= start_dt]
    combined.index.name = "Date"

    for c in cols:
        if c not in combined.columns: combined[c] = pd.NA
    combined = combined[cols]

    out = combined.reset_index()
    out["Date"] = pd.to_datetime(out["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    out = out.fillna("")

    ws.clear()
    ws.update(range_name="A1", values=[headers] + out.values.tolist(), value_input_option="USER_ENTERED")
    print(f"✅ {tab}: Full Reloaded {len(out)} rows.")

# =========================
# ECOS QUARTERLY (Full Reload)
# =========================
def update_quarterly_bok_only(sh):
    tab = "data-quarterly"
    ws = ensure_worksheet(sh, tab)
    cols = [f"{ccy}_Growth" for ccy in CCY_LIST]
    headers = ["Date"] + cols

    # Full Reload: 1990년 부터
    start_dt = pd.Timestamp("1990-01-01")
    
    def to_q_str(dt): return f"{dt.year}Q{((dt.month-1)//3)+1}"
    start_q = to_q_str(start_dt)
    end_q = to_q_str(pd.Timestamp.today())
    print(f"📌 {tab}: FULL RELOAD window {start_q} ~ {end_q}")

    print("   🔎 Auto-discovering Growth codes...")
    found_growth_code, growth_item_map = find_ecos_meta(BOK_API_KEY, ["주요국", "경제성장률"], COUNTRY_NAME_MAP)
    growth_code = found_growth_code if found_growth_code else ECOS_GROWTH

    combined = pd.DataFrame()

    for ccy in CCY_LIST:
        target_ccy = "DE" if ccy == "XM" else ccy
        ecos_item = growth_item_map.get(target_ccy, target_ccy)
        
        try:
            s = ecos_stat_search(BOK_API_KEY, growth_code, "Q", start_q, end_q, item_code1=ecos_item)
            s = to_period_index(s, "Q")
            if not s.empty:
                tmp = s.to_frame(name=f"{ccy}_Growth")
                combined = tmp if combined.empty else combined.join(tmp, how="outer")
            time.sleep(0.1)
        except Exception as e:
            print(f"   ⚠️ Error {ccy}: {e}")

    if combined.empty:
        print(f"❌ {tab}: No valid data.")
        return

    combined = combined.sort_index()
    combined = combined[combined.index >= start_dt]
    combined.index.name = "Date"

    for c in cols:
        if c not in combined.columns: combined[c] = pd.NA
    combined = combined[cols]

    out = combined.reset_index()
    out["Date"] = pd.to_datetime(out["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    out = out.fillna("")

    ws.clear()
    ws.update(range_name="A1", values=[headers] + out.values.tolist(), value_input_option="USER_ENTERED")
    print(f"✅ {tab}: Full Reloaded {len(out)} rows.")

def main():
    try:
        fred = Fred(api_key=FRED_API_KEY)
        gc = get_gspread_client(SERVICE_ACCOUNT_JSON)
        sh = gc.open_by_key(GSHEET_ID)
    except Exception as e:
        print(f"❌ Init Failed: {e}")
        return

    # 1. Daily
    try:
        update_daily(fred, sh)
    except Exception as e:
        print(f"❌ Daily Update Failed: {e}")

    # 2. Weekly (복구됨)
    try:
        update_weekly_ofr(sh)
    except Exception as e:
        print(f"❌ Weekly Update Failed: {e}")

    # 3. Monthly (Full Reload)
    update_monthly_bok_only(sh)
    
    # 4. Quarterly (Full Reload)
    update_quarterly_bok_only(sh)
    
    print("\n🎉 All updates completed.")

if __name__ == "__main__":
    main()
