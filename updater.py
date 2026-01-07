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
# ECOS helpers (FIXED)
# =========================
def ecos_stat_search(
    api_key: str,
    stat_code: str,
    cycle: str,        # "M" or "Q"
    start: str,        # "YYYYMM" or "YYYYQn"
    end: str,
    item_code1: str = "?",
    item_code2: str = "?",
    item_code3: str = "?",
    item_code4: str = "?",
    lang: str = "kr",
    timeout: int = 30,
) -> pd.Series:
    """
    [FIXED] URL Structure:
    Correct: /api/StatisticSearch/{KEY}/{TYPE}/{LANG}/{START}/{END}/{STAT_CODE}/...
    Was:     /api/{KEY}/.../StatisticSearch/... (Wrong)
    """
    def _call_once(start_arg, end_arg):
        # 🟢 [수정됨] StatisticSearch 위치 변경 (API_KEY 앞)
        url = (
            f"https://ecos.bok.or.kr/api/StatisticSearch/{api_key}/json/{lang}/1/100000/"
            f"{stat_code}/{cycle}/{start_arg}/{end_arg}/"
            f"{item_code1}/{item_code2}/{item_code3}/{item_code4}"
        )
        try:
            r = requests.get(url, timeout=timeout)
            r.raise_for_status()
        except Exception as e:
            print(f"⚠️ ECOS request failed for {stat_code} ({item_code1}): {e}")
            return None, None, url

        try:
            js = r.json()
        except Exception as e:
            print(f"⚠️ ECOS JSON decode failed. URL: {url} Error: {e}")
            return None, None, url
        
        # ECOS API 에러 코드 확인 (데이터 없음 등)
        if "RESULT" in js:
            code = js["RESULT"].get("CODE")
            msg = js["RESULT"].get("MESSAGE")
            if code != "INFO-000":
                # 데이터가 없는 경우(INFO-200)는 에러가 아니므로 조용히 처리
                if code != "INFO-200":
                    print(f"ℹ️ ECOS API Message: {code} - {msg} | URL: {url}")
                return js, [], url

        rows = js.get("StatisticSearch", {}).get("row", [])
        return js, rows, url

    js, rows, url = _call_once(start, end)

    # ... (Q 포맷 재시도 로직은 기존 코드 유지) ...
    if not rows and cycle.upper() == "Q":
        # 쿼터 포맷 변환 로직 (기존 코드 활용)
        def to_q(fmt_ym):
            try:
                dt = pd.to_datetime(fmt_ym, format="%Y%m")
                q = ((dt.month - 1) // 3) + 1
                return f"{dt.year}Q{q}"
            except: return None
        
        sq, eq = to_q(start), to_q(end)
        if sq and eq:
            js2, rows2, url2 = _call_once(sq, eq)
            if rows2:
                rows = rows2

    if not rows:
        return pd.Series(dtype="float64")

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

    if not out:
        return pd.Series(dtype="float64")

    s = pd.Series({d: v for d, v in out}).sort_index()
    s.index = pd.to_datetime(s.index)
    s.index.name = "Date"
    return s
# =========================
# Update routines
# =========================
def update_daily(fred, sh):
    FULL_BACKFILL = False
    FULL_START_DATE = "2006-01-01"
    TAB_NAME = "data-daily"
    LOOKBACK_DAYS = 30

    print("DEBUG sheet title:", sh.title)
    print("DEBUG worksheets:", [w.title for w in sh.worksheets()][:50])

    ws = ensure_worksheet(sh, "data-daily")
    values = ws.get_all_values()
    print("DEBUG first row:", values[0] if values else None)
    print("DEBUG last row:", values[-1] if values else None)


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


    
    # =========================
    # (9) BUILD OUTPUT (Date 강제 생성 – reset_index 사용 안 함)
    # =========================
    df_out = df_merged.copy()

    # 🔥 Date 컬럼을 index에서 직접 생성 (pandas index 메타데이터 의존 제거)
    df_out["Date"] = df_out.index

    # 컬럼 순서 보장: Date + 나머지 헤더
    df_out = df_out[["Date"] + headers[1:]]

    # Date 포맷 통일
    df_out["Date"] = pd.to_datetime(df_out["Date"], errors="coerce").dt.strftime("%Y-%m-%d")

    # NaN → 빈 문자열
    df_out = df_out.fillna("")

    # =========================
    # (10) REWRITE SHEET
    # =========================
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
    tab = "data-monthly"
    ws = ensure_worksheet(sh, tab)

    # 헤더 정의
    cols = []
    for ccy in CCY_LIST:
        cols += [f"{ccy}_CPI_YoY", f"{ccy}_Unemployment", f"{ccy}_PolicyRate"]
    headers = ["Date"] + cols

    # 날짜 계산
    header, last_date = get_header_and_last_date(ws)
    if last_date:
        d_last = pd.to_datetime(last_date, errors="coerce")
        start_dt = (d_last - pd.DateOffset(months=15)) if pd.notna(d_last) else pd.Timestamp("2000-01-01")
    else:
        start_dt = pd.Timestamp("2000-01-01")
    
    start_dt = start_dt.to_period("M").to_timestamp("MS")
    start_ym = start_dt.strftime("%Y%m")
    end_ym = pd.Timestamp.today().strftime("%Y%m")
    
    print(f"📌 {tab}: ECOS window {start_ym} ~ {end_ym}")

    combined = pd.DataFrame()

    # 🚨 국가 코드 주의사항: 
    # ECOS 902Y(주요국) 시리즈에서 'XM'(유로지역)은 보통 'EZ' 또는 'U4' 등을 사용합니다.
    # 'KR'(한국)은 902Y에 없을 수 있으며 별도 테이블(물가: 021Y, 금리: 060Y)을 써야 할 수도 있습니다.
    # 아래는 시도해볼 만한 매핑입니다. 데이터가 안 나오면 ECOS 웹사이트에서 코드를 확인해야 합니다.
    country_map = {"XM": "EZ", "KR": "KR"} 

    for ccy in CCY_LIST:
        ecos_ccy = country_map.get(ccy, ccy) # 매핑 없으면 그대로 사용
        
        try:
            # 1. CPI (902Y008) -> YoY 계산
            cpi_ix = ecos_stat_search(BOK_API_KEY, ECOS_CPI, "M", start_ym, end_ym, item_code1=ecos_ccy)
            cpi_ix = to_period_index(cpi_ix, "M")
            cpi_yoy = build_cpi_yoy_from_index(cpi_ix)

            # 2. 실업률 (902Y021)
            un = ecos_stat_search(BOK_API_KEY, ECOS_UNEMP, "M", start_ym, end_ym, item_code1=ecos_ccy)
            un = to_period_index(un, "M")

            # 3. 정책금리 (902Y006)
            pr = ecos_stat_search(BOK_API_KEY, ECOS_POLICY, "M", start_ym, end_ym, item_code1=ecos_ccy)
            pr = to_period_index(pr, "M")

            # 병합
            tmp = pd.DataFrame(index=cpi_yoy.index.union(un.index).union(pr.index).sort_values())
            if not cpi_yoy.empty: tmp[f"{ccy}_CPI_YoY"] = cpi_yoy
            if not un.empty: tmp[f"{ccy}_Unemployment"] = un
            if not pr.empty: tmp[f"{ccy}_PolicyRate"] = pr

            combined = tmp if combined.empty else combined.join(tmp, how="outer")
            time.sleep(0.1)

        except Exception as e:
            print(f"⚠️ {tab}: Failed for {ccy}: {e}")

    # 🛡️ 데이터가 비어있으면 시트를 건드리지 않고 종료
    if combined.empty:
        print(f"❌ {tab}: No data fetched. Check API URL or Item Codes. Sheet NOT updated.")
        return

    # 데이터 정리
    combined.index = pd.to_datetime(combined.index, errors="coerce")
    combined = combined.groupby(combined.index.to_period("M").to_timestamp("MS")).last().sort_index()
    combined = combined[combined.index >= start_dt]
    
    # 누락된 컬럼 채우기
    for c in cols:
        if c not in combined.columns: combined[c] = pd.NA
    combined = combined[cols]

    # 최종 쓰기
    out = combined.reset_index()
    out["Date"] = pd.to_datetime(out["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    out = out.fillna("")

    ws.clear()
    ws.update([headers] + out.values.tolist(), value_input_option="USER_ENTERED")
    print(f"✅ {tab}: updated {len(out)} rows.")

def update_quarterly_bok_only(sh):
    tab = "data-quarterly"
    ws = ensure_worksheet(sh, tab)

    cols = [f"{ccy}_Growth" for ccy in CCY_LIST]
    headers = ["Date"] + cols

    # 날짜 설정
    header, last_date = get_header_and_last_date(ws)
    if last_date:
        d_last = pd.to_datetime(last_date, errors="coerce")
        start_dt = (d_last - pd.DateOffset(months=9)) if pd.notna(d_last) else pd.Timestamp("1990-01-01")
    else:
        start_dt = pd.Timestamp("1990-01-01")

    # 분기 포맷 변환 (YYYYQn)
    def to_q_str(dt): return f"{dt.year}Q{((dt.month - 1) // 3) + 1}"
    
    start_q = to_q_str(start_dt)
    end_q = to_q_str(pd.Timestamp.today())
    print(f"📌 {tab}: ECOS window {start_q} ~ {end_q}")

    combined = pd.DataFrame()
    country_map = {"XM": "EZ", "KR": "KR"} # 국가 코드 매핑 확인 필요

    for ccy in CCY_LIST:
        ecos_ccy = country_map.get(ccy, ccy)
        try:
            s = ecos_stat_search(BOK_API_KEY, ECOS_GROWTH, "Q", start_q, end_q, item_code1=ecos_ccy)
            s = to_period_index(s, "Q")
            
            if not s.empty:
                tmp = s.to_frame(name=f"{ccy}_Growth")
                combined = tmp if combined.empty else combined.join(tmp, how="outer")
            time.sleep(0.1)
        except Exception as e:
            print(f"⚠️ {tab}: Failed for {ccy}: {e}")

    if combined.empty:
        print(f"❌ {tab}: No data fetched. Sheet NOT updated.")
        return

    combined = combined.sort_index()
    combined = combined[combined.index >= start_dt]
    
    for c in cols:
        if c not in combined.columns: combined[c] = pd.NA
    combined = combined[cols]

    out = combined.reset_index()
    out["Date"] = pd.to_datetime(out["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    out = out.fillna("")

    ws.clear()
    ws.update([headers] + out.values.tolist(), value_input_option="USER_ENTERED")
    print(f"✅ {tab}: updated {len(out)} rows.")

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
