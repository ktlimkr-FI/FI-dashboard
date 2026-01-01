import json
import pandas as pd
import gspread
import streamlit as st
from google.oauth2.service_account import Credentials

import plotly.graph_objects as go
from plotly.subplots import make_subplots

days_to_show = 365  # 테스트용

def apply_mobile_style(fig):
    return fig  # 테스트용(기존 함수가 있으면 이 줄은 제거)

@st.cache_data(ttl=3600)
def load_daily_from_gsheet(worksheet_name: str = "data-daily") -> pd.DataFrame:
    info = json.loads(st.secrets["GOOGLE_SERVICE_ACCOUNT_JSON"])
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)

    gc = gspread.authorize(creds)
    sh = gc.open_by_key(st.secrets["GSHEET_ID"])
    ws = sh.worksheet(worksheet_name)

    records = ws.get_all_records()
    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)

    if "Date" in df.columns:
        date_col = "Date"
    elif "date" in df.columns:
        date_col = "date"
    else:
        raise ValueError("Google Sheet must have a 'Date' (or 'date') column.")

    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col]).set_index(date_col).sort_index()

    for c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="ignore")

    return df.ffill()

(tab1,) = st.tabs(["📊 Repo 흐름"])   # ✅ 핵심 수정

with tab1:
    st.subheader("1. Overnight Repo Flow (Repo_Volume)")

    try:
        all_daily_df = load_daily_from_gsheet("data-daily")
    except Exception as e:
        st.error(f"구글 시트 로드 실패: {e}")
        st.stop()

    if "Repo_Volume" in all_daily_df.columns:
        repo_df = all_daily_df["Repo_Volume"].tail(days_to_show).dropna()

        fig1 = go.Figure()
        fig1.add_trace(go.Scatter(x=repo_df.index, y=repo_df, mode="lines", fill="tozeroy"))
        fig1.update_layout(title="Daily Repo Volume Trend (from GSheets)", height=350)
        st.plotly_chart(apply_mobile_style(fig1), use_container_width=True)
    else:
        st.warning("시트에 'Repo_Volume' 컬럼이 없습니다.")
