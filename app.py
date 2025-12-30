import streamlit as st
import pandas as pd
import yfinance as yf
from fredapi import Fred
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime
import pytz  # 시간대 설정을 위한 라이브러리

# 1. 페이지 설정
st.set_page_config(page_title="Global Financial Dashboard", layout="wide")
st.title("🏦 Federal Reserve & Global Market Dashboard")

# --- [신규] 업데이트 시각 표시 ---
kst = pytz.timezone('Asia/Seoul')
now_kst = datetime.now(kst).strftime('%Y-%m-%d %H:%M:%S')
st.info(f"🕒 **데이터 업데이트 시각 (KST): {now_kst}** (1시간마다 자동 갱신 및 새로고침 시 반영)")

# 2. API 키 보안 로드
try:
    FRED_API_KEY = st.secrets["FRED_API_KEY"]
except:
    st.error("⚠️ FRED_API_KEY 설정을 확인해주세요.")
    st.stop()

fred = Fred(api_key=FRED_API_KEY)

# 3. 사이드바 - 공통 설정
st.sidebar.header("📅 조회 기간 설정")
period_options = {"6개월": 180, "1년": 365, "3년": 1095, "5년": 1825, "10년": 3650}
selected_label = st.sidebar.selectbox("기간 선택", options=list(period_options.keys()), index=4)
days_to_show = period_options[selected_label]

# 4. 데이터 로드 함수 (캐싱 적용)
@st.cache_data(ttl=3600)
def get_fred_data(series_id):
    try:
        data = fred.get_series(series_id)
        df = pd.DataFrame(data, columns=[series_id])
        df.index.name = 'date'
        return df
    except: return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_yfinance_data():
    tickers = {
        "DXY Index": "DX-Y.NYB", "USD/KRW": "USDKRW=X", "USD/CNY": "USDCNY=X",
        "USD/MXN": "USDMXN=X", "USD/JPY": "USDJPY=X", "USD/EUR": "USDEUR=X"
    }
    data = yf.download(list(tickers.values()), period="10y", interval="1d")['Close']
    inv_tickers = {v: k for k, v in tickers.items()}
    data.rename(columns=inv_tickers, inplace=True)
    return data

# 5. 탭 구성
tab1, tab2, tab3, tab4 = st.tabs(["📊 Repo 흐름", "💸 금리 분석", "🌐 유동성&달러", "💹 환율(Yahoo)"])

# --- 탭 1: Repo ---
with tab1:
    st.subheader("Overnight Repurchase Agreements (RPONTTLD)")
    repo_style = st.radio("차트 종류", ["선 그래프", "바 그래프"], horizontal=True, key="r_style")
    repo_df = get_fred_data('RPONTTLD').tail(days_to_show).dropna()
    if not repo_df.empty:
        fig1 = go.Figure()
        if repo_style == "선 그래프":
            fig1.add_trace(go.Scatter(x=repo_df.index, y=repo_df['RPONTTLD'], mode='lines', fill='tozeroy'))
        else:
            fig1.add_trace(go.Bar(x=repo_df.index, y=repo_df['RPONTTLD'], marker_color='royalblue'))
        fig1.update_layout(template='plotly_white', hovermode='x unified')
        st.plotly_chart(fig1, use_container_width=True)

# --- 탭 2: 금리 ---
with tab2:
    st.subheader("SOFR vs Fed Target Range")
    r_df = pd.concat([get_fred_data('SOFR'), get_fred_data('SOFR99'), get_fred_data('DFEDTARU'), get_fred_data('DFEDTARL')], axis=1).ffill()
    r_df = r_df[r_df.index >= '2017-01-01'].tail(days_to_show)
    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(x=r_df.index, y=r_df['DFEDTARL'], mode='lines', line=dict(width=0), showlegend=False))
    fig2.add_trace(go.Scatter(x=r_df.index, y=r_df['DFEDTARU'], mode='lines', line=dict(width=0), fill='tonexty', fillcolor='rgba(173, 216, 230, 0.3)', name='Target Range'))
    fig2.add_trace(go.Scatter(x=r_df.index, y=r_df['SOFR'], name='SOFR', line=dict(color='darkblue', width=2)))
    fig2.update_layout(template='plotly_white', hovermode='x unified')
    st.plotly_chart(fig2, use_container_width=True)

# --- 탭 3: 유동성&달러 ---
with tab3:
    st.subheader("OBFR Volume & Dollar Indices")
    show_obfr = st.checkbox("Show OBFR Volume", value=True)
    d3 = pd.concat([get_fred_data('OBFRVOL'), get_fred_data('DTWEXBGS'), get_fred_data('DTWEXAFEGS'), get_fred_data('DTWEXEMEGS')], axis=1).ffill()
    d3 = d3[d3.index >= '2015-01-01'].tail(days_to_show)
    fig3 = make_subplots(specs=[[{"secondary_y": True}]])
    if show_obfr:
        fig3.add_trace(go.Scatter(x=d3.index, y=d3['OBFRVOL'], name="OBFR Vol", line=dict(color='lightgrey')), secondary_y=False)
    fig3.add_trace(go.Scatter(x=d3.index, y=d3['DTWEXBGS'], name="Broad Index"), secondary_y=True)
    fig3.update_layout(template='plotly_white', hovermode='x unified')
    st.plotly_chart(fig3, use_container_width=True)

# --- 탭 4: 환율(Yahoo) ---
with tab4:
    st.subheader("Yahoo Finance: Global Currencies (Normalization)")
    yf_raw = get_yfinance_data().tail(days_to_show).ffill().bfill()
    
    view_mode = st.radio("보기 방식", ["상대 수익률 (100 기준)", "절대 가격"], horizontal=True)
    selected_symbols = st.multiselect("지표 선택", options=list(yf_raw.columns), default=list(yf_raw.columns))
    
    target_df = yf_raw.copy()
    if view_mode == "상대 수익률 (100 기준)":
        for col in target_df.columns:
            target_df[col] = (target_df[col] / target_df[col].iloc[0]) * 100

    fig4 = go.Figure()
    for s in selected_symbols:
        fig4.add_trace(go.Scatter(x=target_df.index, y=target_df[s], name=s))
    fig4.update_layout(template='plotly_white', hovermode='x unified', yaxis_title=view_mode)
    st.plotly_chart(fig4, use_container_width=True)
    
