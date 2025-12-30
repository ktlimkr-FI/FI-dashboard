import streamlit as st
import pandas as pd
import yfinance as yf
from fredapi import Fred
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# 1. 페이지 설정
st.set_page_config(page_title="Global Financial Dashboard", layout="wide")
st.title("🏦 Federal Reserve & Global Market Dashboard")

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

# 4. 데이터 로드 함수
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

# 5. 탭 구성 (총 4개)
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

# --- 탭 4: 환율(Yahoo) 가시성 해결 버전 ---
with tab4:
    st.subheader("Global Currency Performance (10Y Daily)")
    
    with st.spinner('데이터를 정제하는 중...'):
        # 1. 데이터 가져오기 및 결측치 보정
        yf_raw = get_yfinance_data()
        
        # 선택한 기간만큼 자른 후, 앞뒤 결측치를 채워 계산 오류 방지
        yf_display = yf_raw.tail(days_to_show).ffill().bfill()

    if not yf_display.empty:
        view_mode = st.radio(
            "통합 차트 보기 방식", 
            ["상대 수익률 (시작점 100 기준)", "절대 가격 (원본)"], 
            horizontal=True
        )

        selected_symbols = st.multiselect(
            "표시할 지표 선택", 
            options=list(yf_display.columns), 
            default=list(yf_display.columns)
        )

        # 2. 상대 수익률 계산 로직 개선
        if view_mode == "상대 수익률 (시작점 100 기준)":
            # 각 컬럼의 첫 번째 유효한 값(NaN이 아닌 첫 값)으로 나누어 100 기준 설정
            target_df = yf_display.copy()
            for col in target_df.columns:
                first_valid_value = target_df[col].iloc[0]
                if first_valid_value != 0:
                    target_df[col] = (target_df[col] / first_valid_value) * 100
            yaxis_title = "Index (Start = 100)"
        else:
            target_df = yf_display
            yaxis_title = "Absolute Value"

        # 3. 통합 차트 생성
        fig4 = go.Figure()
        for s in selected_symbols:
            if s in target_df.columns:
                fig4.add_trace(go.Scatter(x=target_df.index, y=target_df[s], name=s))
        
        fig4.update_layout(
            title=f"통합 환율 추이 ({view_mode})",
            template='plotly_white', 
            hovermode='x unified',
            yaxis_title=yaxis_title,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        st.plotly_chart(fig4, use_container_width=True)
        
        # 4. 개별 상세 차트 (원본 데이터 사용)
        st.divider()
        st.write("### 개별 상세 차트 (절대 가격)")
        cols = st.columns(2)
        for i, s in enumerate(selected_symbols):
            with cols[i % 2]:
                fig_i = go.Figure(go.Scatter(x=yf_display.index, y=yf_display[s], name=s))
                fig_i.update_layout(
                    title=f"{s} (절대 가격)", 
                    height=250, 
                    template='plotly_white',
                    margin=dict(l=0, r=0, t=30, b=0)
                )
                st.plotly_chart(fig_i, use_container_width=True)
