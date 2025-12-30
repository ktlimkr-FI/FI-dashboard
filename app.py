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

# --- 탭 5: Repo Fails (OFR API + 계절성 분석) ---
with tab5:
    st.subheader("Primary Dealer Repo Fails Analysis")
    
    with st.spinner('OFR 데이터를 분석 중...'):
        # 10년치 전체 데이터를 가져와서 계절성 분석에 활용
        fails_all = get_ofr_fails_data().ffill()
        fails_display = fails_all.tail(days_to_show)

    if not fails_all.empty:
        # --- 기존 차트 (누적 및 국채 단독) ---
        col1, col2 = st.columns(2)
        with col1:
            st.write("### 항목별 누적 Fails (선택 기간)")
            fig_stacked = go.Figure()
            for col in fails_display.columns:
                fig_stacked.add_trace(go.Scatter(x=fails_display.index, y=fails_display[col], mode='lines', stackgroup='one', name=col))
            fig_stacked.update_layout(template='plotly_white', height=400)
            st.plotly_chart(fig_stacked, use_container_width=True)
            
        with col2:
            st.write("### UST Fails (선택 기간)")
            fig_ust = go.Figure()
            fig_ust.add_trace(go.Scatter(x=fails_display.index, y=fails_display['UST fails to deliver'], fill='tozeroy', line=dict(color='firebrick')))
            fig_ust.update_layout(template='plotly_white', height=400)
            st.plotly_chart(fig_ust, use_container_width=True)

        st.divider()

        # --- [신규] 계절성 분석 섹션 ---
        st.write("## 🗓️ Repo Fails 계절성 분석 (UST Fails 기준)")
        st.info("추세를 제거하고 10년치 데이터를 주간 단위로 분석하여 매년 반복되는 패턴을 보여줍니다.")

        # 1. 추세 제거 (Detrending)
        # 52주(1년) 이동평균을 구하여 원본에서 뺌으로써 장기 추세 제거
        ust_fails = fails_all[['UST fails to deliver']].copy()
        ust_fails['Trend'] = ust_fails['UST fails to deliver'].rolling(window=52, center=True).mean()
        ust_fails['Detrended'] = ust_fails['UST fails to deliver'] - ust_fails['Trend']

        # 2. 주간 평균 계절성 계산 (10년치 활용)
        # 날짜에서 주차(Week Number) 추출
        ust_fails['Week'] = ust_fails.index.isocalendar().week
        seasonal_pattern = ust_fails.groupby('Week')['Detrended'].mean().reset_index()

        c1, c2 = st.columns(2)
        
        with c1:
            st.write("### 1. 추세 제거 데이터 (Detrended)")
            st.caption("장기 추세를 제거하여 평균 대비 과도하게 발생한 시점을 보여줍니다.")
            fig_detrended = go.Figure()
            fig_detrended.add_trace(go.Scatter(x=ust_fails.index, y=ust_fails['Detrended'], line=dict(color='purple', width=1)))
            fig_detrended.add_hline(y=0, line_dash="dash", line_color="grey")
            fig_detrended.update_layout(template='plotly_white', height=400)
            st.plotly_chart(fig_detrended, use_container_width=True)

        with c2:
            st.write("### 2. 10년 주간 평균 계절성")
            st.caption("1월(1주)부터 12월(52주)까지의 평균적인 Fails 발생 패턴")
            fig_seasonal = go.Figure()
            fig_seasonal.add_trace(go.Bar(
                x=seasonal_pattern['Week'], 
                y=seasonal_pattern['Detrended'],
                marker_color='orange'
            ))
            fig_seasonal.update_layout(
                template='plotly_white', 
                height=400,
                xaxis_title="주차 (Week Number)",
                yaxis_title="평균 대비 편차"
            )
            st.plotly_chart(fig_seasonal, use_container_width=True)

        st.success("💡 **분석 결과:** 특정 주차(예: 분말, 연말)에 막대가 높게 나타난다면, 해당 시기에 정기적으로 국채 결제 불이행이 증가하는 경향이 있음을 의미합니다.")
    else:
        st.error("데이터를 불러올 수 없습니다.")


