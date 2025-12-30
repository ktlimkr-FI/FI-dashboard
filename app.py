import streamlit as st
import pandas as pd
import yfinance as yf
from fredapi import Fred
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime
import pytz
import requests

# 1. 페이지 설정
st.set_page_config(page_title="Global Financial Dashboard", layout="wide")
st.title("🏦 Comprehensive Financial Market Dashboard")

# 업데이트 시각 표시 (KST)
kst = pytz.timezone('Asia/Seoul')
now_kst = datetime.now(kst).strftime('%Y-%m-%d %H:%M:%S')
st.info(f"🕒 **데이터 업데이트 시각 (KST): {now_kst}** (새로고침 시 갱신)")

# 2. API 키 보안 로드
try:
    FRED_API_KEY = st.secrets["FRED_API_KEY"]
except:
    st.error("⚠️ FRED_API_KEY 설정을 확인해주세요.")
    st.stop()

fred = Fred(api_key=FRED_API_KEY)

# 3. 사이드바 설정
st.sidebar.header("📅 조회 기간 설정")
period_options = {"6개월": 180, "1년": 365, "3년": 1095, "5년": 1825, "10년": 3650}
selected_label = st.sidebar.selectbox("기간 선택", options=list(period_options.keys()), index=4)
days_to_show = period_options[selected_label]

# 4. 데이터 로드 함수들 (캐싱 적용)
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

@st.cache_data(ttl=3600)
def get_ofr_fails_data():
    mnemonics = {
        "NYPD-PD_AFtD_T-A": "UST fails to deliver",
        "NYPD-PD_AFtD_AG-A": "Agency/GSE fails to deliver",
        "NYPD-PD_AFtD_CORS-A": "Corporate fails to deliver",
        "NYPD-PD_AFtD_OMBS-A": "Other MBS fails to deliver",
    }
    url = "https://data.financialresearch.gov/v1/series/multifull"
    params = {"mnemonics": ",".join(mnemonics.keys())}
    try:
        resp = requests.get(url, params=params)
        raw = resp.json()
        frames = []
        for mnem, entry in raw.items():
            if 'timeseries' in entry and 'aggregation' in entry['timeseries']:
                df = pd.DataFrame(entry['timeseries']['aggregation'], columns=['date', 'value'])
                df["date"] = pd.to_datetime(df["date"])
                df = df.set_index("date").rename(columns={"value": mnemonics[mnem]})
                frames.append(df)
        return pd.concat(frames, axis=1).sort_index()
    except: return pd.DataFrame()

# 5. 탭 구성
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 Repo 흐름", "💸 금리 분석", "🌐 유동성&달러", "💹 환율(Yahoo)", "⚠️ Repo Fails (OFR)"
])

# --- 탭 1: Repo 흐름 & SOFR Spread 분석 ---
with tab1:
    st.subheader("1. Overnight Repo Flow (RPONTTLD)")
    repo_df = get_fred_data('RPONTTLD').tail(days_to_show).dropna()
    if not repo_df.empty:
        fig1 = go.Figure()
        fig1.add_trace(go.Scatter(x=repo_df.index, y=repo_df['RPONTTLD'], mode='lines', fill='tozeroy', line=dict(color='royalblue', width=2)))
        fig1.update_layout(title="Daily Repo Volume Trend", template='plotly_white', height=350)
        st.plotly_chart(fig1, use_container_width=True)

    st.subheader("2. SOFR Market Stress (SOFR99th - SOFR)")
    st.caption("상위 1% 금리와 중앙값의 차이입니다. 급등할수록 자금 조달에 어려움을 겪는 기관이 많음을 뜻합니다.")
    
    sofr_d = get_fred_data('SOFR')
    sofr99_d = get_fred_data('SOFR99')
    spread_df = pd.concat([sofr_d, sofr99_d], axis=1).dropna()
    spread_df['Spread'] = spread_df['SOFR99'] - spread_df['SOFR']
    spread_display = spread_df.tail(days_to_show)

    if not spread_display.empty:
        fig_spread = go.Figure()
        fig_spread.add_trace(go.Scatter(
            x=spread_display.index, y=spread_display['Spread'], 
            mode='lines', line=dict(color='darkorange', width=2),
            fill='tozeroy', name="Spread (99th-Median)"
        ))
        fig_spread.update_layout(title="SOFR Spread Trend", template='plotly_white', height=350, yaxis_title="Percent (%)")
        st.plotly_chart(fig_spread, use_container_width=True)

    st.divider()
    st.subheader("3. 🗓️ SOFR 월간 계절성 분석 (10년 평균)")
    with st.spinner('계절성 분석 중...'):
        seasonal_df = pd.concat([get_fred_data('SOFR'), get_fred_data('SOFR99')], axis=1).dropna().tail(3650)
        seasonal_df['Month'] = seasonal_df.index.month
        monthly_avg = seasonal_df.groupby('Month').mean()
        # 스프레드 계절성도 함께 계산
        monthly_avg['Spread'] = monthly_avg['SOFR99'] - monthly_avg['SOFR']

    fig_season = make_subplots(specs=[[{"secondary_y": True}]])
    fig_season.add_trace(go.Bar(x=monthly_avg.index, y=monthly_avg['SOFR'], name="SOFR Avg", marker_color='darkblue', opacity=0.6), secondary_y=False)
    fig_season.add_trace(go.Scatter(x=monthly_avg.index, y=monthly_avg['SOFR99'], name="SOFR 99th Avg", line=dict(color='firebrick', width=2)), secondary_y=False)
    fig_season.add_trace(go.Scatter(x=monthly_avg.index, y=monthly_avg['Spread'], name="Spread Avg (Right)", line=dict(color='orange', width=3, dash='dot')), secondary_y=True)

    fig_season.update_layout(
        title="Monthly Seasonality: SOFR vs Spread",
        xaxis=dict(tickmode='array', tickvals=list(range(1, 13)), ticktext=['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']),
        template='plotly_white', hovermode='x unified'
    )
    fig_season.update_yaxes(title_text="Interest Rate (%)", secondary_y=False)
    fig_season.update_yaxes(title_text="Spread (%)", secondary_y=True)
    st.plotly_chart(fig_season, use_container_width=True)
# --- 탭 2: 금리 분석 (Target Range 음영 포함) ---
with tab2:
    st.subheader("SOFR vs Fed Target Range")
    r_df = pd.concat([
        get_fred_data('SOFR'), get_fred_data('SOFR99'), 
        get_fred_data('DFEDTARU'), get_fred_data('DFEDTARL')
    ], axis=1).ffill()
    r_df = r_df[r_df.index >= '2017-01-01'].tail(days_to_show)
    
    if not r_df.empty:
        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(x=r_df.index, y=r_df['DFEDTARL'], mode='lines', line=dict(width=0), showlegend=False))
        fig2.add_trace(go.Scatter(x=r_df.index, y=r_df['DFEDTARU'], mode='lines', line=dict(width=0), fill='tonexty', fillcolor='rgba(173, 216, 230, 0.3)', name='Target Range'))
        fig2.add_trace(go.Scatter(x=r_df.index, y=r_df['SOFR'], name='SOFR', line=dict(color='darkblue', width=2)))
        fig2.add_trace(go.Scatter(x=r_df.index, y=r_df['SOFR99'], name='SOFR 99th', line=dict(color='orange', width=1, dash='dot')))
        fig2.update_layout(title="SOFR & Target Range Trend", template='plotly_white', hovermode='x unified')
        st.plotly_chart(fig2, use_container_width=True)

# --- 탭 3: 유동성&달러 (이중 축 및 개별 선택 옵션 포함) ---
with tab3:
    st.subheader("OBFR Volume & U.S. Dollar Indices")
    st.caption("거래량(Volume)은 왼쪽 축, 달러 인덱스들은 오른쪽 축을 기준으로 표시됩니다.")

    # 지표 선택을 위한 체크박스들을 한 줄에 배치
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        show_obfr = st.checkbox("OBFR Volume", value=True)
    with c2:
        show_broad = st.checkbox("Broad Index", value=True)
    with c3:
        show_afe = st.checkbox("AFE Index (선진국)", value=False)
    with c4:
        show_eme = st.checkbox("EME Index (신흥국)", value=False)

    # 데이터 로드 및 통합
    d3_raw = pd.concat([
        get_fred_data('OBFRVOL'), 
        get_fred_data('DTWEXBGS'), 
        get_fred_data('DTWEXAFEGS'), 
        get_fred_data('DTWEXEMEGS')
    ], axis=1).ffill()
    
    # 2015년 이후 필터링 및 선택 기간 적용
    d3 = d3_raw[d3_raw.index >= '2015-01-01'].tail(days_to_show)
    
    if not d3.empty:
        # 이중 축 차트 생성
        fig3 = make_subplots(specs=[[{"secondary_y": True}]])
        
        # 1. OBFR 거래량 (왼쪽 축 - 볼륨)
        if show_obfr:
            fig3.add_trace(
                go.Scatter(
                    x=d3.index, y=d3['OBFRVOL'], 
                    name="OBFR Vol (Left)", 
                    line=dict(color='rgba(150, 150, 150, 0.5)', width=1.5),
                    fill='tozeroy' # 거래량 느낌을 주기 위해 바닥 채우기
                ),
                secondary_y=False,
            )
        
        # 2. 달러 인덱스 시리즈 (오른쪽 축 - 지수)
        if show_broad:
            fig3.add_trace(
                go.Scatter(x=d3.index, y=d3['DTWEXBGS'], name="Broad Index (Right)", line=dict(color='royalblue', width=2.5)),
                secondary_y=True,
            )
        
        if show_afe:
            fig3.add_trace(
                go.Scatter(x=d3.index, y=d3['DTWEXAFEGS'], name="AFE Index (Right)", line=dict(color='green', width=1.5)),
                secondary_y=True,
            )
            
        if show_eme:
            fig3.add_trace(
                go.Scatter(x=d3.index, y=d3['DTWEXEMEGS'], name="EME Index (Right)", line=dict(color='firebrick', width=1.5)),
                secondary_y=True,
            )

        # 레이아웃 설정
        fig3.update_layout(
            title=f"Volume vs Dollar Indices ({selected_label})",
            template='plotly_white',
            hovermode='x unified',
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )

        fig3.update_yaxes(title_text="<b>Volume</b> (Millions of $)", secondary_y=False)
        fig3.update_yaxes(title_text="<b>Index Value</b>", secondary_y=True)

        st.plotly_chart(fig3, use_container_width=True)
        
        # 상세 데이터 표 (선택된 지표만 표시)
        st.write("### 데이터 상세 내역")
        st.dataframe(d3.tail(10).iloc[::-1], use_container_width=True)
    else:
        st.warning("데이터를 불러올 수 없습니다.")

# --- 탭 4: 환율 (상대 수익률 및 개별 차트 포함) ---
with tab4:
    st.subheader("Yahoo Finance: Global Currencies")
    yf_raw = get_yfinance_data().tail(days_to_show).ffill().bfill()
    
    view_mode = st.radio("보기 방식", ["상대 수익률 (100 기준)", "절대 가격"], horizontal=True, key="y_view")
    selected_symbols = st.multiselect("지표 선택", options=list(yf_raw.columns), default=list(yf_raw.columns))
    
    target_df = yf_raw.copy()
    if view_mode == "상대 수익률 (100 기준)":
        for col in target_df.columns:
            target_df[col] = (target_df[col] / target_df[col].iloc[0]) * 100

    fig4 = go.Figure()
    for s in selected_symbols:
        fig4.add_trace(go.Scatter(x=target_df.index, y=target_df[s], name=s))
    fig4.update_layout(title=f"통합 환율 ({view_mode})", template='plotly_white', hovermode='x unified')
    st.plotly_chart(fig4, use_container_width=True)
    
    st.write("### 개별 상세 차트 (절대 가격)")
    cols = st.columns(2)
    for i, s in enumerate(selected_symbols):
        with cols[i % 2]:
            fig_i = go.Figure(go.Scatter(x=yf_raw.index, y=yf_raw[s], name=s, line=dict(color='royalblue')))
            fig_i.update_layout(title=s, height=250, margin=dict(l=0,r=0,t=30,b=0), template='plotly_white')
            st.plotly_chart(fig_i, use_container_width=True)

# --- 탭 5: Repo Fails (OFR API + 계절성 분석) ---
with tab5:
    st.subheader("Primary Dealer Repo Fails Analysis")
    with st.spinner('OFR 데이터를 분석 중...'):
        fails_all = get_ofr_fails_data().ffill()
        fails_display = fails_all.tail(days_to_show)

    if not fails_all.empty:
        col1, col2 = st.columns(2)
        with col1:
            st.write("### 항목별 누적 Fails (선택 기간)")
            fig_stacked = go.Figure()
            for col in fails_display.columns:
                fig_stacked.add_trace(go.Scatter(x=fails_display.index, y=fails_display[col], mode='lines', stackgroup='one', name=col))
            fig_stacked.update_layout(template='plotly_white', height=400, yaxis_title="$M")
            st.plotly_chart(fig_stacked, use_container_width=True)
            
        with col2:
            st.write("### UST Fails (선택 기간)")
            fig_ust = go.Figure()
            fig_ust.add_trace(go.Scatter(x=fails_display.index, y=fails_display['UST fails to deliver'], fill='tozeroy', line=dict(color='firebrick')))
            fig_ust.update_layout(template='plotly_white', height=400, yaxis_title="$M")
            st.plotly_chart(fig_ust, use_container_width=True)

        st.divider()
        st.write("## 🗓️ Repo Fails 계절성 분석 (UST Fails 기준)")
        st.info("추세를 제거하고 10년치 데이터를 주간 단위로 분석하여 매년 반복되는 패턴을 보여줍니다.")

        ust_fails = fails_all[['UST fails to deliver']].copy()
        ust_fails['Trend'] = ust_fails['UST fails to deliver'].rolling(window=52, center=True).mean()
        ust_fails['Detrended'] = ust_fails['UST fails to deliver'] - ust_fails['Trend']
        ust_fails['Week'] = ust_fails.index.isocalendar().week
        seasonal_pattern = ust_fails.groupby('Week')['Detrended'].mean().reset_index()

        c1, c2 = st.columns(2)
        with c1:
            st.write("### 1. 추세 제거 데이터 (Detrended)")
            fig_detrended = go.Figure()
            fig_detrended.add_trace(go.Scatter(x=ust_fails.index, y=ust_fails['Detrended'], line=dict(color='purple', width=1)))
            fig_detrended.add_hline(y=0, line_dash="dash", line_color="grey")
            fig_detrended.update_layout(template='plotly_white', height=400)
            st.plotly_chart(fig_detrended, use_container_width=True)

        with c2:
            st.write("### 2. 10년 주간 평균 계절성")
            fig_seasonal = go.Figure()
            fig_seasonal.add_trace(go.Bar(x=seasonal_pattern['Week'], y=seasonal_pattern['Detrended'], marker_color='orange'))
            fig_seasonal.update_layout(template='plotly_white', height=400, xaxis_title="주차 (Week)", yaxis_title="편차")
            st.plotly_chart(fig_seasonal, use_container_width=True)
    else:
        st.error("데이터를 불러올 수 없습니다.")
