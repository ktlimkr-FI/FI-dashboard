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

# 업데이트 시각 표시
kst = pytz.timezone('Asia/Seoul')
now_kst = datetime.now(kst).strftime('%Y-%m-%d %H:%M:%S')
st.info(f"🕒 **데이터 업데이트 시각 (KST): {now_kst}**")

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

# 4. 데이터 로드 함수들
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
    tickers = {"DXY Index": "DX-Y.NYB", "USD/KRW": "USDKRW=X", "USD/CNY": "USDCNY=X", "USD/MXN": "USDMXN=X", "USD/JPY": "USDJPY=X", "USD/EUR": "USDEUR=X"}
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
tab1, tab2, tab3, tab4, tab5 = st.tabs(["📊 Repo 흐름", "💸 금리 분석", "🌐 유동성&달러", "💹 환율(Yahoo)", "⚠️ Repo Fails (OFR)"])

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

# --- 탭 2: 금리 데이터 ---
with tab2:
    st.subheader("SOFR vs Fed Target Range")
    rates_df = pd.concat([get_data('SOFR'), get_data('SOFR99'), get_data('DFEDTARU'), get_data('DFEDTARL')], axis=1).ffill()
    rates_df = rates_df[rates_df.index >= '2017-01-01'].tail(days_to_show)
    if not rates_df.empty:
        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(x=rates_df.index, y=rates_df['DFEDTARL'], mode='lines', line=dict(width=0), showlegend=False))
        fig2.add_trace(go.Scatter(x=rates_df.index, y=rates_df['DFEDTARU'], mode='lines', line=dict(width=0), fill='tonexty', fillcolor='rgba(173, 216, 230, 0.3)', name='Target Range'))
        fig2.add_trace(go.Scatter(x=rates_df.index, y=rates_df['SOFR'], mode='lines', line=dict(color='darkblue', width=2), name='SOFR'))
        fig2.add_trace(go.Scatter(x=rates_df.index, y=rates_df['SOFR99'], mode='lines', line=dict(color='orange', width=1, dash='dot'), name='SOFR 99th'))
        fig2.update_layout(template='plotly_white', hovermode='x unified')
        st.plotly_chart(fig2, use_container_width=True)

# --- 탭 3: 유동성 & 달러 인덱스 (신규) ---
with tab3:
    st.subheader("Overnight Bank Funding Volume & U.S. Dollar Indices")
    st.caption("2015년 이후 데이터 (달러 인덱스는 오른쪽 축 표시)")
    
    # OBFR 표시 여부 버튼
    show_obfr = st.checkbox("Show Overnight Bank Funding Volume (OBFRVOL)", value=True)

    with st.spinner('데이터를 불러오는 중...'):
        # 데이터 수집
        d_obfr = get_data('OBFRVOL')
        d_broad = get_data('DTWEXBGS')
        d_afe = get_data('DTWEXAFEGS')
        d_eme = get_data('DTWEXEMEGS')

        # 데이터 통합 및 2015년 이후 필터링
        df3 = pd.concat([d_obfr, d_broad, d_afe, d_eme], axis=1).ffill()
        df3 = df3[df3.index >= '2015-01-01'].tail(days_to_show)

    if not df3.empty:
        # 이중 축 차트 생성
        fig3 = make_subplots(specs=[[{"secondary_y": True}]])

        # 1. OBFR 거래량 (왼쪽 축) - 버튼 선택 시에만 표시
        if show_obfr:
            fig3.add_trace(
                go.Scatter(x=df3.index, y=df3['OBFRVOL'], name="OBFR Volume (Left)", 
                           line=dict(color='rgba(100, 100, 100, 0.5)', width=1.5)),
                secondary_y=False,
            )

        # 2. 달러 인덱스 시리즈 (오른쪽 축)
        fig3.add_trace(
            go.Scatter(x=df3.index, y=df3['DTWEXBGS'], name="Broad Dollar (Right)", line=dict(color='royalblue', width=2)),
            secondary_y=True,
        )
        fig3.add_trace(
            go.Scatter(x=df3.index, y=df3['DTWEXAFEGS'], name="AFE Dollar (Right)", line=dict(color='green', width=1.5)),
            secondary_y=True,
        )
        fig3.add_trace(
            go.Scatter(x=df3.index, y=df3['DTWEXEMEGS'], name="EME Dollar (Right)", line=dict(color='firebrick', width=1.5)),
            secondary_y=True,
        )

        fig3.update_layout(
            title=f"Volume vs Dollar Index Trend ({selected_label})",
            template='plotly_white',
            hovermode='x unified',
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )

        fig3.update_yaxes(title_text="Volume (Millions of $)", secondary_y=False)
        fig3.update_yaxes(title_text="Index Value", secondary_y=True)

        st.plotly_chart(fig3, use_container_width=True)
        st.dataframe(df3.tail(10).iloc[::-1])
    else:
        st.warning("데이터를 불러올 수 없습니다.")

# --- 탭 4: 환율 및 달러 인덱스 (Yahoo Finance) ---
with tab4:
    st.subheader("Global Currency & Dollar Index (10Y Daily)")
    
    with st.spinner('Yahoo Finance 데이터를 불러오는 중...'):
        yf_data = get_yfinance_data()
        # 선택한 기간만큼 필터링
        yf_display = yf_data.tail(days_to_show)

    if not yf_display.empty:
        # 1. 통합 차트 섹션
        st.write("### 통합 비교 차트")
        # 제거/추가 옵션 버튼 (Multiselect 활용)
        selected_symbols = st.multiselect(
            "차트에 표시할 지표를 선택하세요 (제거하려면 X 클릭)",
            options=list(yf_display.columns),
            default=list(yf_display.columns)
        )

        fig4_combined = go.Figure()
        for symbol in selected_symbols:
            fig4_combined.add_trace(go.Scatter(
                x=yf_display.index, y=yf_display[symbol],
                mode='lines', name=symbol
            ))
        
        fig4_combined.update_layout(
            title="통합 환율 추이",
            template='plotly_white',
            hovermode='x unified',
            yaxis_title="Value"
        )
        st.plotly_chart(fig4_combined, use_container_width=True)

        # 2. 개별 차트 섹션
        st.divider()
        st.write("### 개별 상세 차트")
        # 2개씩 한 줄에 배치
        cols = st.columns(2)
        for i, symbol in enumerate(yf_display.columns):
            with cols[i % 2]:
                fig_ind = go.Figure()
                fig_ind.add_trace(go.Scatter(
                    x=yf_display.index, y=yf_display[symbol],
                    mode='lines', name=symbol, line=dict(width=2)
                ))
                fig_ind.update_layout(
                    title=f"{symbol} 상세",
                    template='plotly_white',
                    height=300,
                    margin=dict(l=0, r=0, t=30, b=0)
                )
                st.plotly_chart(fig_ind, use_container_width=True)
    else:
        st.error("데이터를 가져오는 데 실패했습니다.")

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


