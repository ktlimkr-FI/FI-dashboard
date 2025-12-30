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
    BOK_API_KEY = st.secrets["BOK_API_KEY"] # 한국은행 키 추가
except:
    st.error("⚠️ API_KEY 설정을 확인해주세요 (FRED_API_KEY, BOK_API_KEY).")
    st.stop()

fred = Fred(api_key=FRED_API_KEY)

# 3. 사이드바 설정
st.sidebar.header("📅 조회 기간 설정")
period_options = {"6개월": 180, "1년": 365, "3년": 1095, "5년": 1825, "10년": 3650}
selected_label = st.sidebar.selectbox("기간 선택", options=list(period_options.keys()), index=2)
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

@st.cache_data(ttl=3600)
def get_bok_yield_data(item_code, item_name):
    """
    한국은행 ECOS API를 통해 금리 데이터를 가져옵니다.
    817Y002: 시장금리(일일)
    010200000: 국고채(3년)
    010210000: 국고채(10년)
    """
    start_date = (datetime.now() - pd.Timedelta(days=3650)).strftime('%Y%m%d')
    end_date = datetime.now().strftime('%Y%m%d')
    
    url = f"http://ecos.bok.or.kr/api/StatisticSearch/{BOK_API_KEY}/json/kr/1/10000/817Y002/D/{start_date}/{end_date}/{item_code}"
    
    try:
        resp = requests.get(url)
        data = resp.json()
        if 'StatisticSearch' in data:
            rows = data['StatisticSearch']['row']
            df = pd.DataFrame(rows)
            df['TIME'] = pd.to_datetime(df['TIME'])
            df['DATA_VALUE'] = pd.to_numeric(df['DATA_VALUE'])
            df = df[['TIME', 'DATA_VALUE']].rename(columns={'TIME': 'date', 'DATA_VALUE': item_name})
            return df.set_index('date')
    except Exception as e:
        st.error(f"BOK API 에러 ({item_name}): {e}")
    return pd.DataFrame()

# 5. 탭 구성
tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
    "📊 Repo 흐름", "💸 금리 분석", "🌐 유동성&달러", "💹 환율(Yahoo)", "⚠️ Repo Fails (OFR)", "⚠️ Dollar Index Weight", "⚠️ Yield Curve(US&KR)"
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

# --- 탭 2: 금리 분석 & 정책 이탈도(Deviation) 분석 ---
with tab2:
    st.subheader("SOFR vs Fed Target Range")
    
    # 데이터 준비
    r_df = pd.concat([
        get_fred_data('SOFR'), get_fred_data('SOFR99'), 
        get_fred_data('DFEDTARU'), get_fred_data('DFEDTARL')
    ], axis=1).ffill()
    
    # 분석에 필요한 중간값 및 이탈도 계산
    r_df['Mid'] = (r_df['DFEDTARU'] + r_df['DFEDTARL']) / 2
    r_df['SOFR_Diff'] = r_df['SOFR'] - r_df['Mid']
    r_df['SOFR99_Diff'] = r_df['SOFR99'] - r_df['Mid']
    
    r_df = r_df[r_df.index >= '2017-01-01'].tail(days_to_show)
    
    if not r_df.empty:
        # 차트 1: 원본 금리 추이 (진한 음영 버전)
        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(x=r_df.index, y=r_df['DFEDTARL'], mode='lines', line=dict(width=0), showlegend=False))
        fig2.add_trace(go.Scatter(
            x=r_df.index, y=r_df['DFEDTARU'], mode='lines', line=dict(width=0), 
            fill='tonexty', fillcolor='rgba(100, 149, 237, 0.6)', name='Target Range'
        ))
        fig2.add_trace(go.Scatter(x=r_df.index, y=r_df['SOFR'], name='SOFR', line=dict(color='darkblue', width=2.5)))
        fig2.add_trace(go.Scatter(x=r_df.index, y=r_df['SOFR99'], name='SOFR 99th', line=dict(color='orange', width=1.5, dash='dot')))
        
        fig2.update_layout(title="SOFR & Target Range Trend", template='plotly_white', hovermode='x unified', yaxis_title="Percent (%)")
        st.plotly_chart(fig2, use_container_width=True)

        st.divider()

        # 차트 2: 정책 이탈도 분석 (Deviation from Midpoint)
        st.subheader("🎯 Policy Deviation Analysis")
        st.caption("연준 목표 범위 중간값(0선) 대비 시장 금리의 이탈 정도를 보여줍니다.")
        
        
        
        fig_diff = go.Figure()
        
        # 0선(중간값 가이드라인)
        fig_diff.add_hline(y=0, line_dash="solid", line_color="black", line_width=2, annotation_text="Target Midpoint")
        
        # 이탈도 데이터 추가
        fig_diff.add_trace(go.Scatter(
            x=r_df.index, y=r_df['SOFR_Diff'], 
            name='SOFR - Midpoint', 
            line=dict(color='darkblue', width=2),
            fill='tozeroy', fillcolor='rgba(0, 0, 139, 0.1)' # 가시성을 위해 옅은 채우기 추가
        ))
        
        fig_diff.add_trace(go.Scatter(
            x=r_df.index, y=r_df['SOFR99_Diff'], 
            name='SOFR99th - Midpoint', 
            line=dict(color='orange', width=1.5, dash='dot')
        ))
        
        fig_diff.update_layout(
            title="Deviation from Fed Target Midpoint (Market Stress)", 
            template='plotly_white', 
            hovermode='x unified',
            yaxis_title="Basis Points (Difference)",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        
        # Y축 단위를 %로 표시하기 위한 설정
        fig_diff.update_yaxes(ticksuffix="%")
        
        st.plotly_chart(fig_diff, use_container_width=True)
        
        st.success("""
        💡 **분석 팁:**
        * **SOFR - Midpoint가 0 위로 크게 튈 때:** 연준의 의도보다 시장의 실제 자금 사정이 빡빡함을 의미합니다.
        * **SOFR99th - Midpoint:** 시장 내에서 가장 비싸게 돈을 빌리는 주체가 연준의 가이드라인에서 얼마나 멀어져 있는지를 보여줍니다. 이 수치가 급증하면 시스템 리스크 신호로 해석될 수 있습니다.
        """)
        
# --- 탭 3: 유동성&달러 (변화율 분석 테이블 추가) ---
with tab3:
    st.subheader("🌐 Global Dollar Strength Analysis")
    st.caption("달러 인덱스와 주요 통화의 기간별 변화율을 비교합니다. (수치가 +이면 달러 강세/해당 통화 가치 하락)")

    # 1. 상단 차트 섹션 (기존 코드 유지 및 일부 최적화)
    c1, c2, c3, c4 = st.columns(4)
    with c1: show_obfr = st.checkbox("OBFR Volume", value=True)
    with c2: show_broad = st.checkbox("Broad Index", value=True)
    with c3: show_afe = st.checkbox("AFE Index", value=False)
    with c4: show_eme = st.checkbox("EME Index", value=False)

    # 데이터 로드 (FRED 인덱스 + Yahoo 환율 통합)
    d3_indices = pd.concat([
        get_fred_data('OBFRVOL'), get_fred_data('DTWEXBGS'), 
        get_fred_data('DTWEXAFEGS'), get_fred_data('DTWEXEMEGS')
    ], axis=1).ffill()
    
    yf_fx = get_yfinance_data().ffill() # 탭 4에서 쓰는 환율 데이터 가져오기
    
    # 분석을 위한 전체 데이터 통합
    combined_df = pd.concat([d3_indices, yf_fx], axis=1).ffill().dropna()
    d3 = combined_df.tail(days_to_show)

    if not d3.empty:
        fig3 = make_subplots(specs=[[{"secondary_y": True}]])
        if show_obfr:
            fig3.add_trace(go.Scatter(x=d3.index, y=d3['OBFRVOL'], name="OBFR Vol (Left)", 
                                     line=dict(color='rgba(150, 150, 150, 0.5)', width=1.5), fill='tozeroy'), secondary_y=False)
        if show_broad:
            fig3.add_trace(go.Scatter(x=d3.index, y=d3['DTWEXBGS'], name="Broad Index (Right)", line=dict(color='royalblue', width=2.5)), secondary_y=True)
        if show_afe:
            fig3.add_trace(go.Scatter(x=d3.index, y=d3['DTWEXAFEGS'], name="AFE Index (Right)", line=dict(color='green', width=1.5)), secondary_y=True)
        if show_eme:
            fig3.add_trace(go.Scatter(x=d3.index, y=d3['DTWEXEMEGS'], name="EME Index (Right)", line=dict(color='firebrick', width=1.5)), secondary_y=True)

        fig3.update_layout(template='plotly_white', hovermode='x unified', height=400,
                          legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
        st.plotly_chart(fig3, use_container_width=True)

        st.divider()

        # 2. [신규] 기간별 변화율(Rate of Change) 분석 테이블
        st.write("### 📈 달러 기준 기간별 변화율 (%)")
        st.caption("기준일로부터 현재까지의 변동폭입니다. 빨간색은 달러 강세, 파란색은 달러 약세를 의미합니다.")

        # 변화율 계산 함수
        def calc_roc(df):
            # 영업일 기준 오프셋 (1일, 1주, 1달, 3달, 6달, 1년)
            intervals = {'1D': 1, '1W': 5, '1M': 21, '3M': 63, '6M': 126, '1Y': 252}
            assets = ['DTWEXBGS', 'DTWEXAFEGS', 'DTWEXEMEGS', 'USD/KRW', 'USD/JPY', 'USD/EUR', 'USD/CNY', 'USD/MXN']
            
            roc_results = []
            current_vals = df.iloc[-1]
            
            for asset in assets:
                if asset in df.columns:
                    row = {'Asset': asset}
                    for label, days in intervals.items():
                        if len(df) > days:
                            prev_val = df[asset].iloc[-(days + 1)]
                            change = ((current_vals[asset] / prev_val) - 1) * 100
                            row[label] = round(change, 2)
                        else:
                            row[label] = None
                    roc_results.append(row)
            
            return pd.DataFrame(roc_results).set_index('Asset')

        roc_df = calc_roc(combined_df)

        # 테이블 스타일링 (양수는 빨강, 음수는 파랑)
        def color_map(val):
            if val is None: return ''
            color = 'red' if val > 0 else 'blue'
            return f'color: {color}; font-weight: bold'

        st.dataframe(
            roc_df.style.applymap(color_map, subset=['1D', '1W', '1M', '3M', '6M', '1Y'])
                       .format("{:+.2f}%", na_rep="-"),
            use_container_width=True
        )

        st.info("""
        💡 **데이터 해석 가이드:**
        * **달러 인덱스(DTWEX...) 상승:** 전반적인 달러 가치 상승.
        * **환율(USD/KRW 등) 상승:** 달러 대비 해당 통화의 가치 하락 (달러 강세).
        * 모든 지표가 **빨간색(Plus)**을 나타내면 전방위적인 '킹달러' 국면으로 해석할 수 있습니다.
        """)
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
# --- [수정] 계절성 분석 섹션 ---
        st.write("## 🗓️ Repo Fails 계절성 분석 (UST Fails 기준)")
        st.info("9월~12월(연말 구간)은 회색 음영으로 표시됩니다. 이 시기의 패턴 변화를 주목하세요.")

        # 1. 추세 제거 (Detrending)
        ust_fails = fails_all[['UST fails to deliver']].copy()
        ust_fails['Trend'] = ust_fails['UST fails to deliver'].rolling(window=52, center=True).mean()
        ust_fails['Detrended'] = ust_fails['UST fails to deliver'] - ust_fails['Trend']
        ust_fails['Week'] = ust_fails.index.isocalendar().week
        seasonal_pattern = ust_fails.groupby('Week')['Detrended'].mean().reset_index()

        c1, c2 = st.columns(2)
        
        with c1:
            st.write("### 1. 추세 제거 데이터 (Detrended)")
            fig_detrended = go.Figure()
            
            # 매년 9월 1일부터 12월 31일까지 음영 추가
            years = ust_fails.index.year.unique()
            for year in years:
                fig_detrended.add_vrect(
                    x0=f"{year}-09-01", x1=f"{year}-12-31",
                    fillcolor="rgba(128, 128, 128, 0.2)", opacity=0.3,
                    layer="below", line_width=0,
                )
            
            fig_detrended.add_trace(go.Scatter(x=ust_fails.index, y=ust_fails['Detrended'], line=dict(color='purple', width=1), name="Detrended"))
            fig_detrended.add_hline(y=0, line_dash="dash", line_color="grey")
            fig_detrended.update_layout(template='plotly_white', height=400, showlegend=False)
            st.plotly_chart(fig_detrended, use_container_width=True)

        with c2:
            st.write("### 2. 10년 주간 평균 계절성")
            fig_seasonal = go.Figure()
            
            # 주간 차트 음영: 보통 36주차(9월 초) ~ 52주차(12월 말)
            fig_seasonal.add_vrect(
                x0=35.5, x1=52.5,
                fillcolor="rgba(128, 128, 128, 0.2)", opacity=0.3,
                layer="below", line_width=0,
                annotation_text="Sep-Dec Area", annotation_position="top left"
            )
            
            fig_seasonal.add_trace(go.Bar(
                x=seasonal_pattern['Week'], 
                y=seasonal_pattern['Detrended'], 
                marker_color='orange',
                name="Avg Deviation"
            ))
            
            fig_seasonal.update_layout(
                template='plotly_white', 
                height=400, 
                xaxis_title="주차 (Week)", 
                yaxis_title="편차",
                showlegend=False
            )
            st.plotly_chart(fig_seasonal, use_container_width=True)

        st.success("💡 **분석 가이드:** 음영 구역(9월-12월) 내에서 막대가 솟아오르는 패턴이 보인다면, 연말 결제 수요로 인한 정기적인 레포 시장 병목 현상이 존재함을 시사합니다.")

# --- 탭 6: Fed 달러 인덱스 가중치 분석 (H.10 데이터) ---
with tab6:
    st.subheader("📊 Fed Dollar Index Weights Analysis")
    st.info("연준(Federal Reserve) 공식 H.10 데이터를 실시간으로 스크래핑하여 인덱스 구성 비중을 분석합니다.")

    @st.cache_data(ttl=86400) # 데이터가 자주 바뀌지 않으므로 24시간 캐싱
    def get_fed_weights_data():
        url = "https://www.federalreserve.gov/releases/h10/weights/default.htm"
        try:
            # lxml 또는 html5lib 엔진 사용
            tables = pd.read_html(url)
            # 연준 페이지 구조: 0번(Broad), 1번(AFE), 2번(EME)
            return {
                "Broad Index": tables[0],
                "AFE Index (선진국)": tables[1],
                "EME Index (신흥국)": tables[2]
            }
        except Exception as e:
            st.error(f"연준 사이트 데이터 로드 실패: {e}")
            return None

    weights_dict = get_fed_weights_data()

    if weights_dict:
        # 분석할 인덱스 선택
        selected_idx = st.radio("분석 대상 인덱스", list(weights_dict.keys()), horizontal=True)
        raw_df = weights_dict[selected_idx]

        # 데이터 정제 로직
        # 1. 첫 번째 열(Currency/Country)을 인덱스로 설정
        clean_df = raw_df.set_index(raw_df.columns[0])
        # 2. 숫자 외 데이터 제거 및 형변환
        clean_df = clean_df.apply(pd.to_numeric, errors='coerce').dropna(how='all')
        
        # 최신 연도와 시계열 연도 확인
        years = clean_df.columns.tolist()
        latest_year = years[-1]

        # --- 레이아웃: 왼쪽(파이차트), 오른쪽(시계열) ---
        col_left, col_right = st.columns([1, 1.5])

        with col_left:
            st.write(f"#### 🥧 {selected_idx} 구성 (최신: {latest_year}년)")
            # 상위 8개 추출 및 나머지 'Others' 합산
            current_weights = clean_df[latest_year].sort_values(ascending=False)
            top_8 = current_weights.head(8)
            others = pd.Series({"Others": current_weights.iloc[8:].sum()})
            pie_data = pd.concat([top_8, others])

            fig_pie = go.Figure(data=[go.Pie(
                labels=pie_data.index, 
                values=pie_data.values, 
                hole=.4,
                textinfo='label+percent'
            )])
            fig_pie.update_layout(template='plotly_white', height=450, showlegend=False)
            st.plotly_chart(fig_pie, use_container_width=True)

        with col_right:
            st.write(f"#### 📈 {selected_idx} 비중 변화 추이 (시계열)")
            # 상위 10개 통화만 추적 (가장 최근 비중 기준)
            top_10_names = current_weights.head(10).index.tolist()
            trend_df = clean_df.loc[top_10_names].T

            fig_trend = go.Figure()
            for country in top_10_names:
                fig_trend.add_trace(go.Scatter(
                    x=trend_df.index, y=trend_df[country],
                    mode='lines',
                    stackgroup='one', # 누적 영역 차트
                    name=country
                ))
            
            fig_trend.update_layout(
                template='plotly_white', 
                height=450,
                xaxis_title="Year",
                yaxis_title="Weight (%)",
                hovermode='x unified'
            )
            st.plotly_chart(fig_trend, use_container_width=True)

        st.divider()

        # --- 하단 분석 섹션: 의미 있는 변화 포착 ---
        st.write("### 🔍 통화 비중 변동 분석")
        
        # 시작 연도와 최신 연도 비교
        start_year = years[0]
        diff_df = ((clean_df[latest_year] - clean_df[start_year])).sort_values(ascending=False)
        
        c1, c2 = st.columns(2)
        with c1:
            st.success(f"✅ **비중이 가장 많이 늘어난 통화 ({start_year} → {latest_year})**")
            st.dataframe(diff_df.head(5).rename("비중 증가율(%)"))
        with c2:
            st.warning(f"⚠️ **비중이 가장 많이 줄어든 통화 ({start_year} → {latest_year})**")
            st.dataframe(diff_df.tail(5).sort_values().rename("비중 감소율(%)"))

        with st.expander("📄 연준 공식 원본 데이터 테이블 보기"):
            st.dataframe(clean_df, use_container_width=True)

import FinanceDataReader as fdr

# --- 탭 7: 금리 커브 (BOK API 적용 버전) ---
with tab7:
    st.subheader("📈 Treasury Yield Curve Analysis (US & KR)")
    st.caption("미국(FRED)과 한국(한국은행 ECOS)의 공식 데이터를 사용하여 분석합니다.")

    # 데이터 호출
    with st.spinner('금리 데이터를 불러오는 중...'):
        us_yields = get_yield_curve_us() # 기존 FRED 함수
        
        # 한국은행 데이터 호출
        kr3y = get_bok_yield_data('010200000', 'KR 3Y')
        kr10y = get_bok_yield_data('010210000', 'KR 10Y')
        kr_yields = pd.concat([kr3y, kr10y], axis=1).ffill()

    # --- 섹션 1: 현재 수익률 곡선 ---
    col_u, col_k = st.columns(2)

    with col_u:
        if not us_yields.empty:
            latest_us = us_yields.iloc[-1]
            fig_us = go.Figure(go.Scatter(x=latest_us.index, y=latest_us.values, mode='lines+markers', line=dict(color='royalblue', width=3)))
            fig_us.update_layout(title=f"US Yield Curve ({latest_us.name.date()})", template='plotly_white')
            st.plotly_chart(fig_us, use_container_width=True)

    with col_k:
        if not kr_yields.empty:
            latest_kr = kr_yields.iloc[-1]
            fig_kr = go.Figure(go.Scatter(x=latest_kr.index, y=latest_kr.values, mode='lines+markers', line=dict(color='firebrick', width=3)))
            fig_kr.update_layout(title=f"KR Yield Curve ({latest_kr.name.date()})", template='plotly_white')
            st.plotly_chart(fig_kr, use_container_width=True)

    st.divider()

    # --- 섹션 2: 장단기 금리차 ---
    if not us_yields.empty and not kr_yields.empty:
        st.write("### 2. Yield Spread Trend (10Y - Short Term)")
        us_spread = (us_yields['10Y'] - us_yields['2Y']).tail(days_to_show)
        kr_spread = (kr_yields['KR 10Y'] - kr_yields['KR 3Y']).tail(days_to_show)

        fig_spread = go.Figure()
        fig_spread.add_hline(y=0, line_dash="dash", line_color="black")
        fig_spread.add_trace(go.Scatter(x=us_spread.index, y=us_spread, name="US 10Y-2Y", line=dict(color='royalblue')))
        fig_spread.add_trace(go.Scatter(x=kr_spread.index, y=kr_spread, name="KR 10Y-3Y", line=dict(color='firebrick')))
        fig_spread.update_layout(template='plotly_white', hovermode='x unified')
        st.plotly_chart(fig_spread, use_container_width=True)
