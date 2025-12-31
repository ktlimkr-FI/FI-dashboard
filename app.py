import streamlit as st
import pandas as pd
import yfinance as yf
from fredapi import Fred
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
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

# --- 모바일 반응형 타이틀 CSS 설정 ---
st.markdown("""
    <style>
    /* 기본(PC) 타이틀 스타일 */
    h1 { font-size: 2.5rem !important; }
    h2 { font-size: 1.8rem !important; }
    h3 { font-size: 1.5rem !important; }

    /* 모바일 기기(화면 너비 768px 이하)일 때 적용 */
    @media (max-width: 768px) {
        h1 {
            font-size: 1.5rem !important; /* st.title 크기 축소 */
            line-height: 1.2;
        }
        h2 {
            font-size: 1.2rem !important; /* st.header 크기 축소 */
            line-height: 1.2;
        }
        h3 {
            font-size: 1.0rem !important; /* st.subheader 크기 축소 */
        }
        /* 탭 메뉴 글자 크기도 모바일에 맞게 조정 */
        .stTabs [data-baseweb="tab"] {
            font-size: 0.8rem !important;
            padding: 5px 10px !important;
        }
    }
    </style>
    """, unsafe_allow_html=True)

def apply_mobile_style(fig):
    """모든 Plotly 차트에 모바일 최적화 스타일(범례 하단 등)을 적용합니다."""
    fig.update_layout(
        legend=dict(
            orientation="h",
            yanchor="top",
            y=-0.25,
            xanchor="center",
            x=0.5,
            font=dict(size=10)
        ),
        margin=dict(l=10, r=10, t=50, b=80), # 좌우 여백 줄이고 하단 확보
        hovermode="x unified"
    )
    return fig

# 사용 예시:
# fig = go.Figure(...)
# st.plotly_chart(apply_mobile_style(fig), use_container_width=True)

# --- 4. 데이터 로드 함수들 (통합 및 최적화) ---

# [1] FRED 데이터 로더
@st.cache_data(ttl=3600)
def get_fred_data(series_id):
    try:
        data = fred.get_series(series_id)
        df = pd.DataFrame(data, columns=[series_id])
        df.index.name = 'date'
        return df
    except: return pd.DataFrame()

# [2] 미국 수익률 곡선 로더 (NameError 방지 위해 상단 배치)
@st.cache_data(ttl=3600)
def get_yield_curve_us():
    tickers = {'3M': 'DGS3MO', '2Y': 'DGS2', '5Y': 'DGS5', '10Y': 'DGS10', '30Y': 'DGS30'}
    frames = []
    for label, tid in tickers.items():
        df = get_fred_data(tid)
        if not df.empty:
            frames.append(df.rename(columns={tid: label}))
    return pd.concat(frames, axis=1).ffill() if frames else pd.DataFrame()

# [3] Yahoo Finance 환율 로더
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

# [4] OFR Repo Fails 로더
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

# [5] BoK 시장 금리 로더

@st.cache_data(ttl=3600)
def get_full_kr_yield_curve():
    # 탭 7에서 정의한 딕셔너리를 함수 내부에서 참조하거나 인자로 받아야 합니다.
    kr_maturities = {
        '1Y': '010190000', '2Y': '010200010', '3Y': '010200000', 
        '5Y': '010210000', '10Y': '010220000', '20Y': '010230000', 
        '30Y': '010240000', '50Y': '010250000'
    }
    
    all_frames = []
    # 프로그레스 바나 상태 메시지를 위해 st.spinner 사용 권장
    for label, code in kr_maturities.items():
        # 이전에 정의한 get_bok_data 함수를 호출합니다.
        df = get_bok_data('817Y002', 'D', code, label)
        if not df.empty:
            all_frames.append(df)
    
    if all_frames:
        # 모든 만기 데이터를 하나의 데이터프레임으로 합칩니다.
        combined = pd.concat(all_frames, axis=1).sort_index().ffill()
        return combined
    return pd.DataFrame()

# [5] 한국은행(BOK) 범용 데이터 로더 (매크로 지표 대응용 수정)
@st.cache_data(ttl=3600)
def get_bok_data(stat_code, cycle, item_code, column_name):
    """
    stat_code: 통계표코드 (예: 817Y002)
    cycle: 주기 (D: 일, M: 월, Q: 분기, Y: 년)
    item_code: 항목코드 (예: 010200000)
    """
    # 충분한 조회를 위해 시작일을 10년 전으로 설정
    start_date = (datetime.now() - pd.Timedelta(days=4000)).strftime('%Y%m%d')
    end_date = datetime.now().strftime('%Y%m%d')
    
    url = f"http://ecos.bok.or.kr/api/StatisticSearch/{BOK_API_KEY}/json/kr/1/10000/{stat_code}/{cycle}/{start_date}/{end_date}/{item_code}"
    
    try:
        resp = requests.get(url)
        data = resp.json()
        if 'StatisticSearch' in data:
            rows = data['StatisticSearch']['row']
            df = pd.DataFrame(rows)
            # 주기(cycle)에 따라 날짜 처리 방식 변경
            if cycle == 'D':
                df['date'] = pd.to_datetime(df['TIME'])
            else: # 월간/분기 데이터 처리 (예: 202401 -> 2024-01-01)
                df['date'] = pd.to_datetime(df['TIME'].str[:4] + "-" + df['TIME'].str[4:6] + "-01")
            
            df['value'] = pd.to_numeric(df['DATA_VALUE'])
            return df[['date', 'value']].rename(columns={'value': column_name}).set_index('date')
    except Exception as e:
        # 로그에만 기록하고 사용자 화면엔 경고만 표시
        pass
    return pd.DataFrame()

# 5. 탭 구성
tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs([
    "📊 Repo 흐름", "💸 금리 분석", "🌐 유동성&달러", "💹 환율(Yahoo)", "⚠️ Repo Fails (OFR)", "⚠️ Dollar Index Weight", "⚠️ Yield Curve(US&KR)", "⚠️ 한미 기준금리 역전 분석"
])

# --- 탭 1: Repo 흐름 & SOFR Spread 분석 ---
with tab1:
    st.subheader("1. Overnight Repo Flow (RPONTTLD)")
    repo_df = get_fred_data('RPONTTLD').tail(days_to_show).dropna()
    if not repo_df.empty:
        fig1 = go.Figure()
        fig1.add_trace(go.Scatter(x=repo_df.index, y=repo_df['RPONTTLD'], mode='lines', fill='tozeroy', line=dict(color='royalblue', width=2)))
        fig1.update_layout(title="Daily Repo Volume Trend", template='plotly_white', height=350)
        st.plotly_chart(apply_mobile_style(fig1), use_container_width=True)

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
        st.plotly_chart(apply_mobile_style(fig_spread), use_container_width=True)

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
    st.plotly_chart(apply_mobile_style(fig_season), use_container_width=True)

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
        st.plotly_chart(apply_mobile_style(fig2), use_container_width=True)

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
        
        st.plotly_chart(apply_mobile_style(fig_diff), use_container_width=True)
        
        st.success("""
        💡 **분석 팁:**
        * **SOFR - Midpoint가 0 위로 크게 튈 때:** 연준의 의도보다 시장의 실제 자금 사정이 빡빡함을 의미합니다.
        * **SOFR99th - Midpoint:** 시장 내에서 가장 비싸게 돈을 빌리는 주체가 연준의 가이드라인에서 얼마나 멀어져 있는지를 보여줍니다. 이 수치가 급증하면 시스템 리스크 신호로 해석될 수 있습니다.
        """)
        
# --- 탭 3: 유동성&달러 (데이터 정합성 강화 버전) ---
with tab3:
    st.subheader("🌐 Global Dollar Strength Analysis")
    
    # 1. 지표 선택 체크박스
    c1, c2, c3, c4 = st.columns(4)
    with c1: show_obfr = st.checkbox("OBFR Volume", value=True)
    with c2: show_broad = st.checkbox("Broad Index", value=True)
    with c3: show_afe = st.checkbox("AFE Index", value=False)
    with c4: show_eme = st.checkbox("EME Index", value=False)

    # 2. 데이터 로드
    with st.spinner('데이터를 통합하는 중...'):
        d3_indices = pd.concat([
            get_fred_data('OBFRVOL'), get_fred_data('DTWEXBGS'), 
            get_fred_data('DTWEXAFEGS'), get_fred_data('DTWEXEMEGS')
        ], axis=1)
        
        yf_fx = get_yfinance_data()
        
        # 두 데이터를 합치고 시차를 고려해 ffill()만 수행 (dropna()는 나중에)
        combined_df = pd.concat([d3_indices, yf_fx], axis=1).sort_index().ffill()
        
        # 선택한 기간만큼 자르기
        d3 = combined_df.tail(days_to_show)

    # 3. 데이터 로드 실패 시 디버깅 정보 표시
    if d3.empty:
        st.error("⚠️ 결합된 데이터가 비어 있습니다. 소스 데이터를 확인하세요.")
        col1, col2 = st.columns(2)
        with col1: st.write("FRED 데이터 상태:", "성공" if not d3_indices.empty else "실패")
        with col2: st.write("Yahoo Finance 데이터 상태:", "성공" if not yf_fx.empty else "실패")
    else:
        # 4. 차트 섹션
        fig3 = make_subplots(specs=[[{"secondary_y": True}]])
        
        if show_obfr and 'OBFRVOL' in d3.columns:
            fig3.add_trace(go.Scatter(x=d3.index, y=d3['OBFRVOL'], name="OBFR Vol (Left)", 
                                     line=dict(color='rgba(150, 150, 150, 0.5)', width=1.5), fill='tozeroy'), secondary_y=False)
        
        if show_broad and 'DTWEXBGS' in d3.columns:
            fig3.add_trace(go.Scatter(x=d3.index, y=d3['DTWEXBGS'], name="Broad Index (Right)", line=dict(color='royalblue', width=2.5)), secondary_y=True)
        
        if show_afe and 'DTWEXAFEGS' in d3.columns:
            fig3.add_trace(go.Scatter(x=d3.index, y=d3['DTWEXAFEGS'], name="AFE Index (Right)", line=dict(color='green', width=1.5)), secondary_y=True)
        
        if show_eme and 'DTWEXEMEGS' in d3.columns:
            fig3.add_trace(go.Scatter(x=d3.index, y=d3['DTWEXEMEGS'], name="EME Index (Right)", line=dict(color='firebrick', width=1.5)), secondary_y=True)

        fig3.update_layout(template='plotly_white', hovermode='x unified', height=400,
                          legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
        st.plotly_chart(apply_mobile_style(fig3), use_container_width=True)

        st.divider()

        # 5. 변화율 분석 테이블
        st.write("### 📈 달러 기준 기간별 변화율 (%)")
        
        def calc_roc(df):
            intervals = {'1D': 1, '1W': 5, '1M': 21, '3M': 63, '6M': 126, '1Y': 252}
            assets = ['DTWEXBGS', 'DTWEXAFEGS', 'DTWEXEMEGS', 'USD/KRW', 'USD/JPY', 'USD/EUR', 'USD/CNY', 'USD/MXN']
            
            roc_results = []
            # 유효한 마지막 데이터 가져오기 (가장 최근 행)
            current_vals = df.iloc[-1]
            
            for asset in assets:
                if asset in df.columns and not pd.isna(current_vals[asset]):
                    row = {'Asset': asset}
                    for label, days in intervals.items():
                        if len(df) > days:
                            # 시차를 고려하여 NaN이 아닌 유효한 과거 값 찾기
                            prev_val = df[asset].iloc[-(days + 1)]
                            if not pd.isna(prev_val) and prev_val != 0:
                                change = ((current_vals[asset] / prev_val) - 1) * 100
                                row[label] = round(change, 2)
                            else:
                                row[label] = None
                        else:
                            row[label] = None
                    roc_results.append(row)
            return pd.DataFrame(roc_results).set_index('Asset')

        roc_df = calc_roc(combined_df)

        if not roc_df.empty:
            def color_map(val):
                if val is None or pd.isna(val): return ''
                color = '#EF553B' if val > 0 else '#636EFA' # Plotly 표준 빨강/파랑
                return f'color: {color}; font-weight: bold'

            st.dataframe(
                roc_df.style.applymap(color_map)
                           .format("{:+.2f}%", na_rep="-"),
                use_container_width=True
            )
        else:
            st.info("변화율을 계산할 수 있는 최신 데이터가 부족합니다.")
            
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
    st.plotly_chart(apply_mobile_style(fig4), use_container_width=True)
    
    st.write("### 개별 상세 차트 (절대 가격)")
    cols = st.columns(2)
    for i, s in enumerate(selected_symbols):
        with cols[i % 2]:
            fig_i = go.Figure(go.Scatter(x=yf_raw.index, y=yf_raw[s], name=s, line=dict(color='royalblue')))
            fig_i.update_layout(title=s, height=250, margin=dict(l=0,r=0,t=30,b=0), template='plotly_white')
            st.plotly_chart(apply_mobile_style(fig_i), use_container_width=True)

# --- 탭 5: Repo Fails (조회 기간 연동 및 계절성 분석) ---
with tab5:
    st.subheader("Primary Dealer Repo Fails Analysis")
    with st.spinner('OFR 데이터를 분석 중...'):
        fails_all = get_ofr_fails_data().ffill()
        # [연동] 사이드바 설정 기간만큼만 필터링
        fails_display = fails_all.tail(days_to_show)

    if not fails_all.empty:
        # 1. 상단 섹션: 필터링된 기간의 데이터 시각화
        col1, col2 = st.columns(2)
        with col1:
            st.write(f"### 항목별 누적 Fails ({selected_label})")
            fig_stacked = go.Figure()
            for col in fails_display.columns:
                fig_stacked.add_trace(go.Scatter(
                    x=fails_display.index, y=fails_display[col], 
                    mode='lines', stackgroup='one', name=col
                ))
            fig_stacked.update_layout(template='plotly_white', height=400, yaxis_title="$M", hovermode='x unified')
            st.plotly_chart(apply_mobile_style(fig_stacked), use_container_width=True)
            
        with col2:
            st.write(f"### UST Fails ({selected_label})")
            fig_ust = go.Figure()
            fig_ust.add_trace(go.Scatter(
                x=fails_display.index, y=fails_display['UST fails to deliver'], 
                fill='tozeroy', line=dict(color='firebrick'), name="UST Fails"
            ))
            fig_ust.update_layout(template='plotly_white', height=400, yaxis_title="$M", hovermode='x unified')
            st.plotly_chart(apply_mobile_style(fig_ust), use_container_width=True)

        st.divider()

        # 2. 하단 섹션: 계절성 분석 (계산은 전체 데이터, 시계열 차트는 필터링 연동)
        st.write("## 🗓️ Repo Fails 계절성 분석 (UST Fails 기준)")
        st.info("💡 **안내:** 계절성 패턴(막대 차트)은 10년 전체 데이터를 기반으로 산출된 구조적 지표입니다.")

        # [계산] 추세 제거는 전체 데이터(fails_all)를 사용하여 52주 평균의 정확도를 확보
        ust_fails = fails_all[['UST fails to deliver']].copy()
        ust_fails['Trend'] = ust_fails['UST fails to deliver'].rolling(window=52, center=True).mean()
        ust_fails['Detrended'] = ust_fails['UST fails to deliver'] - ust_fails['Trend']
        ust_fails['Week'] = ust_fails.index.isocalendar().week
        
        # 주간 평균 패턴 (이 부분은 '시계열 분석'의 핵심으로 전체 기간 유지)
        seasonal_pattern = ust_fails.groupby('Week')['Detrended'].mean().reset_index()

        # [연동] 추세 제거 데이터 차트만 현재 조회 기간으로 슬라이싱
        ust_fails_display = ust_fails.tail(days_to_show)

        c1, c2 = st.columns(2)
        
        with c1:
            st.write(f"### 1. 추세 제거 데이터 (Detrended - {selected_label})")
            fig_detrended = go.Figure()
            
            # 매년 9월~12월 음영 추가
            years = ust_fails_display.index.year.unique()
            for year in years:
                fig_detrended.add_vrect(
                    x0=f"{year}-09-01", x1=f"{year}-12-31",
                    fillcolor="rgba(128, 128, 128, 0.2)", opacity=0.3,
                    layer="below", line_width=0,
                )
            
            # [연동된 데이터 사용]
            fig_detrended.add_trace(go.Scatter(
                x=ust_fails_display.index, y=ust_fails_display['Detrended'], 
                line=dict(color='purple', width=1.5), name="Detrended"
            ))
            fig_detrended.add_hline(y=0, line_dash="dash", line_color="grey")
            fig_detrended.update_layout(template='plotly_white', height=400, showlegend=False)
            st.plotly_chart(apply_mobile_style(fig_detrended), use_container_width=True)

        with c2:
            st.write("### 2. 10년 주간 평균 계절성 (전체 기간)")
            fig_seasonal = go.Figure()
            
            # 주간 차트 음영 (36주~52주)
            fig_seasonal.add_vrect(
                x0=35.5, x1=52.5,
                fillcolor="rgba(128, 128, 128, 0.2)", opacity=0.3,
                layer="below", line_width=0,
                annotation_text="Sep-Dec", annotation_position="top left"
            )
            
            # [전체 패턴 사용]
            fig_seasonal.add_trace(go.Bar(
                x=seasonal_pattern['Week'], 
                y=seasonal_pattern['Detrended'], 
                marker_color='orange',
                name="Avg Deviation"
            ))
            
            fig_seasonal.update_layout(
                template='plotly_white', height=400, 
                xaxis_title="주차 (Week)", yaxis_title="편차",
                showlegend=False
            )
            st.plotly_chart(apply_mobile_style(fig_seasonal), use_container_width=True)

        st.success("💡 **분석 가이드:** 음영 구역(연말) 내에서 '편차'가 플러스로 튀는 현상은 해당 시기에 정기적으로 결제 실패가 급증함을 의미합니다.")

from streamlit_gsheets import GSheetsConnection

# --- 탭 6: Fed 달러 인덱스 비중 분석 (전체 기간 옵션 추가) ---
with tab6:
    st.subheader("📊 Fed Dollar Index: Weights vs Price Analysis")
    
    sheet_url = "https://docs.google.com/spreadsheets/d/1rOh_s5JeKw_mP98u2URa8OO-xBgSdAHn73qqjnI95rs/export?format=csv"
    
    try:
        @st.cache_data(ttl=3600)
        def load_gsheet_data(url):
            return pd.read_csv(url)

        df_raw = load_gsheet_data(sheet_url)
        
        # 1. TOTAL 행 제거 및 데이터 정제
        df_raw = df_raw.rename(columns={df_raw.columns[0]: 'Currency'})
        df_raw = df_raw[~df_raw['Currency'].str.upper().str.contains('TOTAL', na=False)].copy()
        
        year_cols = [c for c in df_raw.columns if str(c).isdigit() or (isinstance(c, str) and c.startswith('20'))]
        year_cols = sorted(year_cols)
        
        df_raw['Is_AFE'] = df_raw['Currency'].str.startswith('*')
        df_raw['Clean_Name'] = df_raw['Currency'].str.replace('*', '', regex=False)

        # --- [신규] 기간 제어 옵션 ---
        st.write("#### 🗓️ 데이터 조회 범위 설정")
        col_opt1, col_opt2 = st.columns([1, 2])
        with col_opt1:
            # 체크박스로 전체 기간 보기 활성화
            show_full_history = st.checkbox("전체 역사 보기 (2006~)", value=False)
        
        # 2. FRED 가격 데이터 로드 및 기간 필터링
        with st.spinner('달러 인덱스 가격 데이터를 로드 중...'):
            dxy_price_raw = get_fred_data('DTWEXBGS')
            
            # [연동 로직 수정]
            if show_full_history:
                dxy_price = dxy_price_raw # 전체 데이터 사용
                display_label = "전체 기간 (2006~)"
            else:
                dxy_price = dxy_price_raw.tail(days_to_show) # 사이드바 연동
                display_label = f"최근 {days_to_show}일"

        # 3. [상관관계 분석 섹션]
        st.write(f"### 📈 1. 가격-비중 상관관계 시각화 ({display_label})")
        latest_yr = year_cols[-1]
        
        if not dxy_price.empty:
            sorted_currencies = df_raw.sort_values(by=latest_yr, ascending=False)['Clean_Name'].tolist()
            selected_currency = st.selectbox("비교 분석할 통화 선택", sorted_currencies)
            
            curr_row = df_raw[df_raw['Clean_Name'] == selected_currency].iloc[0]
            
            # 현재 화면에 보이는 가격 데이터의 시작 연도에 맞춰 비중 데이터 필터링
            min_visible_year = dxy_price.index.min().year
            visible_year_cols = [y for y in year_cols if int(y) >= min_visible_year]
            
            weights_series = curr_row[visible_year_cols].astype(float)
            
            fig_corr = make_subplots(specs=[[{"secondary_y": True}]])
            fig_corr.add_trace(go.Scatter(x=dxy_price.index, y=dxy_price['DTWEXBGS'], 
                                         name="Broad Index Price", line=dict(color='royalblue', width=2)), secondary_y=False)
            
            weight_dates = [pd.to_datetime(f"{y}-01-01") for y in visible_year_cols]
            fig_corr.add_trace(go.Bar(x=weight_dates, y=weights_series.values, 
                                     name=f"{selected_currency} Weight (%)", marker_color='orange', opacity=0.4), secondary_y=True)
            
            fig_corr.update_layout(template='plotly_white', height=500, hovermode='x unified',
                                  xaxis_range=[dxy_price.index.min(), dxy_price.index.max()])
            st.plotly_chart(apply_mobile_style(fig_corr), use_container_width=True)

        st.divider()

        # 4. [그룹별 비중 분석 섹션]
        st.write(f"### 🔍 2. 그룹별 비중 분석 ({display_label})")
        idx_choice = st.radio("분석할 그룹 선택", ["Broad (전체)", "AFE (선진국)", "EME (신흥국)"], horizontal=True)

        target_df = df_raw.copy()
        if idx_choice == "AFE (선진국)":
            target_df = df_raw[df_raw['Is_AFE'] == True].copy()
        elif idx_choice == "EME (신흥국)":
            target_df = df_raw[df_raw['Is_AFE'] == False].copy()

        # 정규화
        for col in visible_year_cols:
            col_sum = target_df[col].sum()
            if col_sum > 0:
                target_df[col] = (target_df[col] / col_sum) * 100

        c1, c2 = st.columns([1, 1.5])
        with c1:
            st.write(f"#### 🥧 {idx_choice} 최신 구성 ({latest_yr}년)")
            pie_data = target_df[['Clean_Name', latest_yr]].sort_values(by=latest_yr, ascending=False)
            display_text = [f"<b>{name}</b>" if i < 5 else "" for i, name in enumerate(pie_data['Clean_Name'])]
            fig_pie = go.Figure(data=[go.Pie(labels=pie_data['Clean_Name'], values=pie_data[latest_yr], hole=.4,
                                            text=display_text, textinfo='text+percent', textposition='outside', automargin=True)])
            fig_pie.update_layout(height=550, showlegend=False)
            st.plotly_chart(apply_mobile_style(fig_pie), use_container_width=True)

        with c2:
            st.write(f"#### 📈 {idx_choice} 비중 추이")
            trend_df = target_df.set_index('Clean_Name')[visible_year_cols].T.sort_index()
            fig_trend = go.Figure()
            for curr in pie_data.head(10)['Clean_Name'].tolist():
                fig_trend.add_trace(go.Scatter(x=trend_df.index, y=trend_df[curr], mode='lines', stackgroup='one', name=curr))
            fig_trend.update_layout(height=450, yaxis_title="Weight (%)")
            st.plotly_chart(apply_mobile_style(fig_trend), use_container_width=True)

        # 5. AFE vs EME 그룹 합산 분석
        st.divider()
        st.write(f"### 🌐 3. AFE(선진국) vs EME(신흥국) 그룹 합산 분석 ({display_label})")
        
        group_trend = df_raw.groupby('Is_AFE')[visible_year_cols].sum().T
        # 컬럼 인덱스(True/False)를 이름으로 변환
        group_trend.columns = [('Advanced (AFE)' if c else 'Emerging (EME)') for c in group_trend.columns]
        group_trend = group_trend.sort_index()

        latest_group_val = group_trend.iloc[-1]
        c1_sub, c2_sub = st.columns([1, 1.5])

        with c1_sub:
            st.write(f"#### 🥧 그룹별 현재 비중 ({latest_yr}년)")
            fig_group_pie = go.Figure(data=[go.Pie(labels=latest_group_val.index, values=latest_group_val.values, hole=.4,
                                                 marker_colors=['#636EFA', '#EF553B'], textinfo='label+percent', textposition='outside')])
            fig_group_pie.update_layout(height=400, showlegend=False)
            st.plotly_chart(apply_mobile_style(fig_group_pie), use_container_width=True)

        with c2_sub:
            st.write("#### 📈 그룹별 비중 시계열 추이")
            fig_group_trend = go.Figure()
            # AFE(파랑), EME(빨강) 색상 고정 출력
            color_map = {'Advanced (AFE)': '#636EFA', 'Emerging (EME)': '#EF553B'}
            for col in group_trend.columns:
                color = color_map.get(col, '#333333')
                fig_group_trend.add_trace(go.Scatter(
                    x=group_trend.index, y=group_trend[col], name=col,
                    mode='lines', stackgroup='one', line=dict(color=color, width=0.5),
                    fillcolor=f'rgba{tuple(list(int(color.lstrip("#")[i:i+2], 16) for i in (0, 2, 4)) + [0.5])}'
                ))
            fig_group_trend.update_layout(template='plotly_white', height=400, yaxis_title="Weight (%)", hovermode='x unified')
            st.plotly_chart(apply_mobile_style(fig_group_trend), use_container_width=True)

    except Exception as e:
        st.error(f"데이터 로드 및 분석 실패: {e}")

# --- 탭 7: KR/US Yield Curve & Spread Matrix (통합 분석 버전) ---
with tab7:
    st.subheader("🏛️ Yield Curve & Spread Matrix")
    st.info("💡 **2Y 데이터 안내:** 2021년 3월 이전은 **통안증권 2년**, 이후는 **국고채 2년** 데이터를 결합하여 분석합니다.")

    # [함수] 데이터 수집 및 하이브리드 병합
    @st.cache_data(ttl=3600)
    def fetch_final_yield_data():
        api_key_bok = st.secrets.get("BOK_API_KEY")
        # 주요 만기 코드 정의
        kr_codes = {'1Y':'010190000','3Y':'010200000','5Y':'010210000','10Y':'010220000','20Y':'010230000','30Y':'010240000'}
        us_codes = {'1Y':'DGS1','2Y':'DGS2','3Y':'DGS3','5Y':'DGS5','10Y':'DGS10','20Y':'DGS20','30Y':'DGS30'}
        
        end_date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
        start_date = "20100101" # 2Y 하이브리드를 위해 충분한 과거 데이터 확보
        
        results_kr = []

        # 1. KR 2Y 하이브리드 (KTB + MSB)
        # 컬럼명을 미리 '2Y'로 통일하여 가져오는 것이 KeyError 방지의 핵심입니다.
        df_ktb2 = get_bok_data('817Y002', 'D', '010200010', '2Y')
        df_msb2 = get_bok_data('817Y002', 'D', '010400002', '2Y')
        
        switch_date = None
        if not df_ktb2.empty:
            switch_date = df_ktb2.index.min() # 국고채 시작 시점

        # 병합: 국고채 우선, 빈 곳은 통안채로 보강
        if not df_ktb2.empty or not df_msb2.empty:
            df_2y = df_ktb2.combine_first(df_msb2) if not df_ktb2.empty and not df_msb2.empty else (df_ktb2 if not df_ktb2.empty else df_msb2)
            results_kr.append(df_2y)

        # 2. KR 기타 만기 및 기준금리
        for label, code in kr_codes.items():
            df = get_bok_data('817Y002', 'D', code, label)
            if not df.empty: results_kr.append(df)
        
        df_base = get_bok_data('722Y001', 'D', '0101000', 'KR_BaseRate')
        if not df_base.empty: results_kr.append(df_base)

        final_kr = pd.concat(results_kr, axis=1).sort_index().ffill()

        # 3. US 데이터 (FRED)
        results_us = []
        for label, code in us_codes.items():
            df_u = get_fred_data(code).rename(columns={code: label})
            results_us.append(df_u)
        final_us = pd.concat(results_us, axis=1).sort_index().ffill()
        
        return final_kr, final_us, switch_date

    # 데이터 실행
    with st.spinner('한/미 금리 매트릭스 수집 중...'):
        df_kr, df_us, switch_date = fetch_final_yield_data()

    if not df_kr.empty:
        # --- [섹션 1] Yield Curve Shape (시점별/국가별 비교) ---
        st.write("### 📉 1. Yield Curve Dynamics")
        latest_date = df_kr.index[-1]
        
        # 6개 시점 오프셋 설정
        offsets = {
            'Current': latest_date,
            '1W Ago': df_kr.index[df_kr.index <= (latest_date - timedelta(weeks=1))][-1],
            '1M Ago': df_kr.index[df_kr.index <= (latest_date - pd.DateOffset(months=1))][-1],
            '3M Ago': df_kr.index[df_kr.index <= (latest_date - pd.DateOffset(months=3))][-1],
            '6M Ago': df_kr.index[df_kr.index <= (latest_date - pd.DateOffset(months=6))][-1],
            '1Y Ago': df_kr.index[df_kr.index <= (latest_date - pd.DateOffset(years=1))][-1]
        }
        
        x_mats = ['1Y', '2Y', '3Y', '5Y', '10Y', '20Y', '30Y']
        
        col_c1, col_c2 = st.columns(2)
        
        with col_c1:
            st.write("#### 🇰🇷 KR Curve History")
            fig_kr = go.Figure()
            colors = ['#B22222', '#FF8C00', '#228B22', '#4169E1', '#9932CC', '#808080']
            for (name, date), color in zip(offsets.items(), colors):
                valid_y = [df_kr.loc[date, m] for m in x_mats if m in df_kr.columns]
                valid_x = [m for m in x_mats if m in df_kr.columns]
                fig_kr.add_trace(go.Scatter(x=valid_x, y=valid_y, name=name, 
                                           line=dict(color=color, width=3 if name=='Current' else 1.5,
                                                    dash='solid' if name=='Current' else 'dot')))
            st.plotly_chart(apply_mobile_style(fig_kr), use_container_width=True)

        with col_c2:
            st.write("#### 🇺🇸 vs 🇰🇷 Comparison")
            fig_comp = go.Figure()
            # KR
            fig_comp.add_trace(go.Scatter(x=x_mats, y=[df_kr.loc[latest_date, m] for m in x_mats if m in df_kr.columns], 
                                        name="KR Treasury", line=dict(color='#B22222', width=3)))
            # US
            us_latest = df_us.index[-1]
            fig_comp.add_trace(go.Scatter(x=x_mats, y=[df_us.loc[us_latest, m] for m in x_mats if m in df_us.columns], 
                                        name="US Treasury", line=dict(color='#4169E1', width=3)))
            st.plotly_chart(apply_mobile_style(fig_comp), use_container_width=True)

        st.divider()

        # --- [섹션 2] Yield Spread Analysis (시계열 분석) ---
        st.write("### 📈 2. Yield Spread Matrix")
        
        t1, t2, t3 = st.tabs(["구간별 선택", "기준금리 대비", "중요 구간(1/3, 2/5)"])
        
        with t1:
            st.write("#### 자유 구간 스프레드")
            sc1, sc2 = st.columns(2)
            long_term = sc1.selectbox("장기물", x_mats, index=4) # 10Y
            short_term = sc2.selectbox("단기물", x_mats, index=1) # 2Y
            
            if long_term in df_kr.columns and short_term in df_kr.columns:
                spread_val = (df_kr[long_term] - df_kr[short_term]).tail(days_to_show)
                fig_s1 = go.Figure()
                fig_s1.add_hline(y=0, line_color="black")
                fig_s1.add_trace(go.Scatter(x=spread_val.index, y=spread_val, fill='tozeroy', name=f"{long_term}-{short_term}"))
                st.plotly_chart(apply_mobile_style(fig_s1), use_container_width=True)

        with t2:
            st.write("#### 국채 금리 vs 기준금리")
            if 'KR_BaseRate' in df_kr.columns:
                target_m = st.selectbox("비교 만기", x_mats, index=2) # 3Y
                b_spread = (df_kr[target_m] - df_kr['KR_BaseRate']).tail(days_to_show)
                fig_s2 = go.Figure()
                fig_s2.add_hline(y=0, line_color="black")
                fig_s2.add_trace(go.Scatter(x=b_spread.index, y=b_spread, name=f"{target_m}-BaseRate", line=dict(color='darkgreen')))
                st.plotly_chart(apply_mobile_style(fig_s2), use_container_width=True)

        with t3:
            st.write("#### 주요 모니터링 구간")
            fig_s3 = go.Figure()
            if '3Y' in df_kr.columns and '1Y' in df_kr.columns:
                s13 = (df_kr['3Y'] - df_kr['1Y']).tail(days_to_show)
                fig_s3.add_trace(go.Scatter(x=s13.index, y=s13, name="3Y-1Y (Policy Sensitive)"))
            if '5Y' in df_kr.columns and '2Y' in df_kr.columns:
                s25 = (df_kr['5Y'] - df_kr['2Y']).tail(days_to_show)
                fig_s3.add_trace(go.Scatter(x=s25.index, y=s25, name="5Y-2Y (Mid-term Outlook)"))
            
            fig_s3.update_layout(yaxis_title="Spread (%)")
            st.plotly_chart(apply_mobile_style(fig_s3), use_container_width=True)

        # 2Y 전환점 시각화 (선택 사항)
        with st.expander("🔍 2Y 하이브리드 데이터 상세 확인"):
            fig_2y_check = go.Figure()
            fig_2y_check.add_trace(go.Scatter(x=df_kr.index, y=df_kr['2Y'], name="2Y Hybrid Series"))
            if switch_date:
                fig_2y_check.add_vline(x=switch_date, line_dash="dash", line_color="red", annotation_text="MSB → KTB 전환")
            st.plotly_chart(apply_mobile_style(fig_2y_check), use_container_width=True)

    else:
        st.error("데이터 로드 실패. API 키와 호출 한도를 확인하세요.")
        
# --- 탭 8: Macro Indicators (한-미 기준금리 역전 분석) ---
with tab8:
    st.subheader("🌐 Central Bank Policy Rates (US vs KR)")
    st.caption("한국은행과 연준의 기준금리 격차는 환율과 자본 흐름의 핵심 변수입니다.")

    with st.spinner('매크로 데이터를 분석 중...'):
        # 한국 기준금리 (722Y001: 기준금리, 0101000: 한국은행 기준금리)
        bok_policy = get_bok_data('722Y001', 'D', '0101000', 'BOK Rate')
        
        # 미국 기준금리 (FRED: FEDFUNDS)
        fed_policy = get_fred_data('FEDFUNDS')
        
        if not bok_policy.empty and not fed_policy.empty:
            policy_df = pd.concat([bok_policy, fed_policy.rename(columns={'FEDFUNDS': 'Fed Rate'})], axis=1).ffill()
            policy_df['Spread'] = policy_df['BOK Rate'] - policy_df['Fed Rate']
            policy_df = policy_df.tail(days_to_show)

            # 차트 1: 기준금리 비교 선 차트
            fig_policy = go.Figure()
            fig_policy.add_trace(go.Scatter(x=policy_df.index, y=policy_df['BOK Rate'], name="BOK Rate", line=dict(color='firebrick', width=3)))
            fig_policy.add_trace(go.Scatter(x=policy_df.index, y=policy_df['Fed Rate'], name="Fed Rate", line=dict(color='royalblue', width=3, dash='dash')))
            fig_policy.update_layout(title="BOK vs Fed Policy Rate Trend", template='plotly_white', hovermode='x unified')
            st.plotly_chart(apply_mobile_style(fig_policy), use_container_width=True)

            # 차트 2: 금리차 (Spread) 막대 차트
            fig_p_spread = go.Figure()
            fig_p_spread.add_hline(y=0, line_dash="solid", line_color="black")
            # 금리 역전(음수)일 때 빨간색으로 표시
            colors = ['#EF553B' if x < 0 else '#636EFA' for x in policy_df['Spread']]
            fig_p_spread.add_trace(go.Bar(x=policy_df.index, y=policy_df['Spread'], marker_color=colors, name="Spread (KR-US)"))
            fig_p_spread.update_layout(title="Interest Rate Differential (KR - US)", template='plotly_white', yaxis_title="Basis Points / %")
            st.plotly_chart(apply_mobile_style(fig_p_spread), use_container_width=True)
        else:
            st.warning("정책 금리 데이터를 불러올 수 없습니다. API 설정을 확인하세요.")
