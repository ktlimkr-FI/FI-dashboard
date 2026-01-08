import streamlit as st
import pandas as pd
import yfinance as yf
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import pytz
import json
import gspread
from google.oauth2.service_account import Credentials

# 1. 페이지 설정
st.set_page_config(page_title="Global Financial Dashboard", layout="wide")
st.title("🏦 Comprehensive Financial Market Dashboard (Sheet Data)")

# 업데이트 시각 표시
kst = pytz.timezone('Asia/Seoul')
now_kst = datetime.now(kst).strftime('%Y-%m-%d %H:%M:%S')
st.info(f"🕒 **데이터 조회 시각 (KST): {now_kst}** (구글 시트 기반)")

# 2. Google Sheet 데이터 로드 설정
# secrets.toml에 GSHEET_ID와 GOOGLE_SERVICE_ACCOUNT_JSON이 있어야 합니다.
if "GSHEET_ID" not in st.secrets or "GOOGLE_SERVICE_ACCOUNT_JSON" not in st.secrets:
    st.error("⚠️ secrets.toml에 GSHEET_ID와 GOOGLE_SERVICE_ACCOUNT_JSON 설정이 필요합니다.")
    st.stop()

@st.cache_data(ttl=600)  # 10분 캐시 (시트 API 호출 최소화)
def load_all_sheet_data():
    # Credentials 설정
    json_str = st.secrets["GOOGLE_SERVICE_ACCOUNT_JSON"]
    creds_dict = json.loads(json_str)
    creds = Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
    )
    client = gspread.authorize(creds)
    sh = client.open_by_key(st.secrets["GSHEET_ID"])

    # 1. Daily Data
    ws_daily = sh.worksheet("data-daily")
    df_daily = pd.DataFrame(ws_daily.get_all_records())
    if not df_daily.empty:
        df_daily['Date'] = pd.to_datetime(df_daily['Date'])
        df_daily = df_daily.set_index('Date').sort_index()
        # 숫자형 변환 (빈 문자열 등 에러 방지)
        for col in df_daily.columns:
            df_daily[col] = pd.to_numeric(df_daily[col], errors='coerce')

    # 2. Weekly Data
    ws_weekly = sh.worksheet("data-weekly")
    df_weekly = pd.DataFrame(ws_weekly.get_all_records())
    if not df_weekly.empty:
        df_weekly['Date'] = pd.to_datetime(df_weekly['Date'])
        df_weekly = df_weekly.set_index('Date').sort_index()
        for col in df_weekly.columns:
            df_weekly[col] = pd.to_numeric(df_weekly[col], errors='coerce')

    # 3. Monthly Data
    ws_monthly = sh.worksheet("data-monthly")
    df_monthly = pd.DataFrame(ws_monthly.get_all_records())
    if not df_monthly.empty:
        df_monthly['Date'] = pd.to_datetime(df_monthly['Date'])
        df_monthly = df_monthly.set_index('Date').sort_index()
        for col in df_monthly.columns:
            df_monthly[col] = pd.to_numeric(df_monthly[col], errors='coerce')

    # 4. Quarterly Data
    ws_quarterly = sh.worksheet("data-quarterly")
    df_quarterly = pd.DataFrame(ws_quarterly.get_all_records())
    if not df_quarterly.empty:
        df_quarterly['Date'] = pd.to_datetime(df_quarterly['Date'])
        df_quarterly = df_quarterly.set_index('Date').sort_index()
        for col in df_quarterly.columns:
            df_quarterly[col] = pd.to_numeric(df_quarterly[col], errors='coerce')

    return df_daily, df_weekly, df_monthly, df_quarterly

# 데이터 로드 실행
try:
    with st.spinner("구글 시트에서 데이터를 불러오는 중..."):
        df_daily, df_weekly, df_monthly, df_quarterly = load_all_sheet_data()
    st.success("✅ 데이터 로드 완료")
except Exception as e:
    st.error(f"데이터 로드 실패: {e}")
    st.stop()

# 3. 사이드바 설정
st.sidebar.header("📅 조회 기간 설정")
period_options = {"6개월": 180, "1년": 365, "3년": 1095, "5년": 1825, "10년": 3650}
selected_label = st.sidebar.selectbox("기간 선택", options=list(period_options.keys()), index=1)
days_to_show = period_options[selected_label]

# --- 모바일 반응형 스타일 ---
st.markdown("""
    <style>
    h1 { font-size: 2.5rem !important; }
    h2 { font-size: 1.8rem !important; }
    h3 { font-size: 1.5rem !important; }
    @media (max-width: 768px) {
        h1 { font-size: 1.5rem !important; line-height: 1.2; }
        h2 { font-size: 1.2rem !important; line-height: 1.2; }
        h3 { font-size: 1.0rem !important; }
        .stTabs [data-baseweb="tab"] { font-size: 0.8rem !important; padding: 5px 10px !important; }
    }
    </style>
    """, unsafe_allow_html=True)

def apply_mobile_style(fig):
    fig.update_layout(
        legend=dict(orientation="h", yanchor="top", y=-0.25, xanchor="center", x=0.5, font=dict(size=10)),
        margin=dict(l=10, r=10, t=50, b=80),
        hovermode="x unified"
    )
    return fig

# --- Yahoo Finance 데이터 (시트에 없는 데이터) ---
@st.cache_data(ttl=3600)
def get_yfinance_data():
    tickers = {
        "DXY Index": "DX-Y.NYB", "USD/KRW": "USDKRW=X", "USD/CNY": "USDCNY=X", 
        "USD/MXN": "USDMXN=X", "USD/JPY": "USDJPY=X", "USD/EUR": "USDEUR=X"
    }
    try:
        data = yf.download(list(tickers.values()), period="10y", interval="1d")['Close']
        inv_tickers = {v: k for k, v in tickers.items()}
        data.rename(columns=inv_tickers, inplace=True)
        return data
    except: return pd.DataFrame()

# 5. 탭 구성
tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs([
    "📊 Repo 흐름", "💸 금리 분석", "🌐 유동성&달러", "💹 환율(Yahoo)", "⚠️ Repo Fails", "⚠️ Dollar Index", "⚠️ Yield Curve", "⚠️ Global Macro"
])

# --- 탭 1: Repo 흐름 ---
with tab1:
    st.subheader("1. Overnight Repo Flow")
    # Sheet Column: Repo_Volume
    if "Repo_Volume" in df_daily.columns:
        repo_data = df_daily["Repo_Volume"].tail(days_to_show)
        fig1 = go.Figure()
        fig1.add_trace(go.Scatter(x=repo_data.index, y=repo_data, mode='lines', fill='tozeroy', line=dict(color='royalblue', width=2)))
        fig1.update_layout(title="Daily Repo Volume Trend", template='plotly_white', height=350)
        st.plotly_chart(apply_mobile_style(fig1), use_container_width=True)
    else:
        st.warning("데이터(Repo_Volume)가 시트에 없습니다.")

    st.subheader("2. SOFR Market Stress")
    # Columns: SOFR, SOFR_99th
    if 'SOFR' in df_daily.columns and 'SOFR_99th' in df_daily.columns:
        sofr_df = df_daily[['SOFR', 'SOFR_99th']].dropna().tail(days_to_show)
        sofr_df['Spread'] = sofr_df['SOFR_99th'] - sofr_df['SOFR']
        
        fig_spread = go.Figure()
        fig_spread.add_trace(go.Scatter(x=sofr_df.index, y=sofr_df['Spread'], mode='lines', line=dict(color='darkorange', width=2), fill='tozeroy', name="Spread"))
        fig_spread.update_layout(title="SOFR Spread (99th - Median)", template='plotly_white', height=350)
        st.plotly_chart(apply_mobile_style(fig_spread), use_container_width=True)
    
    st.divider()
    st.subheader("3. SOFR Seasonality (All Data)")
    if 'SOFR' in df_daily.columns:
        seasonal_df = df_daily[['SOFR', 'SOFR_99th']].dropna()
        seasonal_df['Month'] = seasonal_df.index.month
        monthly_avg = seasonal_df.groupby('Month').mean()
        monthly_avg['Spread'] = monthly_avg['SOFR_99th'] - monthly_avg['SOFR']
        
        fig_season = make_subplots(specs=[[{"secondary_y": True}]])
        fig_season.add_trace(go.Bar(x=monthly_avg.index, y=monthly_avg['SOFR'], name="SOFR Avg", marker_color='darkblue', opacity=0.6), secondary_y=False)
        fig_season.add_trace(go.Scatter(x=monthly_avg.index, y=monthly_avg['Spread'], name="Spread Avg (R)", line=dict(color='orange', width=3, dash='dot')), secondary_y=True)
        fig_season.update_layout(title="Monthly Seasonality", template='plotly_white', xaxis=dict(tickmode='array', tickvals=list(range(1, 13))))
        st.plotly_chart(apply_mobile_style(fig_season), use_container_width=True)

# --- 탭 2: 금리 분석 ---
with tab2:
    st.subheader("SOFR vs Fed Target")
    # Columns: Fed_Target_Upper, Fed_Target_Lower (updater.py에서 매핑한 이름 확인)
    cols_needed = ['SOFR', 'SOFR_99th', 'Fed_Target_Upper', 'Fed_Target_Lower']
    
    # 컬럼 존재 여부 확인
    available = [c for c in cols_needed if c in df_daily.columns]
    
    if len(available) == 4:
        r_df = df_daily[cols_needed].dropna().tail(days_to_show)
        
        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(x=r_df.index, y=r_df['Fed_Target_Lower'], mode='lines', line=dict(width=0), showlegend=False))
        fig2.add_trace(go.Scatter(x=r_df.index, y=r_df['Fed_Target_Upper'], mode='lines', line=dict(width=0), fill='tonexty', fillcolor='rgba(100, 149, 237, 0.4)', name='Target Range'))
        fig2.add_trace(go.Scatter(x=r_df.index, y=r_df['SOFR'], name='SOFR', line=dict(color='darkblue', width=2)))
        fig2.add_trace(go.Scatter(x=r_df.index, y=r_df['SOFR_99th'], name='SOFR 99th', line=dict(color='orange', width=1.5, dash='dot')))
        fig2.update_layout(title="SOFR vs Fed Target", template='plotly_white', height=400)
        st.plotly_chart(apply_mobile_style(fig2), use_container_width=True)
        
        st.divider()
        st.subheader("🎯 Policy Deviation")
        r_df['Mid'] = (r_df['Fed_Target_Upper'] + r_df['Fed_Target_Lower']) / 2
        r_df['Diff'] = r_df['SOFR'] - r_df['Mid']
        
        fig_diff = go.Figure()
        fig_diff.add_hline(y=0, line_color="black")
        fig_diff.add_trace(go.Scatter(x=r_df.index, y=r_df['Diff'], name='Deviation', fill='tozeroy', line=dict(color='darkblue')))
        fig_diff.update_layout(title="Deviation from Midpoint", template='plotly_white', height=300)
        st.plotly_chart(apply_mobile_style(fig_diff), use_container_width=True)
    else:
        st.warning("Fed Target 데이터가 시트에 없습니다.")

# --- 탭 3: 유동성 ---
with tab3:
    st.subheader("🌐 Global Dollar Liquidity")
    # Columns: OBFRVOL, DTWEXBGS, DTWEXAFEGS, DTWEXEMEGS
    l_cols = ['OBFRVOL', 'DTWEXBGS', 'DTWEXAFEGS', 'DTWEXEMEGS']
    avail_cols = [c for c in l_cols if c in df_daily.columns]
    
    if avail_cols:
        d3 = df_daily[avail_cols].tail(days_to_show)
        # Yahoo FX 추가 (시트에 없음)
        yf_df = get_yfinance_data().tail(days_to_show)
        d3 = d3.join(yf_df, how='outer').ffill().tail(days_to_show)
        
        fig3 = make_subplots(specs=[[{"secondary_y": True}]])
        if 'OBFRVOL' in d3.columns:
            fig3.add_trace(go.Scatter(x=d3.index, y=d3['OBFRVOL'], name="OBFR Vol (L)", fill='tozeroy', line=dict(color='gray', width=1)), secondary_y=False)
        if 'DTWEXBGS' in d3.columns:
            fig3.add_trace(go.Scatter(x=d3.index, y=d3['DTWEXBGS'], name="Broad Dollar (R)", line=dict(color='blue', width=2)), secondary_y=True)
            
        fig3.update_layout(height=400, template='plotly_white')
        st.plotly_chart(apply_mobile_style(fig3), use_container_width=True)

# --- 탭 4: 환율 (Yahoo) ---
with tab4:
    st.subheader("Yahoo Finance: Currencies")
    yf_raw = get_yfinance_data().tail(days_to_show)
    if not yf_raw.empty:
        # 정규화 차트
        norm_df = yf_raw / yf_raw.iloc[0] * 100
        fig4 = go.Figure()
        for c in norm_df.columns:
            fig4.add_trace(go.Scatter(x=norm_df.index, y=norm_df[c], name=c))
        fig4.update_layout(title="Relative Performance (100 Base)", template='plotly_white')
        st.plotly_chart(apply_mobile_style(fig4), use_container_width=True)

# --- 탭 5: Repo Fails (Weekly Sheet) ---
with tab5:
    st.subheader("Primary Dealer Repo Fails")
    if not df_weekly.empty:
        # 최근 데이터 조회 (주간 데이터라 개수가 적음)
        w_df = df_weekly.tail(max(20, days_to_show // 7))
        
        fig_fail = go.Figure()
        # OFR 컬럼명은 updater.py에서 매핑됨 (UST_fails_to_deliver 등)
        for col in df_weekly.columns:
            fig_fail.add_trace(go.Scatter(x=w_df.index, y=w_df[col], name=col, stackgroup='one'))
        
        fig_fail.update_layout(title="Repo Fails (Stacked)", template='plotly_white', height=400)
        st.plotly_chart(apply_mobile_style(fig_fail), use_container_width=True)
        
        # 계절성 (전체 데이터 사용)
        if 'UST_fails_to_deliver' in df_weekly.columns:
            ust = df_weekly[['UST_fails_to_deliver']].copy()
            ust['Week'] = ust.index.isocalendar().week
            ust_avg = ust.groupby('Week').mean()
            
            fig_season_fail = go.Figure()
            fig_season_fail.add_trace(go.Bar(x=ust_avg.index, y=ust_avg['UST_fails_to_deliver'], name="Avg Fails"))
            fig_season_fail.update_layout(title="Weekly Seasonality (UST Fails)", template='plotly_white', height=300)
            st.plotly_chart(apply_mobile_style(fig_season_fail), use_container_width=True)

# --- 탭 6: Dollar Index (외부 Sheet) ---
with tab6:
    st.info("이 탭은 별도의 공개 시트를 사용하므로 기존 로직을 유지합니다.")
    sheet_url = "https://docs.google.com/spreadsheets/d/1rOh_s5JeKw_mP98u2URa8OO-xBgSdAHn73qqjnI95rs/export?format=csv"
    try:
        df_w = pd.read_csv(sheet_url)
        st.dataframe(df_w.head())
    except:
        st.error("외부 시트 로드 실패")

# --- 탭 7: Yield Curve (US & KR from Daily Sheet) ---
with tab7:
    st.subheader("Yield Curve Analysis")
    
    # KR Columns: KR_BaseRate, KR_1Y, KR_2Y, ...
    # US Columns: US_3M, US_1Y, US_2Y, ... (updater.py에서 저장한 이름)
    
    if not df_daily.empty:
        latest = df_daily.iloc[-1]
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("#### 🇰🇷 KR Yield Curve")
            kr_mats = ['KR_1Y', 'KR_2Y', 'KR_3Y', 'KR_5Y', 'KR_10Y', 'KR_20Y', 'KR_30Y']
            # 시트에 존재하는 컬럼만 필터링
            valid_kr = [m for m in kr_mats if m in df_daily.columns]
            
            if valid_kr:
                x_kr = [m.replace('KR_','') for m in valid_kr]
                y_kr = latest[valid_kr].values
                
                fig_kr = go.Figure()
                fig_kr.add_trace(go.Scatter(x=x_kr, y=y_kr, mode='lines+markers', name='KR Treasury', line=dict(color='firebrick', width=3)))
                
                if 'KR_BaseRate' in latest:
                    fig_kr.add_hline(y=latest['KR_BaseRate'], line_dash='dot', annotation_text="Base Rate")
                
                fig_kr.update_layout(height=400, template='plotly_white')
                st.plotly_chart(apply_mobile_style(fig_kr), use_container_width=True)
            else:
                st.warning("KR 금리 데이터가 시트에 없습니다.")
            
        with col2:
            st.write("#### 🇺🇸 US Yield Curve")
            us_mats = ['US_3M', 'US_1Y', 'US_2Y', 'US_3Y', 'US_5Y', 'US_10Y', 'US_30Y']
            valid_us = [m for m in us_mats if m in df_daily.columns]
            
            if valid_us:
                x_us = [m.replace('US_','') for m in valid_us]
                y_us = latest[valid_us].values
                
                fig_us = go.Figure()
                fig_us.add_trace(go.Scatter(x=x_us, y=y_us, mode='lines+markers', name='US Treasury', line=dict(color='royalblue', width=3)))
                fig_us.update_layout(height=400, template='plotly_white')
                st.plotly_chart(apply_mobile_style(fig_us), use_container_width=True)
            
        st.divider()
        st.write("#### 📉 Spread Analysis (KR 10Y - 3Y)")
        if 'KR_10Y' in df_daily.columns and 'KR_3Y' in df_daily.columns:
            spread = df_daily['KR_10Y'] - df_daily['KR_3Y']
            s_plot = spread.tail(days_to_show)
            fig_s = go.Figure()
            fig_s.add_trace(go.Scatter(x=s_plot.index, y=s_plot, fill='tozeroy', name='10Y-3Y'))
            fig_s.update_layout(title="KR Term Spread", height=300, template='plotly_white')
            st.plotly_chart(apply_mobile_style(fig_s), use_container_width=True)

# --- 탭 8: Global Macro (Sheet Data) ---
with tab8:
    st.subheader("🌎 Global Macro (Updated from ECOS/FRED)")
    
    countries = {
        "South Korea": "KR", "USA": "US", "China": "CN", 
        "Eurozone": "XM", "Germany": "DE", "Japan": "JP"
    }
    
    m_tabs = st.tabs(list(countries.keys()))
    
    for i, (name, code) in enumerate(countries.items()):
        with m_tabs[i]:
            # Sheet Columns: {code}_GDP_Growth, {code}_CPI_YoY, {code}_Unemployment, {code}_PolicyRate
            c1, c2 = st.columns(2)
            
            # Monthly Data
            m_cols = [f"{code}_CPI_YoY", f"{code}_Unemployment", f"{code}_PolicyRate"]
            if all(c in df_monthly.columns for c in m_cols):
                m_data = df_monthly[m_cols].dropna().tail(24) # 최근 2년
                
                with c1:
                    fig_cpi = go.Figure()
                    fig_cpi.add_trace(go.Scatter(x=m_data.index, y=m_data[f"{code}_CPI_YoY"], name="CPI YoY", line=dict(color='red')))
                    fig_cpi.update_layout(title=f"{name} Inflation", height=300, template='plotly_white')
                    st.plotly_chart(apply_mobile_style(fig_cpi), use_container_width=True)
                
                with c2:
                    fig_mix = go.Figure()
                    # PolicyRate는 왼쪽 축
                    fig_mix.add_trace(go.Scatter(x=m_data.index, y=m_data[f"{code}_PolicyRate"], name="Policy Rate", line=dict(color='black')))
                    # Unemployment는 시각적 구분을 위해 같은 축에 그리되 스타일 변경
                    fig_mix.add_trace(go.Scatter(x=m_data.index, y=m_data[f"{code}_Unemployment"], name="Unemployment", line=dict(color='blue', dash='dot')))
                    fig_mix.update_layout(title=f"{name} Rate & Job", height=300, template='plotly_white')
                    st.plotly_chart(apply_mobile_style(fig_mix), use_container_width=True)
            else:
                st.info(f"Monthly data not available for {name} (Columns: {m_cols})")

            # Quarterly Data (GDP)
            q_col = f"{code}_Growth"
            if q_col in df_quarterly.columns:
                q_data = df_quarterly[[q_col]].dropna().tail(12) # 최근 3년 (12분기)
                fig_gdp = go.Figure()
                fig_gdp.add_trace(go.Bar(x=q_data.index, y=q_data[q_col], name="GDP Growth"))
                fig_gdp.update_layout(title=f"{name} GDP Growth", height=300, template='plotly_white')
                st.plotly_chart(apply_mobile_style(fig_gdp), use_container_width=True)
            else:
                st.info(f"GDP data not available for {name}")
