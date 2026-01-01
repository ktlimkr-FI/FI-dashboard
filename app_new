import streamlit as st
import pandas as pd
from streamlit_gsheets import GSheetsConnection  # 새 연결 도구
# ... 기존 라이브러리들 (plotly, pytz 등) ...

# --- [데이터 로드 부분 수정] ---

# 구글 시트 연결 설정
conn = st.connection("gsheets", type=GSheetsConnection)

@st.cache_data(ttl=3600) # 1시간마다 시트에서 새로 읽어옴
def load_data_from_sheets():
    # 'data-daily' 탭을 데이터프레임으로 읽어오기
    df = conn.read(worksheet="data-daily")
    
    # Date 컬럼을 인덱스로 설정하고 날짜 형식으로 변환
    df['Date'] = pd.to_datetime(df['Date'])
    df = df.set_index('Date')
    
    # 데이터가 비어있는 칸(주말 등)을 앞의 데이터로 채움
    df = df.ffill()
    return df

# 전체 데이터 로드
try:
    all_daily_df = load_data_from_sheets()
except Exception as e:
    st.error(f"구글 시트 로드 실패: {e}")
    st.stop()

# --- 탭 1 구현 (구글 시트 데이터 활용) ---
with tab1:
    st.subheader("1. Overnight Repo Flow (Repo_Volume)")
    
    # 구글 시트의 'Repo_Volume' 컬럼 사용
    if 'Repo_Volume' in all_daily_df.columns:
        repo_df = all_daily_df['Repo_Volume'].tail(days_to_show)
        
        fig1 = go.Figure()
        fig1.add_trace(go.Scatter(x=repo_df.index, y=repo_df, mode='lines', 
                                 fill='tozeroy', line=dict(color='royalblue', width=2)))
        fig1.update_layout(title="Daily Repo Volume Trend (from GSheets)", template='plotly_white', height=350)
        st.plotly_chart(apply_mobile_style(fig1), use_container_width=True)

    st.subheader("2. SOFR Market Stress (SOFR_99th - SOFR)")
    st.caption("구글 시트에 저장된 SOFR 데이터를 사용하여 분석합니다.")
    
    # 구글 시트의 'SOFR' 및 'SOFR_99th' 컬럼 사용
    if 'SOFR' in all_daily_df.columns and 'SOFR_99th' in all_daily_df.columns:
        spread_display = all_daily_df[['SOFR', 'SOFR_99th']].tail(days_to_show).copy()
        spread_display['Spread'] = spread_display['SOFR_99th'] - spread_display['SOFR']

        fig_spread = go.Figure()
        fig_spread.add_trace(go.Scatter(
            x=spread_display.index, y=spread_display['Spread'], 
            mode='lines', line=dict(color='darkorange', width=2),
            fill='tozeroy', name="Spread (99th-Median)"
        ))
        fig_spread.update_layout(title="SOFR Spread Trend", template='plotly_white', height=350, yaxis_title="Percent (%)")
        st.plotly_chart(apply_mobile_style(fig_spread), use_container_width=True)

    st.divider()
    st.subheader("3. 🗓️ SOFR 월간 계절성 분석 (전체 기간 데이터)")
    
    # 2006년부터 쌓인 전체 데이터를 활용하여 계절성 분석
    seasonal_df = all_daily_df[['SOFR', 'SOFR_99th']].dropna().copy()
    seasonal_df['Month'] = seasonal_df.index.month
    monthly_avg = seasonal_df.groupby('Month').mean()
    monthly_avg['Spread'] = monthly_avg['SOFR_99th'] - monthly_avg['SOFR']

    fig_season = make_subplots(specs=[[{"secondary_y": True}]])
    # ... (기존 fig_season 시각화 로직 동일) ...
    st.plotly_chart(apply_mobile_style(fig_season), use_container_width=True)
