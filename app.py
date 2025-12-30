import streamlit as st
import pandas as pd
from fredapi import Fred
import plotly.graph_objects as go
from datetime import datetime

# 1. 페이지 설정 및 제목
st.set_page_config(page_title="Financial Dashboard", layout="wide")
st.title("🏦 Federal Reserve Economic Data Dashboard")

# 2. API 키 보안 로드
try:
    FRED_API_KEY = st.secrets["FRED_API_KEY"]
except:
    st.error("⚠️ Streamlit Cloud 설정에서 'FRED_API_KEY'를 확인해주세요.")
    st.stop()

fred = Fred(api_key=FRED_API_KEY)

# 3. 사이드바 - 공통 설정
st.sidebar.header("📅 전역 설정")
period_options = {"6개월": 180, "1년": 365, "3년": 1095, "5년": 1825, "10년": 3650}
selected_label = st.sidebar.selectbox("조회 기간", options=list(period_options.keys()), index=4)
days_to_show = period_options[selected_label]

# 4. 데이터 로드 함수 (캐싱 적용)
@st.cache_data(ttl=3600)
def get_data(series_id):
    try:
        data = fred.get_series(series_id)
        df = pd.DataFrame(data, columns=[series_id])
        return df
    except:
        return pd.DataFrame()

# 5. 탭 생성 (이 부분이 화면 상단에 탭을 만듭니다)
tab1, tab2 = st.tabs(["📊 Repo 흐름 (RPONTTLD)", "💸 금리 분석 (SOFR & Target)"])

# --- 탭 1: Repo 데이터 영역 ---
with tab1:
    st.subheader("Overnight Repurchase Agreements")
    repo_chart_style = st.radio("차트 스타일", ["선 그래프", "바 그래프"], horizontal=True)
    
    with st.spinner('Repo 데이터를 불러오는 중...'):
        repo_raw = get_data('RPONTTLD')
        if not repo_raw.empty:
            repo_df = repo_raw.tail(days_to_show).dropna()
            
            # 가시성 조절 (리샘플링)
            if days_to_show >= 1825:
                repo_df = repo_df.resample('M').mean()
                lbl = "(월간 평균)"
            elif days_to_show >= 365:
                repo_df = repo_df.resample('W').mean()
                lbl = "(주간 평균)"
            else:
                lbl = "(일간)"

            fig1 = go.Figure()
            if repo_chart_style == "선 그래프":
                fig1.add_trace(go.Scatter(x=repo_df.index, y=repo_df['RPONTTLD'], mode='lines', fill='tozeroy', line=dict(color='#1f77b4')))
            else:
                fig1.add_trace(go.Bar(x=repo_df.index, y=repo_df['RPONTTLD'], marker_color='royalblue', marker_line_width=0))
            
            fig1.update_layout(title=f"RPONTTLD {lbl}", template='plotly_white', hovermode='x unified')
            st.plotly_chart(fig1, use_container_width=True)
        else:
            st.warning("Repo 데이터를 불러올 수 없습니다.")

# --- 탭 2: 금리 데이터 영역 ---
with tab2:
    st.subheader("SOFR vs Federal Funds Target Range")
    
    with st.spinner('금리 데이터를 분석 중...'):
        # 데이터 수집
        s_sofr = get_data('SOFR')
        s_sofr99 = get_data('SOFR99')
        s_upper = get_data('DFEDTARU')
        s_lower = get_data('DFEDTARL')

        # 통합 및 필터링 (2017년 이후)
        rates_df = pd.concat([s_sofr, s_sofr99, s_upper, s_lower], axis=1).ffill()
        rates_df = rates_df[rates_df.index >= '2017-01-01'].tail(days_to_show)

        if not rates_df.empty:
            fig2 = go.Figure()
            # 1. Target Range 음영 (Lower -> Upper)
            fig2.add_trace(go.Scatter(x=rates_df.index, y=rates_df['DFEDTARL'], mode='lines', line=dict(width=0), showlegend=False))
            fig2.add_trace(go.Scatter(x=rates_df.index, y=rates_df['DFEDTARU'], mode='lines', line=dict(width=0), 
                                     fill='tonexty', fillcolor='rgba(173, 216, 230, 0.3)', name='Target Range'))
            
            # 2. SOFR 라인들
            fig2.add_trace(go.Scatter(x=rates_df.index, y=rates_df['SOFR'], mode='lines', line=dict(color='darkblue', width=2), name='SOFR'))
            fig2.add_trace(go.Scatter(x=rates_df.index, y=rates_df['SOFR99'], mode='lines', line=dict(color='orange', width=1, dash='dot'), name='SOFR 99th'))

            fig2.update_layout(title="SOFR & Fed Target Range Trend", template='plotly_white', hovermode='x unified')
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.warning("금리 데이터를 불러올 수 없습니다. API 키나 지표 ID를 확인해주세요.")
