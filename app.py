import streamlit as st
import pandas as pd
from fredapi import Fred
import plotly.graph_objects as go

# 1. 페이지 설정
st.set_page_config(page_title="FRED Financial Dashboard", layout="wide")
st.title("🏦 Federal Reserve Economic Data Dashboard")

# 2. 보안 처리된 API 키 가져오기
try:
    FRED_API_KEY = st.secrets["FRED_API_KEY"]
except:
    st.error("API 키 설정을 확인해주세요.")
    st.stop()

fred = Fred(api_key=FRED_API_KEY)

# 3. 사이드바 설정 (공통 적용)
st.sidebar.header("조회 설정")

period_options = {
    "6개월": 180,
    "1년": 365,
    "3년": 1095,
    "5년": 1825,
    "10년": 3650
}

selected_label = st.sidebar.selectbox(
    "조회 기간 선택",
    options=list(period_options.keys()),
    index=4
)
days_to_show = period_options[selected_label]

# 4. 데이터 로드 함수 (캐싱 적용)
@st.cache_data(ttl=3600)
def get_fred_data(series_id):
    data = fred.get_series(series_id)
    df = pd.DataFrame(data, columns=[series_id])
    df.index.name = 'date'
    return df

# 5. 탭 구성
tab1, tab2 = st.tabs(["📈 Overnight Repo (RPONTTLD)", "💰 Interest Rates (SOFR & Target Range)"])

# --- 탭 1: 기존 Repo 데이터 ---
with tab1:
    st.subheader("Overnight Repurchase Agreements")
    chart_type = st.sidebar.radio("Repo 차트 종류", ["선 그래프", "바 그래프"], key="repo_chart")
    
    raw_repo = get_fred_data('RPONTTLD')
    repo_df = raw_repo.tail(days_to_show).dropna()
    
    # 가시성 조절을 위한 리샘플링
    if days_to_show >= 1825:
        repo_df = repo_df.resample('M').mean()
        p_label = "(월간 평균)"
    elif days_to_show >= 365:
        repo_df = repo_df.resample('W').mean()
        p_label = "(주간 평균)"
    else:
        p_label = "(일간)"

    fig1 = go.Figure()
    if chart_type == "선 그래프":
        fig1.add_trace(go.Scatter(x=repo_df.index, y=repo_df['RPONTTLD'], mode='lines', fill='tozeroy', line=dict(color='royalblue')))
    else:
        fig1.add_trace(go.Bar(x=repo_df.index, y=repo_df['RPONTTLD'], marker_color='royalblue', marker_line_width=0))
    
    fig1.update_layout(title=f"Repo Flow {p_label}", template='plotly_white', hovermode='x unified')
    st.plotly_chart(fig1, use_container_width=True)

# --- 탭 2: 금리 데이터 (SOFR & Target Range) ---
with tab2:
    st.subheader("SOFR vs Federal Funds Target Range")
    st.caption("2017년 이후 데이터 표시 (Target Range는 음영으로 표시)")

    # 필요한 지표들 가져오기
    with st.spinner('금리 데이터를 불러오는 중...'):
        sofr = get_fred_data('SOFR')
        sofr99 = get_fred_data('SOFR99')
        upper = get_fred_data('DFEDTARU')
        lower = get_fred_data('DFEDTARL')

        # 데이터 병합
        rates_df = pd.concat([sofr, sofr99, upper, lower], axis=1)
        # 2017년 이후 데이터만 필터링 및 선택 기간 적용
        rates_df = rates_df[rates_df.index >= '2017-01-01'].tail(days_to_show).ffill()

    fig2 = go.Figure()

    # 1. Target Range 음영 처리 (Lower를 먼저 그리고 Upper를 Lower까지 채움)
    fig2.add_trace(go.Scatter(
        x=rates_df.index, y=rates_df['DFEDTARL'],
        mode='lines', line=dict(width=0),
        showlegend=False, name='Lower Limit'
    ))
    fig2.add_trace(go.Scatter(
        x=rates_df.index, y=rates_df['DFEDTARU'],
        mode='lines', line=dict(width=0),
        fill='tonexty', fillcolor='rgba(173, 216, 230, 0.4)', # 연한 파란색 음영
        name='Target Range (Upper/Lower)'
    ))

    # 2. SOFR 및 SOFR99 라인 추가
    fig2.add_trace(go.Scatter(
        x=rates_df.index, y=rates_df['SOFR'],
        mode='lines', line=dict(color='darkblue', width=2),
        name='SOFR'
    ))
    fig2.add_trace(go.Scatter(
        x=rates_df.index, y=rates_df['SOFR99'],
        mode='lines', line=dict(color='orange', width=1, dash='dot'),
        name='SOFR 99th Percentile'
    ))

    fig2.update_layout(
        title=f"Interest Rates Trend ({selected_label})",
        xaxis_title="Date",
        yaxis_title="Percent",
        template='plotly_white',
        hovermode='x unified',
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )

    st.plotly_chart(fig2, use_container_width=True)
    st.table(rates_df.tail(10).iloc[::-1])
