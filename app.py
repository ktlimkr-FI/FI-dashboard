import streamlit as st
import pandas as pd
from fredapi import Fred
import plotly.graph_objects as go

# 1. 페이지 설정
st.set_page_config(page_title="FRED Interactive Dashboard", layout="wide")
st.title("📈 Overnight Repurchase Agreements (RPONTTLD)")

# 2. 보안 처리된 API 키 가져오기
try:
    FRED_API_KEY = st.secrets["FRED_API_KEY"]
except:
    st.error("API 키가 설정되지 않았습니다. Secrets 설정을 확인해주세요.")
    st.stop()

fred = Fred(api_key=FRED_API_KEY)

# 3. 사이드바 설정
st.sidebar.header("대시보드 설정")

# 기간 선택 슬라이더 (최대 10년)
days_to_show = st.sidebar.slider(
    "조회 기간 선택 (일 단위)", 
    min_value=30, 
    max_value=3650, 
    value=3650, 
    step=30
)

# 차트 종류 선택 라디오 버튼 추가
chart_type = st.sidebar.radio(
    "차트 종류 선택",
    ["선 그래프 (흐름 파악)", "바 그래프 (상세 비교)"]
)

# 4. 데이터 가져오기 (캐싱 처리)
@st.cache_data(ttl=3600)
def get_fred_data():
    data = fred.get_series('RPONTTLD')
    df = pd.DataFrame(data, columns=['value'])
    df.index.name = 'date'
    return df.dropna()

with st.spinner('데이터를 분석 중입니다...'):
    all_df = get_fred_data()
    actual_days = min(len(all_df), days_to_show)
    display_df = all_df.tail(actual_days)

# 5. 시각화 로직
fig = go.Figure()

if "선 그래프" in chart_type:
    fig.add_trace(go.Scatter(
        x=display_df.index, 
        y=display_df['value'],
        mode='lines',
        line=dict(color='#1f77b4', width=1.5), # 세련된 블루톤
        fill='tozeroy',
        name='Repo Value'
    ))
else:
    # 바 그래프 가시성 개선 설정
    fig.add_trace(go.Bar(
        x=display_df.index, 
        y=display_df['value'],
        marker_color='midnightblue', # 훨씬 진한 남색으로 변경
        marker_line_width=0,         # 막대 테두리 선을 없애야 촘촘할 때 더 잘 보입니다
        name='Repo Value'
    ))

fig.update_layout(
    title=f"최근 {actual_days}일 {chart_type}",
    xaxis_title='Date',
    yaxis_title='Millions of Dollars',
    template='plotly_white',
    hovermode='x unified',
    bargap=0,  # ⭐ 핵심: 막대 사이의 간격을 0으로 만들어 빈 공간을 없앱니다
    margin=dict(l=0, r=0, t=50, b=0)
)

# 6. 화면 출력
st.plotly_chart(fig, use_container_width=True)

# 최신 데이터 표
st.subheader("최신 데이터 상세 내역")
st.table(display_df.tail(10).iloc[::-1])
