import streamlit as st
import pandas as pd
from fredapi import Fred
import plotly.graph_objects as go

# 1. 페이지 설정
st.set_page_config(page_title="FRED Repo Dashboard", layout="wide")
st.title("📈 Overnight Repurchase Agreements (RPONTTLD)")

# 2. 보안 처리된 API 키 가져오기
try:
    FRED_API_KEY = st.secrets["FRED_API_KEY"]
except:
    st.error("API 키 설정을 확인해주세요.")
    st.stop()

fred = Fred(api_key=FRED_API_KEY)

# 3. 사이드바 설정: 기간 선택 메뉴 구성
st.sidebar.header("조회 설정")

# 기간 레이블과 실제 일수 매핑
period_options = {
    "6개월": 180,
    "1년": 365,
    "3년": 1095,
    "5년": 1825,
    "10년": 3650
}

# 선택 박스 추가 (기본값: 10년)
selected_label = st.sidebar.selectbox(
    "조회 기간 선택",
    options=list(period_options.keys()),
    index=4  # 리스트의 4번째인 '10년'을 기본값으로 설정
)

days_to_show = period_options[selected_label]

# 차트 종류 선택
chart_type = st.sidebar.radio("차트 종류", ["선 그래프", "바 그래프"])

# 4. 데이터 가져오기 및 자동 리샘플링
@st.cache_data(ttl=3600)
def get_processed_data(days):
    data = fred.get_series('RPONTTLD')
    df = pd.DataFrame(data, columns=['value'])
    df.index.name = 'date'
    df = df.dropna()
    
    # 선택한 기간만큼 자르기
    df = df.tail(days)
    
    # 가시성을 위해 기간별 데이터 묶기 설정
    if days >= 1825: # 5년 이상이면 월간 평균
        df = df.resample('M').mean()
        label = "(월간 평균)"
    elif days >= 365: # 1년 이상이면 주간 평균
        df = df.resample('W').mean()
        label = "(주간 평균)"
    else: # 1년 미만은 일간 데이터 그대로
        label = "(일간)"
        
    return df, label

with st.spinner(f'{selected_label} 데이터를 분석 중입니다...'):
    display_df, period_label = get_processed_data(days_to_show)

# 5. 시각화 로직
fig = go.Figure()

if chart_type == "선 그래프":
    fig.add_trace(go.Scatter(
        x=display_df.index, y=display_df['value'],
        mode='lines', line=dict(color='#1f77b4', width=2),
        fill='tozeroy', name='Repo Value'
    ))
else:
    fig.add_trace(go.Bar(
        x=display_df.index, y=display_df['value'],
        marker_color='royalblue',
        marker_line_width=0,
        name='Repo Value'
    ))

fig.update_layout(
    title=f"{selected_label} 데이터 흐름 {period_label}",
    xaxis_title='Date',
    yaxis_title='Millions of Dollars',
    template='plotly_white',
    hovermode='x unified',
    bargap=0.1,
    margin=dict(l=0, r=0, t=50, b=0)
)

st.plotly_chart(fig, use_container_width=True)

# 6. 최신 데이터 상세 내역 표 (최근 10건)
st.subheader(f"최근 {selected_label} 상세 데이터 (최신 10건)")
st.table(display_df.tail(10).iloc[::-1])
