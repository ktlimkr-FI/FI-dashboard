import streamlit as st
import pandas as pd
from fredapi import Fred
import plotly.graph_objects as go

# 1. 페이지 설정
st.set_page_config(page_title="FRED 10Y Dashboard", layout="wide")
st.title("📈 Overnight Repurchase Agreements (RPONTTLD)")

# 2. 보안 처리된 API 키 가져오기
try:
    FRED_API_KEY = st.secrets["FRED_API_KEY"]
except:
    st.error("API 키 설정을 확인해주세요.")
    st.stop()

fred = Fred(api_key=FRED_API_KEY)

# 3. 사이드바 설정
st.sidebar.header("대시보드 설정")
days_to_show = st.sidebar.slider("조회 기간 (일)", 30, 3650, 3650, 10)
chart_type = st.sidebar.radio("차트 종류", ["선 그래프", "바 그래프"])

# 4. 데이터 가져오기 및 자동 리샘플링 (핵심!)
@st.cache_data(ttl=3600)
def get_processed_data(days):
    # 전체 데이터를 가져옴
    data = fred.get_series('RPONTTLD')
    df = pd.DataFrame(data, columns=['value'])
    df.index.name = 'date'
    df = df.dropna()
    
    # 선택한 기간만큼 자르기
    df = df.tail(days)
    
    # [가시성 해결 포인트] 기간에 따라 데이터 밀도 조절
    if days > 1500: # 약 4년 이상이면 월간 평균으로 묶음
        df = df.resample('M').mean()
        label = "(월간 평균)"
    elif days > 365: # 1년~4년 사이면 주간 평균으로 묶음
        df = df.resample('W').mean()
        label = "(주간 평균)"
    else: # 1년 미만은 일일 데이터 그대로 사용
        label = "(일간)"
        
    return df, label

with st.spinner('데이터 최적화 중...'):
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
    # 바 그래프 가시성 극대화: 테두리를 없애고 색상을 진하게
    fig.add_trace(go.Bar(
        x=display_df.index, y=display_df['value'],
        marker_color='royalblue',
        marker_line_width=0,
        name='Repo Value'
    ))

fig.update_layout(
    title=f"최근 {days_to_show}일 {chart_type} {period_label}",
    xaxis_title='Date',
    yaxis_title='Millions of Dollars',
    template='plotly_white',
    hovermode='x unified',
    bargap=0.1, # 막대 사이의 아주 미세한 간격
    margin=dict(l=0, r=0, t=50, b=0)
)

st.plotly_chart(fig, use_container_width=True)
st.info(f"💡 현재 기간({days_to_show}일)에 최적화하여 **{period_label}** 데이터로 표시 중입니다.")
