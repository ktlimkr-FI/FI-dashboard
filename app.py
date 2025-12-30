import streamlit as st
import pandas as pd
from fredapi import Fred
import plotly.graph_objects as go

# 1. 페이지 제목 설정
st.set_page_config(page_title="FRED Repo Dashboard", layout="wide")
st.title("📈 Overnight Repurchase Agreements (RPONTTLD)")

# 2. 보안 처리된 API 키 가져오기 (나중에 Streamlit 설정에서 입력할 예정)
# 지금은 오류를 방지하기 위해 st.secrets를 사용합니다.
try:
    FRED_API_KEY = st.secrets["FRED_API_KEY"]
except:
    st.error("API 키가 설정되지 않았습니다. Streamlit Cloud의 Secrets 설정을 확인해주세요.")
    st.stop()

fred = Fred(api_key=FRED_API_KEY)

# 3. 데이터 가져오기 (캐싱 처리하여 속도 향상)
@st.cache_data(ttl=3600) # 1시간 동안은 API 재호출 없이 캐시 사용
def get_fred_data():
    data = fred.get_series('RPONTTLD')
    df = pd.DataFrame(data, columns=['value'])
    df.index.name = 'date'
    return df.tail(90) # 최근 90일 데이터

with st.spinner('데이터를 불러오는 중입니다...'):
    df = get_fred_data()

# 4. 바 차트 생성
fig = go.Figure(data=[
    go.Bar(x=df.index, y=df['value'], marker_color='royalblue')
])

fig.update_layout(
    xaxis_title='Date',
    yaxis_title='Millions of Dollars',
    template='plotly_white',
    margin=dict(l=0, r=0, t=0, b=0)
)

# 5. 화면 출력
st.plotly_chart(fig, use_container_width=True)

# 데이터 요약 정보
st.subheader("최근 데이터 요약")
st.dataframe(df.iloc[::-1]) # 최신 날짜가 위로 오도록 출력
