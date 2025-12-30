import streamlit as st
import pandas as pd
from fredapi import Fred
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# 1. 페이지 설정
st.set_page_config(page_title="FRED Financial Dashboard", layout="wide")
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

# 5. 탭 생성 (세 번째 탭 추가)
tab1, tab2, tab3 = st.tabs(["📊 Repo 흐름", "💸 금리 분석", "🌐 유동성 & 달러 인덱스"])

# --- 탭 1: Repo 데이터 ---
with tab1:
    st.subheader("Overnight Repurchase Agreements (RPONTTLD)")
    repo_chart_style = st.radio("Repo 차트 종류", ["선 그래프", "바 그래프"], horizontal=True, key="repo_style")
    repo_raw = get_data('RPONTTLD')
    if not repo_raw.empty:
        repo_df = repo_raw.tail(days_to_show).dropna()
        fig1 = go.Figure()
        if repo_chart_style == "선 그래프":
            fig1.add_trace(go.Scatter(x=repo_df.index, y=repo_df['RPONTTLD'], mode='lines', fill='tozeroy', line=dict(color='#1f77b4')))
        else:
            fig1.add_trace(go.Bar(x=repo_df.index, y=repo_df['RPONTTLD'], marker_color='royalblue', marker_line_width=0))
        fig1.update_layout(template='plotly_white', hovermode='x unified')
        st.plotly_chart(fig1, use_container_width=True)

# --- 탭 2: 금리 데이터 ---
with tab2:
    st.subheader("SOFR vs Fed Target Range")
    rates_df = pd.concat([get_data('SOFR'), get_data('SOFR99'), get_data('DFEDTARU'), get_data('DFEDTARL')], axis=1).ffill()
    rates_df = rates_df[rates_df.index >= '2017-01-01'].tail(days_to_show)
    if not rates_df.empty:
        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(x=rates_df.index, y=rates_df['DFEDTARL'], mode='lines', line=dict(width=0), showlegend=False))
        fig2.add_trace(go.Scatter(x=rates_df.index, y=rates_df['DFEDTARU'], mode='lines', line=dict(width=0), fill='tonexty', fillcolor='rgba(173, 216, 230, 0.3)', name='Target Range'))
        fig2.add_trace(go.Scatter(x=rates_df.index, y=rates_df['SOFR'], mode='lines', line=dict(color='darkblue', width=2), name='SOFR'))
        fig2.add_trace(go.Scatter(x=rates_df.index, y=rates_df['SOFR99'], mode='lines', line=dict(color='orange', width=1, dash='dot'), name='SOFR 99th'))
        fig2.update_layout(template='plotly_white', hovermode='x unified')
        st.plotly_chart(fig2, use_container_width=True)

# --- 탭 3: 유동성 & 달러 인덱스 (신규) ---
with tab3:
    st.subheader("Overnight Bank Funding Volume & U.S. Dollar Indices")
    st.caption("2015년 이후 데이터 (달러 인덱스는 오른쪽 축 표시)")
    
    # OBFR 표시 여부 버튼
    show_obfr = st.checkbox("Show Overnight Bank Funding Volume (OBFRVOL)", value=True)

    with st.spinner('데이터를 불러오는 중...'):
        # 데이터 수집
        d_obfr = get_data('OBFRVOL')
        d_broad = get_data('DTWEXBGS')
        d_afe = get_data('DTWEXAFEGS')
        d_eme = get_data('DTWEXEMEGS')

        # 데이터 통합 및 2015년 이후 필터링
        df3 = pd.concat([d_obfr, d_broad, d_afe, d_eme], axis=1).ffill()
        df3 = df3[df3.index >= '2015-01-01'].tail(days_to_show)

    if not df3.empty:
        # 이중 축 차트 생성
        fig3 = make_subplots(specs=[[{"secondary_y": True}]])

        # 1. OBFR 거래량 (왼쪽 축) - 버튼 선택 시에만 표시
        if show_obfr:
            fig3.add_trace(
                go.Scatter(x=df3.index, y=df3['OBFRVOL'], name="OBFR Volume (Left)", 
                           line=dict(color='rgba(100, 100, 100, 0.5)', width=1.5)),
                secondary_y=False,
            )

        # 2. 달러 인덱스 시리즈 (오른쪽 축)
        fig3.add_trace(
            go.Scatter(x=df3.index, y=df3['DTWEXBGS'], name="Broad Dollar (Right)", line=dict(color='royalblue', width=2)),
            secondary_y=True,
        )
        fig3.add_trace(
            go.Scatter(x=df3.index, y=df3['DTWEXAFEGS'], name="AFE Dollar (Right)", line=dict(color='green', width=1.5)),
            secondary_y=True,
        )
        fig3.add_trace(
            go.Scatter(x=df3.index, y=df3['DTWEXEMEGS'], name="EME Dollar (Right)", line=dict(color='firebrick', width=1.5)),
            secondary_y=True,
        )

        fig3.update_layout(
            title=f"Volume vs Dollar Index Trend ({selected_label})",
            template='plotly_white',
            hovermode='x unified',
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )

        fig3.update_yaxes(title_text="Volume (Millions of $)", secondary_y=False)
        fig3.update_yaxes(title_text="Index Value", secondary_y=True)

        st.plotly_chart(fig3, use_container_width=True)
        st.dataframe(df3.tail(10).iloc[::-1])
    else:
        st.warning("데이터를 불러올 수 없습니다.")
