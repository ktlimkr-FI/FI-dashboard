import json
import pandas as pd
import gspread
import streamlit as st
from google.oauth2.service_account import Credentials

@st.cache_data(ttl=3600)
def load_daily_from_gsheet(worksheet_name: str = "data-daily") -> pd.DataFrame:
    """
    Reads a PRIVATE Google Sheet tab into a DataFrame using a Service Account.
    Expected sheet format:
      - A 'Date' column (or 'date') that can be parsed as datetime
      - Other columns: Repo_Volume, SOFR, SOFR_99th, etc.
    """
    # 1) Load service account JSON from Streamlit secrets
    info = json.loads(st.secrets["GOOGLE_SERVICE_ACCOUNT_JSON"])

    # 2) Read-only scope is enough for dashboard
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)

    # 3) Connect
    gc = gspread.authorize(creds)

    # 4) Open spreadsheet
    sh = gc.open_by_key(st.secrets["GSHEET_ID"])

    # 5) Open worksheet (tab)
    ws = sh.worksheet(worksheet_name)

    # 6) Fetch records (assumes first row is header)
    records = ws.get_all_records()

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)

    # 7) Normalize Date column
    if "Date" in df.columns:
        date_col = "Date"
    elif "date" in df.columns:
        date_col = "date"
    else:
        raise ValueError("Google Sheet must have a 'Date' (or 'date') column.")

    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col]).set_index(date_col).sort_index()

    # 8) Convert numeric columns (best-effort)
    for c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="ignore")

    # 9) Forward fill for weekends/holidays (optional)
    df = df.ffill()

    return df

with tab1:
    st.subheader("1. Overnight Repo Flow (Repo_Volume)")

    try:
        all_daily_df = load_daily_from_gsheet("data-daily")
    except Exception as e:
        st.error(f"구글 시트 로드 실패: {e}")
        st.stop()

    # 1) Repo Volume
    if "Repo_Volume" in all_daily_df.columns:
        repo_df = all_daily_df["Repo_Volume"].tail(days_to_show).dropna()

        if not repo_df.empty:
            fig1 = go.Figure()
            fig1.add_trace(go.Scatter(
                x=repo_df.index, y=repo_df,
                mode="lines", fill="tozeroy",
                line=dict(color="royalblue", width=2)
            ))
            fig1.update_layout(
                title="Daily Repo Volume Trend (from GSheets)",
                template="plotly_white",
                height=350
            )
            st.plotly_chart(apply_mobile_style(fig1), use_container_width=True)
        else:
            st.info("Repo_Volume 데이터가 비어 있습니다.")
    else:
        st.warning("시트에 'Repo_Volume' 컬럼이 없습니다.")

    # 2) SOFR Spread
    st.subheader("2. SOFR Market Stress (SOFR_99th - SOFR)")
    st.caption("구글 시트에 저장된 SOFR 데이터를 사용하여 분석합니다.")

    if "SOFR" in all_daily_df.columns and "SOFR_99th" in all_daily_df.columns:
        spread_display = all_daily_df[["SOFR", "SOFR_99th"]].tail(days_to_show).dropna().copy()
        spread_display["Spread"] = spread_display["SOFR_99th"] - spread_display["SOFR"]

        if not spread_display.empty:
            fig_spread = go.Figure()
            fig_spread.add_trace(go.Scatter(
                x=spread_display.index, y=spread_display["Spread"],
                mode="lines",
                line=dict(color="darkorange", width=2),
                fill="tozeroy",
                name="Spread (99th-Median)"
            ))
            fig_spread.update_layout(
                title="SOFR Spread Trend (from GSheets)",
                template="plotly_white",
                height=350,
                yaxis_title="Percent (%)"
            )
            st.plotly_chart(apply_mobile_style(fig_spread), use_container_width=True)
        else:
            st.info("SOFR 스프레드 계산에 필요한 데이터가 부족합니다.")
    else:
        st.warning("시트에 'SOFR' 또는 'SOFR_99th' 컬럼이 없습니다.")

    st.divider()

    # 3) Monthly seasonality (full history in sheet)
    st.subheader("3. 🗓️ SOFR 월간 계절성 분석 (전체 기간 데이터)")

    if "SOFR" in all_daily_df.columns and "SOFR_99th" in all_daily_df.columns:
        seasonal_df = all_daily_df[["SOFR", "SOFR_99th"]].dropna().copy()
        seasonal_df["Month"] = seasonal_df.index.month
        monthly_avg = seasonal_df.groupby("Month").mean()
        monthly_avg["Spread"] = monthly_avg["SOFR_99th"] - monthly_avg["SOFR"]

        fig_season = make_subplots(specs=[[{"secondary_y": True}]])
        fig_season.add_trace(go.Bar(x=monthly_avg.index, y=monthly_avg["SOFR"], name="SOFR Avg"), secondary_y=False)
        fig_season.add_trace(go.Scatter(x=monthly_avg.index, y=monthly_avg["SOFR_99th"], name="SOFR 99th Avg"), secondary_y=False)
        fig_season.add_trace(go.Scatter(x=monthly_avg.index, y=monthly_avg["Spread"], name="Spread Avg (Right)"), secondary_y=True)

        fig_season.update_layout(
            title="Monthly Seasonality: SOFR vs Spread (from GSheets)",
            xaxis=dict(
                tickmode="array",
                tickvals=list(range(1, 13)),
                ticktext=["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
            ),
            template="plotly_white",
            hovermode="x unified"
        )
        fig_season.update_yaxes(title_text="Interest Rate (%)", secondary_y=False)
        fig_season.update_yaxes(title_text="Spread (%)", secondary_y=True)

        st.plotly_chart(apply_mobile_style(fig_season), use_container_width=True)
    else:
        st.warning("계절성 분석을 위해 'SOFR'와 'SOFR_99th' 컬럼이 필요합니다.")

