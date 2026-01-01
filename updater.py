import os
import pandas as pd
from fredapi import Fred
import gspread
from google.oauth2.service_account import Credentials
import json
from datetime import datetime, timedelta

# 1. 설정 (GitHub Secrets 환경변수)
FRED_API_KEY = os.environ['FRED_API_KEY']
GSHEET_ID = os.environ['GSHEET_ID']
SERVICE_ACCOUNT_JSON = os.environ['GOOGLE_SERVICE_ACCOUNT_JSON']

# 2. 업데이트할 탭 및 지표 정의 (사용자님의 코드 기반)
# 향후 탭이 늘어나면 여기에 추가하면 됩니다.
TARGET_TABS = {
    'data-daily': {
        'RPONTTLD': 'Repo_Volume',
        'SOFR': 'SOFR',
        'SOFR99': 'SOFR_99th',
        'DFEDTARU': 'Fed_Target_Upper',
        'DFEDTARL': 'Fed_Target_Lower'
    }
}

def get_gspread_client(json_str):
    info = json.loads(json_str)
    creds = Credentials.from_service_account_info(info, scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ])
    return gspread.authorize(creds)

def update_sheet():
    fred = Fred(api_key=FRED_API_KEY)
    gc = get_gspread_client(SERVICE_ACCOUNT_JSON)
    sh = gc.open_by_key(GSHEET_ID)

    for tab_name, series_map in TARGET_TABS.items():
        print(f"🔄 {tab_name} 업데이트 시작...")
        try:
            ws = sh.worksheet(tab_name)
        except gspread.exceptions.WorksheetNotFound:
            ws = sh.add_worksheet(title=tab_name, rows="1000", cols="20")
            ws.append_row(['Date'] + list(series_map.values()))
        
        # 기존 데이터 확인
        existing_data = ws.get_all_values()
        if len(existing_data) <= 1:  # 헤더만 있거나 비어있을 때
            start_date = (datetime.now() - timedelta(days=365*10)).strftime('%Y-%m-%d')
            print(f"📅 초기 데이터 로드: 10년치 데이터를 가져옵니다.")
        else:
            df_existing = pd.DataFrame(existing_data[1:], columns=existing_data[0])
            last_date_str = df_existing['Date'].max()
            start_date = (datetime.strptime(last_date_str, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
            print(f"📅 마지막 날짜({last_date_str}) 이후 데이터를 가져옵니다.")

        # FRED 데이터 수집
        combined_new = pd.DataFrame()
        for s_id, col_name in series_map.items():
            try:
                s = fred.get_series(s_id, observation_start=start_date)
                if not s.empty:
                    temp_df = s.to_frame(name=col_name)
                    if combined_new.empty:
                        combined_new = temp_df
                    else:
                        combined_new = combined_new.join(temp_df, how='outer')
            except Exception as e:
                print(f"⚠️ {s_id} 로드 실패: {e}")

        if not combined_new.empty:
            combined_new.index.name = 'Date'
            combined_new = combined_new.reset_index()
            combined_new['Date'] = combined_new['Date'].dt.strftime('%Y-%m-%d')
            # NaN 값 처리 (구글 시트 전송을 위해 빈 문자열로 변경)
            combined_new = combined_new.fillna("")
            
            ws.append_rows(combined_new.values.tolist())
            print(f"✅ {len(combined_new)}건의 데이터 추가 완료.")
        else:
            print("ℹ️ 추가할 새로운 데이터가 없습니다.")

if __name__ == "__main__":
    update_sheet()
