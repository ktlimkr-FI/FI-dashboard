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

# 2. 업데이트할 탭 및 지표 정의 (사용자님의 탭 이름 반영)
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
        print(f"🔄 {tab_name} 업데이트 프로세스 시작...")
        
        # 탭 찾기 또는 생성
        try:
            ws = sh.worksheet(tab_name)
        except gspread.exceptions.WorksheetNotFound:
            ws = sh.add_worksheet(title=tab_name, rows="5000", cols="20")
            print(f"✨ {tab_name} 탭을 새로 생성했습니다.")

        # 시트의 현재 모든 데이터 읽기
        all_values = ws.get_all_values()
        
        # 3. 헤더 및 시작 날짜 설정 로직
        if not all_values:
            # 시트가 완전히 비어있는 경우: 헤더 작성 후 2006년부터 로드
            headers = ['Date'] + list(series_map.values())
            ws.append_row(headers)
            start_date = '2006-01-01'
            print(f"📝 헤더가 없어 새로 작성했습니다: {headers}")
            print(f"📅 초기 데이터 수집 시작: {start_date}")
        elif len(all_values) == 1:
            # 헤더만 있고 데이터는 없는 경우: 2006년부터 로드
            start_date = '2006-01-01'
            print(f"📅 헤더 확인 완료. 2006년부터 데이터를 채웁니다.")
        else:
            # 이미 데이터가 있는 경우: 마지막 날짜 다음 날부터 로드
            last_date_str = all_values[-1][0]
            try:
                start_date = (datetime.strptime(last_date_str, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
                print(f"📅 기존 데이터 확인. {start_date}부터 업데이트를 시작합니다.")
            except ValueError:
                # 날짜 형식이 잘못된 경우 안전하게 2006년부터 다시 시작
                start_date = '2006-01-01'
                print(f"⚠️ 날짜 형식이 올바르지 않아 2006년부터 다시 수집합니다.")

        # 4. FRED 데이터 수집 및 결합
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

        # 5. 구글 시트에 데이터 쓰기
        if not combined_new.empty:
            combined_new.index.name = 'Date'
            combined_new = combined_new.reset_index()
            combined_new['Date'] = combined_new['Date'].dt.strftime('%Y-%m-%d')
            combined_new = combined_new.fillna("") # 빈 칸 처리
            
            # 리스트 형태로 변환하여 전송
            data_to_append = combined_new.values.tolist()
            ws.append_rows(data_to_append)
            print(f"✅ {len(data_to_append)}건의 데이터를 추가했습니다.")
        else:
            print("ℹ️ 새로 추가할 데이터가 없습니다.")

if __name__ == "__main__":
    update_sheet()
