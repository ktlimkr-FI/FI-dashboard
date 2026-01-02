def update_daily(fred: Fred, sh):
    """
    Daily financial series updater with LOOKBACK window.
    - Re-pulls last N days to avoid missing data due to holidays / publication lag
    - Appends only truly new dates to Google Sheet
    """

    LOOKBACK_DAYS = 10
    TAB_NAME = "data-daily"

    ws = ensure_worksheet(sh, TAB_NAME)

    target_headers = ["Date"] + list(DAILY_FRED_SERIES.values())
    header, last_date_str = get_header_and_last_date(ws)

    # 1. Header 정합성 보장
    if header != target_headers:
        write_header(ws, target_headers)

    # 2. Pull start (LOOKBACK)
    pull_start = (datetime.utcnow() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    print(f"📌 {TAB_NAME}: lookback pull from {pull_start}")

    # 3. 모든 daily 시리즈를 동일한 날짜 인덱스로 결합
    combined = pd.DataFrame()

    for sid, col in DAILY_FRED_SERIES.items():
        try:
            s = fred.get_series(sid, observation_start=pull_start)
            if s is None or len(s) == 0:
                continue

            s = s.sort_index()
            s.index = pd.to_datetime(s.index)

            tmp = s.to_frame(name=col)
            combined = tmp if combined.empty else combined.join(tmp, how="outer")

            time.sleep(0.15)  # FRED rate limit 보호

        except Exception as e:
            print(f"⚠️ DAILY load failed: {sid} ({e})")

    if combined.empty:
        print(f"ℹ️ {TAB_NAME}: no data pulled")
        return

    # 4. 마지막 날짜 이후 데이터만 남김 (중복 방지)
    if last_date_str:
        last_dt = pd.to_datetime(last_date_str)
        combined = combined[combined.index > last_dt]

    if combined.empty:
        print(f"ℹ️ {TAB_NAME}: no new rows after filtering")
        return

    # 5. Sheet append
    combined.index.name = "Date"
    combined = combined.reset_index()
    combined["Date"] = combined["Date"].dt.strftime("%Y-%m-%d")

    combined = combined[["Date"] + list(DAILY_FRED_SERIES.values())]
    combined = combined.fillna("")

    rows = combined.values.tolist()
    ws.append_rows(rows, value_input_option="USER_ENTERED")

    print(f"✅ {TAB_NAME}: appended {len(rows)} rows")
