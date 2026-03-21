#!/usr/bin/env python3
"""
歷史數據補填腳本
用途：對 binance_klines.db 補充 2021 年到目前覆蓋缺口

重要：實作完成後不要執行！老師要先確認邏輯。
"""

import argparse
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

HISTORICAL_START = datetime(2021, 1, 1, tzinfo=timezone.utc)
BINANCE_FAPI_KLINES = "https://fapi.binance.com/fapi/v1/klines"
INTERVAL_MAP = {"15m": "15m", "30m": "30m", "1h": "1h", "2h": "2h", "4h": "4h"}
INTERVAL_MS = {
    "15m": 15 * 60 * 1000,
    "30m": 30 * 60 * 1000,
    "1h": 60 * 60 * 1000,
    "2h": 2 * 60 * 60 * 1000,
    "4h": 4 * 60 * 60 * 1000,
}


def get_db_path() -> Path:
    # 回傳 data_cache/binance_klines.db 的路徑
    repo_root = Path(__file__).resolve().parent.parent
    return repo_root / "data_cache" / "binance_klines.db"


def get_db_oldest_open(conn: sqlite3.Connection, symbol: str, timeframe: str) -> int | None:
    """查 DB 中該 symbol/timeframe 的最舊 open_time，沒有資料則回傳 None"""
    cur = conn.execute(
        "SELECT MIN(open_time) FROM klines WHERE symbol=? AND timeframe=?",
        (symbol, timeframe),
    )
    row = cur.fetchone()
    return row[0] if row and row[0] else None


def get_symbols(conn: sqlite3.Connection) -> list[str]:
    """取得 DB 中所有 symbol（distinct）"""
    cur = conn.execute("SELECT DISTINCT symbol FROM klines ORDER BY symbol")
    return [row[0] for row in cur.fetchall()]


def paginate_fetch(symbol: str, timeframe: str, start_ts_ms: int, end_ts_ms: int) -> list:
    """
    從 Binance API 補填歷史數據（paginated）。

    演算法：
    1. 用 startTime 往前抓（從 start_ts_ms 往後，每次 limit=1500）
    2. 直到覆蓋到「DB 目前最舊的資料」or API 無更多資料
    3. 只 upsert 不重複的 klines

    Returns: list of kline rows (raw format)
    """
    interval_ms = INTERVAL_MS[timeframe]
    fetched_all = []
    current_start = start_ts_ms

    while current_start < end_ts_ms:
        params = {
            "symbol": symbol,
            "interval": INTERVAL_MAP[timeframe],
            "startTime": current_start,
            "endTime": end_ts_ms,
            "limit": 1500,
        }
        try:
            resp = requests.get(BINANCE_FAPI_KLINES, params=params, timeout=10)
            if resp.status_code != 200:
                print(f"  API error {resp.status_code}: {resp.text[:100]}")
                break

            data = resp.json()
            if not data:
                break

            fetched_all.extend(data)

            # 下一批：最後一根 close_time + 1ms
            last_close = int(data[-1][6])
            next_start = last_close + 1
            if next_start <= current_start:
                print(f"  Stalled at {current_start}, breaking")
                break

            current_start = next_start
            time.sleep(0.2)  # 避免觸發 rate limit
        except Exception as exc:
            print(f"  Request failed: {exc}")
            break

    return fetched_all


def upsert_klines(conn: sqlite3.Connection, symbol: str, timeframe: str, klines: list):
    """將 klines upsert 進 SQLite（用 open_time 當 PK）"""
    if not klines:
        return 0

    cur = conn.cursor()
    count = 0
    for kline in klines:
        open_time = int(kline[0])
        open_ = kline[1]
        high = kline[2]
        low = kline[3]
        close = kline[4]
        volume = kline[5]
        close_time = int(kline[6])
        quote_volume = kline[7]
        cur.execute(
            """
            INSERT OR IGNORE INTO klines
            (symbol, timeframe, open_time, open, high, low, close, volume, close_time, quote_volume, raw_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                symbol,
                timeframe,
                open_time,
                open_,
                high,
                low,
                close,
                volume,
                close_time,
                quote_volume,
                str(kline),
            ),
        )
        if cur.rowcount > 0:
            count += 1

    conn.commit()
    return count


def backfill_timeframe(timeframe: str, dry_run: bool = True):
    """
    對一個 timeframe 補填所有 symbol 的歷史數據。

    流程：
    1. 取得所有 symbol
    2. 對每個 symbol：
       - 查 DB 目前最舊的 open_time
       - 若比 HISTORICAL_START 還新，算出缺口並補填
       - 若已有覆蓋到 HISTORICAL_START，跳過
    """
    db_path = get_db_path()
    conn = sqlite3.connect(db_path)

    try:
        symbols = get_symbols(conn)

        print(f"\n{'=' * 60}")
        print(f"Timeframe: {timeframe}")
        print(f"Symbols: {len(symbols)}")
        print(f"Target: 補到 {HISTORICAL_START.date()} ~ DB oldest")
        print(f"{'=' * 60}")

        for index, symbol in enumerate(symbols, start=1):
            oldest = get_db_oldest_open(conn, symbol, timeframe)
            if oldest is None:
                print(f"[{index}/{len(symbols)}] {symbol}: No data, skipping")
                continue

            oldest_dt = datetime.fromtimestamp(oldest / 1000, tz=timezone.utc)
            if oldest_dt <= HISTORICAL_START:
                print(f"[{index}/{len(symbols)}] {symbol}: Already covered to {oldest_dt.date()}, skipping")
                continue

            # 需要補填的範圍：HISTORICAL_START ~ oldest
            start_ts = int(HISTORICAL_START.timestamp() * 1000)
            end_ts = oldest  # 補到「目前 DB 最舊資料的開盤時間」（不覆蓋現有資料）

            print(
                f"[{index}/{len(symbols)}] {symbol}: "
                f"Gap {HISTORICAL_START.date()} ~ {oldest_dt.date()}, fetching..."
            )

            if dry_run:
                print(f"  [DRY RUN] Would fetch from {start_ts} to {end_ts}")
                continue

            klines = paginate_fetch(symbol, timeframe, start_ts, end_ts)
            if klines:
                inserted = upsert_klines(conn, symbol, timeframe, klines)
                print(f"  Inserted {inserted}/{len(klines)} klines")
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Backfill historical Binance klines")
    parser.add_argument(
        "--timeframe",
        "-t",
        choices=["15m", "30m", "1h", "2h", "4h", "all"],
        default="all",
        help="Timeframe to backfill",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without fetching data",
    )
    args = parser.parse_args()

    if args.timeframe == "all":
        timeframes = ["4h", "2h", "1h", "30m", "15m"]  # 大的先處理
    else:
        timeframes = [args.timeframe]

    for timeframe in timeframes:
        backfill_timeframe(timeframe, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
