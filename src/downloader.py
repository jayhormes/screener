# -*- coding: utf-8 -*-
import asyncio
import concurrent.futures
import json
import os
import pickle
import random
import re
import sqlite3
import time
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import aiohttp
import numpy as np
import pandas as pd
from binance.client import Client
from polygon import RESTClient
from pytz import timezone
from stocksymbol import StockSymbol
from urllib3.util.retry import Retry


# Configurable parameters
STOCK_SMA = [20, 30, 45, 50, 60, 150, 200]
CRYPTO_SMA = [30, 45, 60]
ATR_PERIOD = 60


BINANCE_FUTURES_BASE = "https://fapi.binance.com"
BINANCE_WS_BASE = "wss://fstream.binance.com"


def calculate_atr(df, period=ATR_PERIOD):
    high = df['high']
    low = df['low']
    close = df['close']

    tr1 = high - low
    tr2 = abs(high - close.shift())
    tr3 = abs(low - close.shift())
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(window=period).mean()
    return atr


def parse_time_string(time_string):
    pattern_with_number = r"(\d+)([mhdMHD])$"
    pattern_without_number = r"([dD])$"
    match_with_number = re.match(pattern_with_number, time_string)
    match_without_number = re.match(pattern_without_number, time_string)

    if match_with_number:
        number = int(match_with_number.group(1))
        unit = match_with_number.group(2)
    elif match_without_number:
        number = 1
        unit = match_without_number.group(1)
    else:
        raise ValueError("Invalid time format. Only formats like '15m', '4h', 'd' are allowed.")

    unit = unit.lower()
    unit_match = {"m": "minute", "h": "hour", "d": "day"}
    return number, unit_match[unit]


def timeframe_to_ms(timeframe: str) -> int:
    n, unit = parse_time_string(timeframe)
    if unit == "minute":
        return n * 60 * 1000
    if unit == "hour":
        return n * 3600 * 1000
    return n * 86400 * 1000


class StockDownloader:
    def __init__(self, api_file: str = "api_keys.json"):
        with open(api_file) as f:
            self.api_keys = json.load(f)

        retry_strategy = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[413, 429, 499, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "PUT", "DELETE", "OPTIONS", "TRACE", "POST"],
            raise_on_status=False,
            respect_retry_after_header=True,
        )

        self.client = RESTClient(
            api_key=self.api_keys["polygon"],
            num_pools=100,
            connect_timeout=1.0,
            read_timeout=1.0,
            retries=10,
        )

    def _validate_data_quality(self, df: pd.DataFrame) -> bool:
        if df.empty:
            return False

        latest_ts = df['timestamp'].max()
        week_ago = time.time() - (7 * 24 * 3600)
        if latest_ts < week_ago:
            return False

        consecutive_same_price = df['close'].rolling(window=10).apply(lambda x: len(set(x)) == 1)
        if consecutive_same_price.any():
            return False

        return True

    def get_data(self, ticker: str, start_ts: int, end_ts: int = None, timeframe: str = "1d", dropna=True, atr=True, validate=True) -> tuple[bool, pd.DataFrame, bool]:
        try:
            if timeframe == "1d":
                multiplier = 1
                timespan = "day"
            elif timeframe == "hour":
                multiplier = 1
                timespan = "hour"
            elif timeframe == "minute":
                multiplier = 1
                timespan = "minute"
            else:
                raise ValueError("Unsupported stock timeframe")

            max_sma = max(STOCK_SMA)
            fc = 1.3 if timeframe == "1d" else 0.6
            extension = np.int64(max_sma * 24 * 3600 * fc)
            extended_start = np.int64(start_ts - extension)

            if end_ts is None:
                end_ts = np.int64(time.time())

            aggs = self.client.get_aggs(
                ticker,
                multiplier,
                timespan,
                np.int64(extended_start * 1000),
                np.int64(end_ts * 1000),
                limit=10000,
            )

            if not aggs:
                return False, pd.DataFrame(), False

            df = pd.DataFrame([
                {
                    'timestamp': np.int64(agg.timestamp // 1000),
                    'open': np.float64(agg.open),
                    'close': np.float64(agg.close),
                    'high': np.float64(agg.high),
                    'low': np.float64(agg.low),
                    'volume': np.float64(agg.volume),
                }
                for agg in aggs
            ])

            if df.empty:
                return False, df, False

            df = df.sort_values('timestamp')

            if timespan in ("hour", "minute"):
                ny_tz = timezone('America/New_York')
                temp_dt = pd.to_datetime(df['timestamp'], unit='s', utc=True).dt.tz_convert(ny_tz)
                if timespan == "hour":
                    market_hours_filter = temp_dt.dt.time.between(pd.to_datetime('09:00').time(), pd.to_datetime('16:00').time(), inclusive='left')
                else:
                    market_hours_filter = temp_dt.dt.time.between(pd.to_datetime('09:30').time(), pd.to_datetime('16:00').time(), inclusive='left')
                df = df[market_hours_filter]

            if validate and not self._validate_data_quality(df):
                return False, pd.DataFrame(), True

            for period in STOCK_SMA:
                df[f'sma_{period}'] = df['close'].rolling(window=period).mean()

            if atr:
                df['atr'] = calculate_atr(df, period=ATR_PERIOD)

            if dropna:
                df = df.dropna()

            return True, df, True

        except Exception as e:
            print(f"Error downloading stock data for {ticker}: {e}")
            return False, pd.DataFrame(), False

    def get_all_tickers(self):
        ss = StockSymbol(self.api_keys["stocksymbol"])
        symbol_list = ss.get_symbol_list(market="US")
        return [s["symbol"] for s in symbol_list]


class CryptoDownloader:
    """Async crypto downloader with cache, SQLite, and WebSocket support."""

    def __init__(self, cache_dir=None, config: Optional[dict] = None):
        self.binance_client = Client(requests_params={"timeout": 300})
        self.config = config or {}
        self.cs_config = self.config.get("crypto_screener", {})
        self.async_config = self.cs_config.get("async", {})
        self.ws_config = self.cs_config.get("websocket", {})

        if cache_dir is None:
            current_file_path = Path(__file__).resolve()
            project_root = current_file_path.parent.parent
            self.cache_dir = project_root / "data_cache"
        else:
            self.cache_dir = Path(cache_dir)

        self.cache_dir.mkdir(exist_ok=True)

        self.use_sqlite = self.async_config.get("use_sqlite_cache", True)
        self.sqlite_path = self.cache_dir / self.async_config.get("sqlite_db_name", "binance_klines.db")
        self.max_concurrency = int(self.async_config.get("max_concurrency", 5))
        self.weight_limit_per_min = int(self.async_config.get("weight_limit_per_min", 1200))
        self.weight_safe_ratio = float(self.async_config.get("weight_safe_ratio", 0.6))
        self.max_retries = int(self.async_config.get("max_retries", 6))
        self.request_interval = float(self.async_config.get("request_interval", 0.15))  # 150ms between requests

        rate_limiter_cfg = self.cs_config.get("rate_limiter", {})
        self.backoff_base_delay = float(rate_limiter_cfg.get("base_delay", 1.0))
        self.backoff_max_delay = float(rate_limiter_cfg.get("max_delay", 30.0))

        self._sem = asyncio.Semaphore(self.max_concurrency)
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_loop: Optional[asyncio.AbstractEventLoop] = None
        self._sync_loop: Optional[asyncio.AbstractEventLoop] = None
        self._used_weight_1m = 0
        self._weight_window_start = time.monotonic()  # Start of current 1-min window
        self._weight_lock = asyncio.Lock()
        self._last_request_time = 0.0  # For inter-request pacing
        self._request_pace_lock = asyncio.Lock()
        self._db_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)

        self.ring_buffer_size = int(self.ws_config.get("ring_buffer_size", 3000))
        self.ring_buffers: Dict[Tuple[str, str], deque] = {}

        if self.use_sqlite:
            self._init_sqlite()

    def _get_cache_path(self, symbol, timeframe):
        return self.cache_dir / f"binance_{symbol}_{timeframe}.pkl"

    def _init_sqlite(self):
        with sqlite3.connect(self.sqlite_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS klines (
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    open_time INTEGER NOT NULL,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume REAL,
                    close_time INTEGER,
                    raw_json TEXT,
                    PRIMARY KEY (symbol, timeframe, open_time)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS kline_state (
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    last_open_time INTEGER,
                    last_close_time INTEGER,
                    updated_at INTEGER,
                    PRIMARY KEY (symbol, timeframe)
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_klines_symbol_tf_time ON klines(symbol, timeframe, open_time)")

    async def _close_session(self):
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None
        self._session_loop = None

    async def _ensure_session(self):
        current_loop = asyncio.get_running_loop()
        needs_new = False

        if self._session is None or self._session.closed:
            needs_new = True
        elif self._session_loop is not current_loop:
            await self._close_session()
            needs_new = True
        else:
            try:
                connector = getattr(self._session, '_connector', None)
                if connector is None or connector.closed:
                    needs_new = True
            except Exception:
                needs_new = True

        if needs_new:
            timeout = aiohttp.ClientTimeout(total=60)
            connector = aiohttp.TCPConnector(
                resolver=aiohttp.resolver.ThreadedResolver(),
                ssl=False  # Disable SSL verification to avoid cert issues
            )
            self._session = aiohttp.ClientSession(timeout=timeout, connector=connector)
            self._session_loop = current_loop

    async def aclose(self):
        await self._close_session()
        self._db_executor.shutdown(wait=False)

        if self._sync_loop is not None and not self._sync_loop.is_closed():
            self._sync_loop.close()
            self._sync_loop = None

    def close(self):
        """Sync close helper for non-async callers."""
        if self._sync_loop is not None and not self._sync_loop.is_closed():
            self._sync_loop.run_until_complete(self.aclose())
        else:
            try:
                asyncio.run(self.aclose())
            except RuntimeError:
                # Best effort in environments with active loop.
                pass

    def __del__(self):
        # Best-effort cleanup to reduce "Unclosed client session" warnings.
        try:
            self.close()
        except Exception:
            pass

    def _check_weight_window(self):
        """Reset weight if the 1-min window has passed. Must be called under _weight_lock."""
        now = time.monotonic()
        if now - self._weight_window_start >= 60.0:
            self._used_weight_1m = 0
            self._weight_window_start = now

    async def _pace_request(self):
        """Ensure minimum interval between requests (anti-burst)."""
        async with self._request_pace_lock:
            now = time.monotonic()
            elapsed = now - self._last_request_time
            if elapsed < self.request_interval:
                await asyncio.sleep(self.request_interval - elapsed)
            self._last_request_time = time.monotonic()

    async def _reserve_request_weight(self, request_weight: int):
        """Block until we have budget for this request within the safe threshold."""
        safe_threshold = self.weight_limit_per_min * self.weight_safe_ratio
        estimated = max(1, int(request_weight))
        max_wait_rounds = 300
        for i in range(max_wait_rounds):
            async with self._weight_lock:
                self._check_weight_window()
                if self._used_weight_1m + estimated <= safe_threshold:
                    self._used_weight_1m += estimated
                    return
            # Weight budget full — wait for window to reset
            wait = max(0.5, 60.0 - (time.monotonic() - self._weight_window_start) + 1.0)
            wait = min(wait, 10.0)  # Check every 10s at most
            print(f"[RateLimit] Weight {self._used_weight_1m}/{safe_threshold:.0f} full, waiting {wait:.1f}s for window reset")
            await asyncio.sleep(wait)
        print(f"[RateLimit] WARNING: Weight reservation timed out (weight={self._used_weight_1m}), proceeding anyway")

    async def _request_json(self, path: str, params: dict, request_weight: int = 1):
        await self._ensure_session()

        for attempt in range(self.max_retries):
            async with self._sem:
                await self._reserve_request_weight(request_weight)
                await self._pace_request()
                try:
                    async with self._session.get(f"{BINANCE_FUTURES_BASE}{path}", params=params) as resp:
                        used_weight = resp.headers.get("X-MBX-USED-WEIGHT-1M")
                        if used_weight is not None:
                            try:
                                server_weight = int(used_weight)
                                async with self._weight_lock:
                                    # Trust server's reported weight (always >= our tracking)
                                    if server_weight > self._used_weight_1m:
                                        self._used_weight_1m = server_weight
                            except ValueError:
                                pass

                        if resp.status == 200:
                            return await resp.json()

                        if resp.status == 429:
                            backoff = self.backoff_base_delay * (2 ** attempt)
                            jitter = random.uniform(0, backoff * 0.5)
                            sleep_time = min(backoff + jitter, self.backoff_max_delay)
                            print(f"[RateLimit] 429 on {params.get('symbol','?')}, attempt {attempt+1}, sleeping {sleep_time:.1f}s (weight={self._used_weight_1m})")
                            await asyncio.sleep(sleep_time)
                            continue

                        if resp.status == 418:
                            retry_after = resp.headers.get("Retry-After")
                            sleep_sec = float(retry_after) if retry_after else 60.0
                            print(f"[RateLimit] 418 IP BAN on {params.get('symbol','?')}, sleeping {sleep_sec:.0f}s")
                            # Reset weight tracking — we're banned, window is meaningless
                            async with self._weight_lock:
                                self._used_weight_1m = 0
                                self._weight_window_start = time.monotonic()
                            await asyncio.sleep(sleep_sec)
                            continue

                        if resp.status >= 500:
                            backoff = self.backoff_base_delay * (2 ** attempt)
                            jitter = random.uniform(0, backoff * 0.5)
                            sleep_time = min(backoff + jitter, max(5.0, self.backoff_max_delay / 2))
                            print(f"[Error] Server {resp.status} on {params.get('symbol','?')}, attempt {attempt+1}, sleeping {sleep_time:.1f}s")
                            await asyncio.sleep(sleep_time)
                            continue

                        text = await resp.text()
                        raise RuntimeError(f"Binance API error {resp.status}: {text}")
                except aiohttp.ClientError as e:
                    # Check for SSL errors and recreate session if needed
                    error_str = str(e).lower()
                    is_ssl_error = any(s in error_str for s in ['ssl', 'transport', 'certificate', 'handshake'])
                    
                    if is_ssl_error:
                        print(f"[SSL Error] {params.get('symbol','?')}: {e}, recreating session")
                        await self._close_session()

                    backoff = self.backoff_base_delay * (2 ** attempt)
                    jitter = random.uniform(0, backoff * 0.5)
                    sleep_time = min(backoff + jitter, self.backoff_max_delay)
                    print(f"[Error] ClientError on {params.get('symbol','?')}: {e}, attempt {attempt+1}, sleeping {sleep_time:.1f}s")
                    await asyncio.sleep(sleep_time)
                    continue

        raise RuntimeError(f"Binance API retries exhausted for path={path}, params={params}")

    def _save_pickle_klines(self, symbol: str, timeframe: str, klines: List[list]):
        cache_path = self._get_cache_path(symbol, timeframe)
        unique_klines = {k[0]: k for k in klines}
        sorted_klines = [unique_klines[ts] for ts in sorted(unique_klines.keys())]
        with open(cache_path, 'wb') as f:
            pickle.dump(sorted_klines, f)

    def _load_pickle_klines(self, symbol: str, timeframe: str) -> List[list]:
        cache_path = self._get_cache_path(symbol, timeframe)
        if not cache_path.exists():
            return []
        try:
            with open(cache_path, 'rb') as f:
                return pickle.load(f)
        except Exception:
            return []

    def _upsert_sqlite_klines_sync(self, symbol: str, timeframe: str, klines: List[list]):
        if not klines:
            return
        rows = []
        for k in klines:
            rows.append((
                symbol,
                timeframe,
                int(k[0]),
                float(k[1]),
                float(k[2]),
                float(k[3]),
                float(k[4]),
                float(k[5]),
                int(k[6]),
                json.dumps(k),
            ))

        last_open = int(klines[-1][0])
        last_close = int(klines[-1][6])
        now = int(time.time())

        with sqlite3.connect(self.sqlite_path) as conn:
            conn.executemany(
                """
                INSERT INTO klines(symbol, timeframe, open_time, open, high, low, close, volume, close_time, raw_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, timeframe, open_time)
                DO UPDATE SET
                    open=excluded.open,
                    high=excluded.high,
                    low=excluded.low,
                    close=excluded.close,
                    volume=excluded.volume,
                    close_time=excluded.close_time,
                    raw_json=excluded.raw_json
                """,
                rows,
            )
            conn.execute(
                """
                INSERT INTO kline_state(symbol, timeframe, last_open_time, last_close_time, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(symbol, timeframe)
                DO UPDATE SET
                    last_open_time=excluded.last_open_time,
                    last_close_time=excluded.last_close_time,
                    updated_at=excluded.updated_at
                """,
                (symbol, timeframe, last_open, last_close, now),
            )

    def _query_sqlite_klines_sync(self, symbol: str, timeframe: str, start_ms: int, end_ms: int) -> List[list]:
        with sqlite3.connect(self.sqlite_path) as conn:
            rows = conn.execute(
                """
                SELECT raw_json FROM klines
                WHERE symbol=? AND timeframe=? AND open_time>=? AND open_time<=?
                ORDER BY open_time ASC
                """,
                (symbol, timeframe, int(start_ms), int(end_ms)),
            ).fetchall()
        return [json.loads(r[0]) for r in rows]

    def _get_last_cached_open_time_sync(self, symbol: str, timeframe: str) -> Optional[int]:
        if not self.use_sqlite:
            cached = self._load_pickle_klines(symbol, timeframe)
            if not cached:
                return None
            return int(cached[-1][0])

        with sqlite3.connect(self.sqlite_path) as conn:
            row = conn.execute(
                "SELECT last_open_time FROM kline_state WHERE symbol=? AND timeframe=?",
                (symbol, timeframe),
            ).fetchone()
            if row and row[0] is not None:
                return int(row[0])
        return None

    async def _upsert_sqlite_klines(self, symbol: str, timeframe: str, klines: List[list]):
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self._db_executor, self._upsert_sqlite_klines_sync, symbol, timeframe, klines)

    async def _query_sqlite_klines(self, symbol: str, timeframe: str, start_ms: int, end_ms: int) -> List[list]:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._db_executor,
            self._query_sqlite_klines_sync,
            symbol,
            timeframe,
            start_ms,
            end_ms,
        )

    async def _get_last_cached_open_time(self, symbol: str, timeframe: str) -> Optional[int]:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._db_executor, self._get_last_cached_open_time_sync, symbol, timeframe)

    async def get_all_symbols_async(self):
        data = await self._request_json("/fapi/v1/exchangeInfo", {}, request_weight=40)
        symbols = []
        for item in data.get("symbols", []):
            symbol_name = item.get("pair") or item.get("symbol")
            if symbol_name and symbol_name.endswith("USDT"):
                symbols.append(symbol_name)
        return sorted(list(set(symbols)))

    def get_all_symbols(self):
        binance_response = self.binance_client.futures_exchange_info()
        binance_symbols = set()
        for item in binance_response["symbols"]:
            symbol_name = item.get("pair") or item.get("symbol")
            if symbol_name and symbol_name.endswith("USDT"):
                binance_symbols.add(symbol_name)
        return sorted(list(binance_symbols))

    def _validate_data_quality(self, df: pd.DataFrame) -> bool:
        if df.empty:
            return False

        latest_ts = df['timestamp'].max()
        week_ago = time.time() - (7 * 24 * 3600)
        if latest_ts < week_ago:
            return False

        consecutive_same_price = df['close'].rolling(window=10).apply(lambda x: len(set(x)) == 1)
        if consecutive_same_price.any():
            return False

        return True

    def _build_dataframe_from_klines(self, klines: List[list], dropna=True, atr=True, validate=True):
        if not klines:
            return False, pd.DataFrame()

        df = pd.DataFrame(
            klines,
            columns=[
                "Datetime", "Open Price", "High Price", "Low Price", "Close Price",
                "Volume", "Close Time", "Quote Volume", "Number of Trades",
                "Taker buy base asset volume", "Taker buy quote asset volume", "Ignore",
            ],
        )

        if df.empty:
            return False, pd.DataFrame()

        df['timestamp'] = df['Datetime'].values.astype(np.int64) // 1000
        df['open'] = df['Open Price'].astype(np.float64)
        df['high'] = df['High Price'].astype(np.float64)
        df['low'] = df['Low Price'].astype(np.float64)
        df['close'] = df['Close Price'].astype(np.float64)
        df['volume'] = df['Volume'].astype(np.float64)
        df = df.drop_duplicates(subset=['timestamp'], keep='last').sort_values('timestamp')

        if validate and not self._validate_data_quality(df):
            return False, pd.DataFrame()

        for duration in CRYPTO_SMA:
            df[f"sma_{duration}"] = df['close'].rolling(window=duration).mean().astype(np.float64)

        if atr:
            df['atr'] = calculate_atr(df, period=ATR_PERIOD).astype(np.float64)

        if dropna:
            df = df.dropna()

        return True, df

    async def get_data_async(self, crypto, start_ts=None, end_ts=None, timeframe="4h", dropna=True, atr=True, validate=True) -> tuple[bool, pd.DataFrame, bool]:
        try:
            if end_ts is None:
                end_ts = int(time.time())

            end_ts_ms = int(end_ts * 1000)
            interval_ms = timeframe_to_ms(timeframe)

            if start_ts is None:
                # Default to recent 1500 candles when start_ts is not provided
                start_ts = int((end_ts_ms - 1500 * interval_ms) / 1000)
            max_sma = max(CRYPTO_SMA)
            extension_ms = int(max_sma * interval_ms * 1.2)
            extended_start_ts_ms = int(start_ts * 1000 - extension_ms)

            cached_klines = []
            if self.use_sqlite:
                cached_klines = await self._query_sqlite_klines(crypto, timeframe, extended_start_ts_ms, end_ts_ms)
            else:
                all_pickled = self._load_pickle_klines(crypto, timeframe)
                cached_klines = [k for k in all_pickled if extended_start_ts_ms <= int(k[0]) <= end_ts_ms]

            last_open = await self._get_last_cached_open_time(crypto, timeframe)
            fetch_from = extended_start_ts_ms
            if last_open is not None:
                fetch_from = max(fetch_from, int(last_open) + interval_ms)

            fetched = []
            new_data_fetched = False

            while fetch_from < (end_ts_ms - interval_ms):
                params = {
                    "symbol": crypto,
                    "interval": timeframe,
                    "startTime": int(fetch_from),
                    "endTime": int(end_ts_ms),
                    "limit": 1500,
                }
                response = await self._request_json("/fapi/v1/klines", params, request_weight=2)
                if not response:
                    break

                fetched.extend(response)
                new_data_fetched = True
                next_from = int(response[-1][6]) + 1
                if next_from <= fetch_from:
                    break
                fetch_from = next_from

                if len(response) < 1500:
                    break

            merged_map = {int(k[0]): k for k in cached_klines}
            for k in fetched:
                merged_map[int(k[0])] = k
            merged = [merged_map[k] for k in sorted(merged_map.keys()) if extended_start_ts_ms <= k <= end_ts_ms]

            if new_data_fetched:
                if self.use_sqlite:
                    await self._upsert_sqlite_klines(crypto, timeframe, fetched)
                else:
                    self._save_pickle_klines(crypto, timeframe, list(merged_map.values()))

            success, df = self._build_dataframe_from_klines(merged, dropna=dropna, atr=atr, validate=validate)
            return success, df, new_data_fetched

        except Exception as e:
            print(f"[Error] get_data_async({crypto}) failed: {e}")
            return False, pd.DataFrame(), False

    def _get_or_create_sync_loop(self) -> asyncio.AbstractEventLoop:
        if self._sync_loop is None or self._sync_loop.is_closed():
            self._sync_loop = asyncio.new_event_loop()
        return self._sync_loop

    def get_data(self, crypto, start_ts=None, end_ts=None, timeframe="4h", dropna=True, atr=True, validate=True) -> tuple[bool, pd.DataFrame, bool]:
        """Synchronous wrapper using one shared event loop per instance."""
        try:
            in_running_loop = False
            try:
                asyncio.get_running_loop()
                in_running_loop = True
            except RuntimeError:
                in_running_loop = False

            if in_running_loop:
                raise RuntimeError("get_data() cannot be called inside a running event loop; use await get_data_async(...)")

            loop = self._get_or_create_sync_loop()
            return loop.run_until_complete(
                self.get_data_async(crypto, start_ts, end_ts, timeframe, dropna, atr, validate)
            )
        except Exception as e:
            print(f"[Error] get_data({crypto}) failed: {e}")
            return False, pd.DataFrame(), False

    def batch_get_data(
        self,
        symbols: List[str],
        start_ts: int,
        end_ts: int,
        timeframe: str = "4h",
        validate: bool = True,
    ) -> dict:
        """Concurrently fetch data for multiple symbols using the same cache and semaphore.

        Returns a dict mapping symbol -> (ok, df, fetched_from_network).
        """
        async def _run_all():
            # _request_json already acquires self._sem for rate limiting;
            # do NOT wrap get_data_async in another self._sem or it deadlocks.
            async def fetch_one(symbol):
                return symbol, await self.get_data_async(
                    symbol, start_ts=start_ts, end_ts=end_ts,
                    timeframe=timeframe, validate=validate
                )
            tasks = [fetch_one(s) for s in symbols]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            output = {}
            for item in results:
                if isinstance(item, Exception):
                    continue
                sym, (ok, df, fetched) = item
                output[sym] = (ok, df, fetched)
            return output

        loop = self._get_or_create_sync_loop()
        return loop.run_until_complete(_run_all())

    async def stream_klines(self, symbols: List[str], timeframe: str, on_kline=None, stop_event: Optional[asyncio.Event] = None):
        """P2: Stream kline data via WebSocket (max 1024 streams)."""
        if len(symbols) > 1024:
            raise ValueError("?®ä? WebSocket ????å¤?1024 streamsï¼è??æ¹???")

        # ?å??ç¨ REST ?å¡«ï¼é¿??ring buffer ?¯ç©º?ï?
        backfill_tasks = [self.get_data_async(s, timeframe=timeframe, validate=False) for s in symbols]
        await asyncio.gather(*backfill_tasks, return_exceptions=True)

        streams = "/".join([f"{s.lower()}@kline_{timeframe}" for s in symbols])
        ws_url = f"{BINANCE_WS_BASE}/stream?streams={streams}"

        await self._ensure_session()
        async with self._session.ws_connect(ws_url, heartbeat=20) as ws:
            async for msg in ws:
                if stop_event and stop_event.is_set():
                    break
                if msg.type != aiohttp.WSMsgType.TEXT:
                    continue

                data = json.loads(msg.data)
                stream_data = data.get("data", {})
                kline = stream_data.get("k", {})
                symbol = stream_data.get("s")
                if not symbol or not kline:
                    continue

                # ?ªè??å·²?¶ç¤ K ç·ï??¿å??è??å?
                if not kline.get("x", False):
                    continue

                candle = [
                    int(kline.get("t", 0)),
                    kline.get("o", "0"),
                    kline.get("h", "0"),
                    kline.get("l", "0"),
                    kline.get("c", "0"),
                    kline.get("v", "0"),
                    int(kline.get("T", 0)),
                    kline.get("q", "0"),
                    int(kline.get("n", 0)),
                    kline.get("V", "0"),
                    kline.get("Q", "0"),
                    "0",
                ]

                key = (symbol, timeframe)
                if key not in self.ring_buffers:
                    self.ring_buffers[key] = deque(maxlen=self.ring_buffer_size)
                self.ring_buffers[key].append(candle)

                if self.use_sqlite:
                    await self._upsert_sqlite_klines(symbol, timeframe, [candle])
                else:
                    existing = self._load_pickle_klines(symbol, timeframe)
                    existing.append(candle)
                    self._save_pickle_klines(symbol, timeframe, existing)

                if on_kline is not None:
                    await on_kline(symbol, timeframe, candle)

