import json
import re
import os
import time
import pickle
import pandas as pd
import numpy as np
from datetime import datetime
from pytz import timezone
from stocksymbol import StockSymbol
from polygon import RESTClient
from urllib3.util.retry import Retry
from binance import Client
from pathlib import Path


# Configurable parameters
STOCK_SMA = [20, 30, 45, 50, 60, 150, 200]
CRYPTO_SMA = [30, 45, 60]
ATR_PERIOD = 60  


def calculate_atr(df, period=ATR_PERIOD):
    """
    Calculate Average True Range (ATR) for the given dataframe
    
    Args:
        df: DataFrame containing 'high', 'low', 'close' columns
        period: Period for ATR calculation (default: ATR_PERIOD)
        
    Returns:
        Series containing ATR values
    """
    high = df['high']
    low = df['low']
    close = df['close']
    
    # Calculate True Range
    tr1 = high - low
    tr2 = abs(high - close.shift())
    tr3 = abs(low - close.shift())
    
    # Get the maximum of the three price ranges
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    
    # Calculate ATR as the simple moving average of True Range
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
    unit_match = {
        "m": "minute",
        "h": "hour",
        "d": "day"
    }
    return number, unit_match[unit]


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
            respect_retry_after_header=True
        )

        self.client = RESTClient(
            api_key=self.api_keys["polygon"],
            num_pools=100,
            connect_timeout=1.0,
            read_timeout=1.0,
            retries=10
        )

    def _validate_data_quality(self, df: pd.DataFrame) -> bool:
        """
        Validate data quality
        - Check if latest data is within a week
        - Check for stale prices (same closing price for 10+ consecutive periods)
        """
        if df.empty:
            return False

        # Check data freshness
        latest_ts = df['timestamp'].max()
        week_ago = time.time() - (7 * 24 * 3600)
        if latest_ts < week_ago:
            return False

        # Check for stale prices
        consecutive_same_price = df['close'].rolling(window=10).apply(
            lambda x: len(set(x)) == 1
        )
        if consecutive_same_price.any():
            return False

        return True

    def get_data(self, ticker: str, start_ts: int, end_ts: int = None, timeframe: str = "1d", dropna=True, atr=True, validate=True) -> tuple[bool, pd.DataFrame, bool]:
        """
        Get stock data with SMA calculation and data quality validation
        Returns:
            (success, DataFrame, fetched_from_network)
        """
        try:
            # Setup timeframe mapping
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

            # Calculate extension needed for SMAs
            max_sma = max(STOCK_SMA)
            fc = 1.3 if timeframe == "1d" else 0.6
            extension = np.int64(max_sma * 24 * 3600 * fc)
            extended_start = np.int64(start_ts - extension)

            # Default end_ts to current time if not provided
            if end_ts is None:
                end_ts = np.int64(time.time())

            # Fetch aggregates from Polygon
            aggs = self.client.get_aggs(
                ticker,
                multiplier,
                timespan,
                np.int64(extended_start * 1000),
                np.int64(end_ts * 1000),
                limit=10000
            )

            if not aggs:
                return False, pd.DataFrame(), False

            # Convert to DataFrame with timestamp
            df = pd.DataFrame([{
                'timestamp': np.int64(agg.timestamp // 1000),
                'open': np.float64(agg.open),
                'close': np.float64(agg.close),
                'high': np.float64(agg.high),
                'low': np.float64(agg.low),
                'volume': np.float64(agg.volume)
            } for agg in aggs])

            if df.empty:
                return False, df

            # Sort by timestamp
            df = df.sort_values('timestamp')

            # Filter market hours (9:00 AM - 4:00 PM NY time)
            if timespan == "hour" or timespan == "minute":
                # Create temporary datetime column in NY timezone for filtering
                ny_tz = timezone('America/New_York')
                temp_dt = pd.to_datetime(df['timestamp'], unit='s', utc=True).dt.tz_convert(ny_tz)
                
                # Create filter based on NY market hours
                if timespan == "hour":
                    market_hours_filter = temp_dt.dt.time.between(
                        pd.to_datetime('09:00').time(),
                        pd.to_datetime('16:00').time(),
                        inclusive='left'
                    )
                else:  # minute timeframe
                    market_hours_filter = temp_dt.dt.time.between(
                        pd.to_datetime('09:30').time(),
                        pd.to_datetime('16:00').time(),
                        inclusive='left'
                    )
                
                # Apply filter and drop temporary column
                df = df[market_hours_filter]

            # Validate data quality
            if validate and not self._validate_data_quality(df):
                return False, pd.DataFrame(), True

            # Calculate SMAs
            for period in STOCK_SMA:
                df[f'sma_{period}'] = df['close'].rolling(window=period).mean()

            # Calculate ATR if requested
            if atr:
                df['atr'] = calculate_atr(df, period=ATR_PERIOD)

            # Drop rows with NaN if requested
            if dropna:
                df = df.dropna()

            return True, df, True

        except Exception as e:
            print(f"Error downloading stock data for {ticker}: {e}")
            return False, pd.DataFrame(), False

    def get_all_tickers(self):
        """
        Get all available stock tickers
        """
        ss = StockSymbol(self.api_keys["stocksymbol"])
        symbol_list = ss.get_symbol_list(market="US")
        return [s["symbol"] for s in symbol_list]


class CryptoDownloader:
    def __init__(self, cache_dir=None):
        self.binance_client = Client(requests_params={"timeout": 300})
        if cache_dir is None:
            # Get the directory where this script is located
            current_file_path = Path(__file__).resolve()
            # src is the parent, screener is the parent of src
            project_root = current_file_path.parent.parent
            self.cache_dir = project_root / "data_cache"
        else:
            self.cache_dir = Path(cache_dir)
        
        self.cache_dir.mkdir(exist_ok=True)

    def _get_cache_path(self, symbol, timeframe):
        return self.cache_dir / f"binance_{symbol}_{timeframe}.pkl"

    def get_all_symbols(self):
        """
        Get all USDT pairs in binance
        """
        binance_response = self.binance_client.futures_exchange_info()
        binance_symbols = set()
        for item in binance_response["symbols"]:
            symbol_name = item["pair"]
            if symbol_name[-4:] == "USDT":
                binance_symbols.add(symbol_name)
        return sorted(list(binance_symbols))

    def _validate_data_quality(self, df: pd.DataFrame) -> bool:
        """
        Validate crypto data quality
        - Check if latest data is within a week
        - Check for stale prices (same closing price for 10+ consecutive periods)
        """
        if df.empty:
            return False

        # Check data freshness
        latest_ts = df['timestamp'].max()
        week_ago = time.time() - (7 * 24 * 3600)
        if latest_ts < week_ago:
            return False

        # Check for stale prices
        consecutive_same_price = df['close'].rolling(window=10).apply(
            lambda x: len(set(x)) == 1
        )
        if consecutive_same_price.any():
            return False

        return True

    def get_data(self, crypto, start_ts=None, end_ts=None, timeframe="4h", dropna=True, atr=True, validate=True) -> tuple[bool, pd.DataFrame, bool]:
        """
        Get cryptocurrency data with SMA calculation and data quality validation
        Returns:
            (success, DataFrame, fetched_from_network)
        """
        try:
            # Default end_ts to current time if not provided
            if end_ts is None:
                end_ts = np.int64(time.time())
            
            # Convert to milliseconds for Binance API
            end_ts_ms = np.int64(end_ts * 1000)
            
            # Load cache
            cache_path = self._get_cache_path(crypto, timeframe)
            cached_klines = []
            if cache_path.exists():
                try:
                    with open(cache_path, 'rb') as f:
                        cached_klines = pickle.load(f)
                except:
                    cached_klines = []

            new_data_fetched = False
            if start_ts is None:
                # Fetch only the latest 1500 datapoints
                response = self.binance_client.futures_klines(
                    symbol=crypto,
                    interval=timeframe,
                    limit=1500
                )
                all_data = response
                new_data_fetched = True
            else:
                # Calculate extended start for SMA calculation
                max_sma = max(CRYPTO_SMA) 
                
                # Calculate number of time intervals in max_sma
                num_intervals, unit = parse_time_string(timeframe)
                if unit == "minute":
                    interval_seconds = np.int64(num_intervals * 60)
                elif unit == "hour":
                    interval_seconds = np.int64(num_intervals * 3600)
                else:  # day
                    interval_seconds = np.int64(num_intervals * 86400)
                
                # Calculate extension in milliseconds (number of bars needed for max SMA)
                extension_ms = np.int64(max_sma * interval_seconds * 1000 * 1.2)  # 20% buffer
                
                # Extended start timestamp with buffer for SMA calculation
                extended_start_ts_ms = np.int64(start_ts * 1000 - extension_ms)
                
                # Check what's in cache
                existing_in_range = [k for k in cached_klines if k[0] >= extended_start_ts_ms and k[0] <= end_ts_ms]
                
                all_data = existing_in_range
                
                # If cache is empty or doesn't cover the end, fetch missing
                current_timestamp = extended_start_ts_ms
                if all_data:
                    current_timestamp = all_data[-1][6] + 1 # Use close_time of last kline
                
                # Only fetch if current_timestamp is significantly before end_ts_ms
                # At least one full candle gap
                if current_timestamp < (end_ts_ms - (interval_seconds * 1000)):
                    while current_timestamp < end_ts_ms:
                        response = self.binance_client.futures_klines(
                            symbol=crypto,
                            interval=timeframe,
                            startTime=np.int64(current_timestamp),
                            endTime=np.int64(end_ts_ms),
                            limit=1500
                        )

                        if not response:
                            break

                        all_data.extend(response)
                        new_data_fetched = True
                        
                        if response:
                            current_timestamp = np.int64(response[-1][6]) + 1
                        else:
                            break

            if not all_data:
                return False, pd.DataFrame(), False

            # Update cache ONLY if new data was fetched
            if new_data_fetched:
                unique_klines = {k[0]: k for k in cached_klines}
                for k in all_data:
                    unique_klines[k[0]] = k
                
                sorted_klines = [unique_klines[ts] for ts in sorted(unique_klines.keys())]
                with open(cache_path, 'wb') as f:
                    pickle.dump(sorted_klines, f)
                final_klines = sorted_klines
            else:
                final_klines = cached_klines

            if start_ts is not None:
                final_data = [k for k in final_klines if k[0] >= extended_start_ts_ms and k[0] <= end_ts_ms]
            else:
                final_data = final_klines[-1500:]

            df = pd.DataFrame(final_data, 
                            columns=["Datetime", "Open Price", "High Price", "Low Price", "Close Price",
                                    "Volume", "Close Time", "Quote Volume", "Number of Trades",
                                    "Taker buy base asset volume", "Taker buy quote asset volume", "Ignore"])
            
            if df.empty:
                return False, pd.DataFrame(), False
            
            df['timestamp'] = df['Datetime'].values.astype(np.int64) // 1000
            df['open'] = df['Open Price'].astype(np.float64)
            df['high'] = df['High Price'].astype(np.float64)
            df['low'] = df['Low Price'].astype(np.float64)
            df['close'] = df['Close Price'].astype(np.float64)
            df['volume'] = df['Volume'].astype(np.float64)
            df = df.drop_duplicates(subset=['timestamp'], keep='first')
            df = df.sort_values('timestamp')

            if validate and not self._validate_data_quality(df):
                return False, pd.DataFrame(), new_data_fetched
            
            for duration in CRYPTO_SMA:
                df[f"sma_{duration}"] = df['close'].rolling(window=duration).mean().astype(np.float64)

            if atr:
                df['atr'] = calculate_atr(df, period=ATR_PERIOD).astype(np.float64)

            if dropna:
                df = df.dropna()
            
            return True, df, new_data_fetched

        except Exception as e:
            return False, pd.DataFrame(), False
