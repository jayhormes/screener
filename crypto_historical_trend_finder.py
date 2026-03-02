"""
Cryptocurrency Similar Pattern Finder using Dynamic Time Warping (DTW)
======================================================================
This script identifies cryptocurrency price patterns that are similar to predefined reference trends
using Dynamic Time Warping (DTW) and Shape-based DTW algorithms. It searches historical data
across multiple timeframes to find patterns matching reference trends, then analyzes their future
price movements to provide statistical insights for trading strategies.

KEY FEATURES:
- Pattern Matching: Uses DTW and ShapeDTW algorithms to find similar price patterns
- Multi-timeframe Analysis: Searches across 15m, 30m, 1h, 2h, 4h timeframes
- Future Trend Prediction: Analyzes price movements after pattern completion (rise/fall statistics)
- Comprehensive Visualization: Generates detailed candlestick charts with volume and moving averages
- Statistical Analysis: Provides detailed statistics for different extension factors (0.25x to 2.5x)
- Parallel Processing: Uses multiprocessing for efficient computation
- Data Caching: Caches downloaded data by timeframe to avoid redundant API calls
- Non-overlapping Filtering: Removes overlapping patterns for cleaner analysis
- Extensible Configuration: Easy to add new reference trends and adjust parameters

USAGE:
# Basic usage with default parameters
python crypto_historical_trend_finder.py

# Custom parameters
python crypto_historical_trend_finder.py -k 500 -s 10

Example workflow:
1. Define reference trends (e.g., AVAX uptrend from Nov 9-14, 2023)
2. Script downloads historical data for 200+ cryptocurrencies
3. Finds similar patterns using DTW similarity matching
4. Analyzes future price movements after each pattern
5. Generates visualizations and statistical reports
6. Output: "75% of similar patterns resulted in price rises within 2x pattern length"
"""

import os
import time
import json
import numpy as np
import pandas as pd
from datetime import datetime
from multiprocessing import Pool, cpu_count
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import argparse
from src.downloader import CryptoDownloader
from src.discord_notifier import get_trend_finder_discord_notifier
from src.message_formatter import TrendFinderMessageFormatter
from src.common import (
   TrendAnalysisConfig,
   DataNormalizer,
   DTWCalculator,
   FileManager,
   ReferenceDataManager,
   DataCacheManager,
   BaseDataProcessor,
   create_output_directory,
   filter_non_overlapping_results,
   plot_candlesticks_with_volume,
   format_dt_with_tz
)


def load_json_config(config_path="config.json"):
    """Load config from JSON file"""
    try:
        if os.path.exists(config_path):
            with open(config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception as e:
        print(f"Config file {config_path} not found or error: {e}")
    return {}


# ================ Configuration ================
# Reference trends definition
REFERENCE_TRENDS = {
   "AVAX": [
       [datetime(2023, 11, 10, 22, 0), datetime(2023, 11, 14, 8, 0), "1h", "stage_0"],
#       [datetime(2023, 11, 10, 22, 0), datetime(2023, 11, 15, 2, 0), "1h", "stage_1"],
#       [datetime(2023, 11, 10, 22, 0), datetime(2023, 11, 15, 15, 0), "1h", "stage_2"],
   ]
}

'''
REFERENCE_TRENDS = {
   "AVAX": [
       [datetime(2023, 11, 9, 12, 0), datetime(2023, 11, 14, 18, 0), "1h", "standard"],
   ],
   "MKR": [
       [datetime(2023, 6, 24, 9, 0), datetime(2023, 7, 18, 5, 0), "4h", "standard"],
   ],
   "CRV": [
       [datetime(2024, 10, 23, 1, 0), datetime(2024, 11, 24, 0, 0), "4h", "uptrend"],
       [datetime(2024, 11, 4, 0, 0), datetime(2024, 11, 21, 0, 0), "4h", "uptrend_1"],
       [datetime(2024, 11, 4, 0, 0), datetime(2024, 11, 29, 0, 0), "4h", "uptrend_2"],
   ],
   "GMT": [
       [datetime(2022, 3, 26, 9, 0), datetime(2022, 4, 14, 21, 0), "4h", "uptrend"]
   ],
   "SOL": [
       [datetime(2023, 9, 21, 1, 0), datetime(2023, 10, 15, 21, 0), "4h", "standard"]
   ],
   "LQTY": [
       [datetime(2025, 5, 7, 5, 0), datetime(2025, 5, 9, 21, 0), "30m", "standard"]
   ],
   "MOODENG": [
       [datetime(2025, 5, 8, 0, 0), datetime(2025, 5, 11, 1, 0), "1h", "standard"]
   ],
}
'''

# Historical starting point (used only if no cached data exists)
HISTORICAL_START_DATE = datetime(2021, 1, 1)

# Timezone for datetime conversion
TIMEZONE = "America/Los_Angeles"

# Timeframes to analyze
#TIMEFRAMES_TO_ANALYZE = ["15m", "30m", "1h", "2h", "4h"]
TIMEFRAMES_TO_ANALYZE = ["30m"]

# Main output directory
OUTPUT_DIR = "historical_trend_finder_reports"

# Top K results to keep per reference trend
TOP_K = 300

# API request parameters
API_SLEEP_SECONDS = 15

# Overlap filtering strategy
# True: Global filtering - no overlaps across all symbols
# False: Per-symbol filtering - allow overlaps between different symbols
GLOBAL_OVERLAP_FILTERING = True

# DTW parameters
DTW_WINDOW_RATIO = 0.12
DTW_MAX_POINT_DISTANCE = 0.6
DTW_WINDOW_RATIO_FOR_DIFF = 0.1
DTW_MAX_POINT_DISTANCE_FOR_DIFF = 0.5

# ShapeDTW parameters
SHAPEDTW_BALANCE_PD_RATIO = 4
PRICE_WEIGHT = 0.6
DIFF_WEIGHT = 0.4
SLOPE_WINDOW_SIZE = 5
PAA_WINDOW_SIZE = 5

# Window scaling factors to test
WINDOW_SCALE_FACTORS = [0.9, 0.95, 1.0, 1.05, 1.1]

# SMA periods for comparison
SMA_PERIODS = [30, 45, 60]

# Sliding window step size (as fraction of reference trend length)
SLIDING_WINDOW_STEP_RATIO = 0.11

# Minimum similarity score to consider (score threshold)
MIN_SIMILARITY_SCORE = 0.25

# Extension visualization parameters
VIS_EXTENSION_PAST_LENGTH_FACTOR = 1.0
VIS_EXTENSION_FUTURE_LENGTH_FACTOR = 2.0

# Extension factors for statistics
EXTENSION_FACTORS_FOR_STATS = [0.25, 0.5, 0.75, 1.0, 1.5, 2.0, 2.5]

# ================ Data Processing Classes ================

class DataProcessor(BaseDataProcessor):
    """Data processor for cryptocurrency analysis with caching support"""
   
    def __init__(self, config: TrendAnalysisConfig = None, json_config: dict = None):
        """Initialize data processor for crypto analysis"""
        super().__init__("crypto", config.sma_periods if config else None)
        
        # Load JSON config for CryptoDownloader optimization
        json_cfg = json_config or load_json_config()
        self.downloader = CryptoDownloader(config=json_cfg)
        
        self.config = config or TrendAnalysisConfig()

    def get_data(self, symbol: str, timeframe: str, start_ts: int, end_ts: int, 
                include_buffer: bool = True, is_reference: bool = False) -> tuple[pd.DataFrame, bool]:
        """Get data with buffer period for SMA calculation"""
        if include_buffer:
            # Calculate buffer period for SMA calculation
            interval = end_ts - start_ts
            buffer_start_ts = start_ts - interval
        else:
            buffer_start_ts = start_ts

        # For crypto, add USDT if not already there
        if not symbol.endswith("USDT"):
            symbol_full = f"{symbol}USDT"
        else:
            symbol_full = symbol
            
        # Set validate=False for reference trends, otherwise use default (True)
        success, df, fetched_from_network = self.downloader.get_data(
            symbol_full,
            buffer_start_ts,
            end_ts,
            validate=not is_reference,  # Disable validation for reference trends
            timeframe=timeframe
        )

        if not success or df is None or df.empty:
            print(f"Failed to get data for {symbol} ({timeframe})")
            return pd.DataFrame(), False

        # Filter to requested time range
        start_time = pd.Timestamp.fromtimestamp(start_ts)
        end_time = pd.Timestamp.fromtimestamp(end_ts)
        
        # Use the processor from common to prepare the dataframe
        df = self.processor.prepare_dataframe(df)
        
        # Filter to requested time range after preparation
        df = df[(df.index >= start_time) & (df.index <= end_time)]

        return df, fetched_from_network


# ================ DTW Similarity Calculator ================

class DTWSimilarityFinder:
    """Class to find similarity using DTW and ShapeDTW"""
    
    def __init__(self, config: TrendAnalysisConfig):
        """Initialize similarity finder with configuration"""
        self.config = config
        self.dtw_calc = DTWCalculator(config)

    def find_similarity_in_window(self, reference_df: pd.DataFrame, target_df: pd.DataFrame, 
                                window_start_index: int, window_size: int) -> dict:
        """Find similarity between reference trend and target window"""
        # Check window boundaries
        if window_start_index + window_size > len(target_df):
            return {
                "similarity": 0.0,
                "price_distance": float('inf'),
                "diff_distance": float('inf'),
                "price_path": None,
                "diff_path": None,
                "window_data": None,
                "window_info": None
            }
        
        # Extract window
        window = target_df.iloc[window_start_index:window_start_index + window_size]
        
        # Normalize reference and window features
        reference_price_normalized, reference_diff_normalized = self.dtw_calc.normalize_features(reference_df)
        window_price_normalized, window_diff_normalized = self.dtw_calc.normalize_features(window)
        
        # Initial DTW screening for price
        _, price_dtw_distance, _ = self.dtw_calc.calculate_dtw_similarity(
            reference_price_normalized, window_price_normalized, 
            self.config.dtw_window_ratio, self.config.dtw_max_point_distance
        )
        
        # If price distance is too high, return early
        if np.isinf(price_dtw_distance):
            return {
                "similarity": 0.0,
                "price_distance": float('inf'),
                "diff_distance": float('inf'),
                "price_path": None,
                "diff_path": None,
                "window_data": None,
                "window_info": None
            }
        
        # Initial DTW screening for difference
        _, diff_dtw_distance, _ = self.dtw_calc.calculate_dtw_similarity(
            reference_diff_normalized, window_diff_normalized, 
            self.config.dtw_window_ratio_diff, self.config.dtw_max_point_distance_diff
        )
        
        # If difference distance is too high, return early
        if np.isinf(diff_dtw_distance):
            return {
                "similarity": 0.0,
                "price_distance": float('inf'),
                "diff_distance": float('inf'),
                "price_path": None,
                "diff_path": None,
                "window_data": None,
                "window_info": None
            }
        
        # Define shape descriptors
        price_descriptor, diff_descriptor = self.dtw_calc.create_shape_descriptors()
        
        # Calculate ShapeDTW for price
        price_shape_distance, price_shape_path = self.dtw_calc.calculate_shapedtw(
            reference_price_normalized, window_price_normalized, price_descriptor, self.config.dtw_window_ratio
        )
        
        # If no valid path found, return early
        if np.isinf(price_shape_distance):
            return {
                "similarity": 0.0,
                "price_distance": float('inf'),
                "diff_distance": float('inf'),
                "price_path": None,
                "diff_path": None,
                "window_data": None,
                "window_info": None
            }
        
        # Calculate ShapeDTW for difference
        diff_shape_distance, diff_shape_path = self.dtw_calc.calculate_shapedtw(
            reference_diff_normalized, window_diff_normalized, diff_descriptor, self.config.dtw_window_ratio_diff
        )
        
        # If no valid path found, return early
        if np.isinf(diff_shape_distance):
            return {
                "similarity": 0.0,
                "price_distance": float('inf'),
                "diff_distance": float('inf'),
                "price_path": None,
                "diff_path": None,
                "window_data": None,
                "window_info": None
            }
        
        # Calculate final scores
        price_score = 1 / (1 + price_shape_distance)
        diff_score = 1 / (1 + diff_shape_distance * self.config.shapedtw_balance_pd_ratio)
        similarity = (price_score * self.config.price_weight) + (diff_score * self.config.diff_weight)
        
        return {
            "similarity": similarity,
            "price_distance": price_shape_distance,
            "diff_distance": diff_shape_distance,
            "price_path": price_shape_path,
            "diff_path": diff_shape_path,
            "window_data": window,
            "window_info": (window_start_index, window_size)
        }

    def process_target(self, args: tuple) -> dict:
        """Process a single target symbol (for multiprocessing)"""
        reference_df, target_df, symbol, timeframe, reference_symbol, reference_timeframe, reference_label = args
        
        if target_df is None or len(target_df) < len(reference_df):
            return {
                "symbol": symbol,
                "timeframe": timeframe,
                "ref_symbol": reference_symbol,
                "ref_timeframe": reference_timeframe,
                "ref_label": reference_label,
                "result": None
            }
        
        print(f"Processing {symbol} ({timeframe}) against {reference_symbol} ({reference_timeframe}, {reference_label})...")
        
        # Calculate sliding window parameters
        reference_length = len(reference_df)
        step_size = max(1, int(reference_length * SLIDING_WINDOW_STEP_RATIO))
        
        best_result = None
        best_similarity = -1
        
        # Sliding window approach
        max_start_index = len(target_df) - reference_length
        
        for start_index in range(max_start_index, 0, -step_size):
            # Try different window scaling factors
            for factor in self.config.window_scale_factors:
                window_size = int(reference_length * factor)
                
                # Skip if window size exceeds available data
                if start_index + window_size > len(target_df):
                    continue
                
                # Calculate similarity
                result = self.find_similarity_in_window(reference_df, target_df, start_index, window_size)
                
                # Keep only the best result that meets minimum similarity threshold
                if result["similarity"] >= MIN_SIMILARITY_SCORE and result["similarity"] > best_similarity:
                    best_similarity = result["similarity"]
                    best_result = result
                    
                    # Log progress for this result
                    window_period = (
                        f"{format_dt_with_tz(result['window_data'].index[0], TIMEZONE)} to {format_dt_with_tz(result['window_data'].index[-1], TIMEZONE)}"
                        if result['window_data'] is not None else "N/A"
                    )

                    print(f"  New best match for {symbol}: score={result['similarity']:.4f}, "
                        f"price_distance={result['price_distance']:.4f}, "
                        f"diff_distance={result['diff_distance']:.4f}, "
                        f"window={window_period}, factor={factor}")
        
        return {
            "symbol": symbol,
            "timeframe": timeframe,
            "ref_symbol": reference_symbol,
            "ref_timeframe": reference_timeframe,
            "ref_label": reference_label,
            "result": best_result
        }


# ================ Analysis Functions ================

def has_unnatural_volume(window_df: pd.DataFrame) -> bool:
    """Check for Unnatural Volume (突兀量) at the end of a matched pattern"""
    if window_df is None or len(window_df) < 2:
        return False
    required_cols = {'Volume', 'Open', 'Close'}
    if not required_cols.issubset(window_df.columns):
        return False
    last = window_df.iloc[-1]
    prev = window_df.iloc[-2]
    try:
        volume_condition = last['Volume'] >= 5 * prev['Volume']
        bullish_condition = last['Close'] > last['Open']
        return bool(volume_condition and bullish_condition)
    except Exception:
        return False


def determine_position_stage(window_df: pd.DataFrame) -> int:
    """
    Determine the position stage (位階) based on SMA and Price relationships.
    Stage 0: Retracement touching SMA30.
    Stage 1: Breaking and staying steady above SMA30, 45, 60.
    Stage 2: Breaking and staying steady above previous pattern high.
    Returns: stage (0, 1, 2) or -1 if not applicable.
    """
    if window_df is None or len(window_df) < 3:
        return -1
        
    required_cols = {'Close', 'High', 'SMA_30', 'SMA_45', 'SMA_60'}
    if not required_cols.issubset(window_df.columns):
        return -1
        
    last_close = window_df['Close'].iloc[-1]
    last_sma30 = window_df['SMA_30'].iloc[-1]
    last_sma45 = window_df['SMA_45'].iloc[-1]
    last_sma60 = window_df['SMA_60'].iloc[-1]
    
    # Check if "Steady" (站穩): 3 consecutive candles > SMA30 OR SMA30 slope > 0
    recent_closes = window_df['Close'].iloc[-3:]
    recent_sma30s = window_df['SMA_30'].iloc[-3:]
    stayed_steady = (recent_closes > recent_sma30s).all() or (window_df['SMA_30'].iloc[-1] > window_df['SMA_30'].iloc[-2])
    
    # Stage 2: Break previous high and stay steady
    # Previous high of the pattern (excluding the last few bars to check for the break)
    prev_high = window_df['High'].iloc[:-5].max() if len(window_df) > 10 else window_df['High'].max()
    if last_close > prev_high and stayed_steady:
        return 2
        
    # Stage 1: Above all SMAs and stay steady
    if last_close > last_sma30 and last_close > last_sma45 and last_close > last_sma60 and stayed_steady:
        return 1
        
    # Stage 0: Touch SMA30 (within a small threshold)
    # Check if price low or close is near/touching SMA30
    last_low = window_df['Low'].iloc[-1]
    if last_low <= last_sma30 <= window_df['High'].iloc[-1]:
        return 0
        
    return -1


def analyze_future_trend(pattern_df: pd.DataFrame, target_df: pd.DataFrame, 
                       extension_factors: list = None) -> dict:
    """Analyze future trend for different extension factors"""
    if extension_factors is None:
        extension_factors = EXTENSION_FACTORS_FOR_STATS
    
    # Find pattern end in target data
    pattern_end_date = pattern_df.index[-1]
    future_data = target_df[target_df.index > pattern_end_date]
    
    if len(future_data) == 0:
        return {factor: {'trend': 'no_future_data', 'data_points': 0, 'insufficient_data': False} for factor in extension_factors}
    
    pattern_length = len(pattern_df)
    pattern_last_close = pattern_df['Close'].iloc[-1]
    
    results = {}
    
    for factor in extension_factors:
        future_length = int(pattern_length * factor)
        
        if future_length < 1:
            results[factor] = {'trend': 'invalid_factor', 'data_points': 0, 'insufficient_data': False}
            continue
        
        if future_length > len(future_data):
            # If requested length exceeds available data, but some data is available
            if len(future_data) > 0:
                # Use all available future data
                future_sample = future_data
                future_last_close = future_sample['Close'].iloc[-1]
                trend = 'rise' if future_last_close > pattern_last_close else 'fall'
                results[factor] = {
                    'trend': trend,
                    'data_points': len(future_sample),
                    'price_change': future_last_close - pattern_last_close,
                    'price_change_pct': ((future_last_close - pattern_last_close) / pattern_last_close) * 100,
                    'insufficient_data': True,  # Mark insufficient data
                    'requested_length': future_length,
                    'available_length': len(future_data)
                }
            else:
                results[factor] = {'trend': 'no_future_data', 'data_points': 0, 'insufficient_data': False}
            continue
        
        future_sample = future_data.iloc[:future_length]
        future_last_close = future_sample['Close'].iloc[-1]
        
        trend = 'rise' if future_last_close > pattern_last_close else 'fall'
        results[factor] = {
            'trend': trend,
            'data_points': len(future_sample),
            'price_change': future_last_close - pattern_last_close,
            'price_change_pct': ((future_last_close - pattern_last_close) / pattern_last_close) * 100,
            'insufficient_data': False  # Sufficient data
        }
    
    return results



def calculate_trend_statistics(results: list, data_dict: dict, extension_factors: list = None) -> dict:
    """Calculate trend statistics for a list of results with Stages and Volume"""
    if extension_factors is None:
        extension_factors = [0.25, 0.5, 0.75, 1.0, 1.5, 2.0, 2.5]
    
    def init_stats():
        return {
            'total': 0,
            'default': {'rise': 0, 'fall': 0, 'insufficient_data': 0, 'no_future_data': 0},
            'extension': {factor: {'rise': 0, 'fall': 0, 'insufficient_data': 0, 'no_future_data': 0} for factor in extension_factors}
        }

    overall_stats = init_stats()
    unnatural_stats = init_stats()
    stage_stats = {s: init_stats() for s in [0, 1, 2]}
    unnatural_stage_stats = {s: init_stats() for s in [0, 1, 2]}
    
    if not results:
        return {
            'overall': overall_stats, 'unnatural': unnatural_stats,
            'stages': stage_stats, 'unnatural_stages': unnatural_stage_stats,
            'total_results': 0
        }

    # Import required constants from the module scope
    import __main__
    vis_factor = getattr(__main__, 'VIS_EXTENSION_FUTURE_LENGTH_FACTOR', 2.0)

    for result in results:
        if result.get('window_data') is None: continue
        symbol = result['symbol']
        target_df = data_dict.get(symbol)
        if target_df is None: continue
        
        is_unnatural = result.get('has_unnatural_volume', False)
        stage = result.get('position_stage', -1)
        
        pattern_df = result['window_data']
        # Dynamically find analyze_future_trend if not in local scope
        analyze_fn = globals().get('analyze_future_trend')
        if not analyze_fn:
            import __main__
            analyze_fn = __main__.analyze_future_trend
            
        all_trends = analyze_fn(pattern_df, target_df, extension_factors + [vis_factor])
        
        def update_stats(stats_obj):
            stats_obj['total'] += 1
            if vis_factor in all_trends:
                t_res = all_trends[vis_factor]['trend']
                if t_res in stats_obj['default']: stats_obj['default'][t_res] += 1
            for factor in extension_factors:
                if factor in all_trends:
                    t_res = all_trends[factor]['trend']
                    if t_res in stats_obj['extension'][factor]: stats_obj['extension'][factor][t_res] += 1

        update_stats(overall_stats)
        if is_unnatural: update_stats(unnatural_stats)
        if stage in stage_stats: update_stats(stage_stats[stage])
        if is_unnatural and stage in unnatural_stage_stats: update_stats(unnatural_stage_stats[stage])
    
    return {
        'total_results': len(results),
        'overall': overall_stats,
        'unnatural': unnatural_stats,
        'stages': stage_stats,
        'unnatural_stages': unnatural_stage_stats,
        'default_factor_stats': overall_stats['default'],
        'extension_factor_stats': overall_stats['extension'],
        'unnatural_volume_total': unnatural_stats['total'],
        'unnatural_volume_default_stats': unnatural_stats['default'],
        'unnatural_volume_extension_stats': unnatural_stats['extension']
    }


def format_trend_statistics(stats: dict, factor_name: str = "Default") -> list:
    """Format trend statistics into readable text with Stages and Unnatural Volume"""
    lines = []
    total = stats.get('total_results', 0)
    if total == 0:
        lines.append(f"{factor_name}: No results available")
        return lines
    
    import __main__
    vis_factor = getattr(__main__, 'VIS_EXTENSION_FUTURE_LENGTH_FACTOR', 2.0)

    def format_group(group_stats, label):
        g_total = group_stats['total']
        if g_total == 0: return [f"\n{label}: No matches found"]
        d_rise = group_stats['default']['rise']
        d_fall = group_stats['default']['fall']
        d_rise_pct = (d_rise / g_total) * 100 if g_total > 0 else 0
        d_fall_pct = (d_fall / g_total) * 100 if g_total > 0 else 0
        group_lines = [
            f"\n{label} (Total: {g_total}):",
            f"  Default ({vis_factor}x): Rise {d_rise}/{g_total} ({d_rise_pct:.1f}%) | Fall {d_fall}/{g_total} ({d_fall_pct:.1f}%)"
        ]
        ext_line = "  Extension Factors (Rise %): "
        ext_parts = []
        for factor in sorted(group_stats['extension'].keys()):
            e_rise = group_stats['extension'][factor]['rise']
            e_pct = (e_rise / g_total) * 100 if g_total > 0 else 0
            ext_parts.append(f"{factor}x: {e_pct:.1f}%")
        group_lines.append(ext_line + " | ".join(ext_parts))
        return group_lines

    lines.extend(format_group(stats['overall'], "OVERALL STATISTICS"))
    lines.extend(format_group(stats['unnatural'], "UNNATURAL VOLUME (突兀量) ONLY"))
    lines.append("\n" + "="*20 + " STAGE ANALYSIS " + "="*20)
    for s in [0, 1, 2]:
        lines.extend(format_group(stats.get('stages', {}).get(s, {'total':0}), f"STAGE {s}"))
    lines.append("\n" + "="*20 + " STAGE +突兀量 ANALYSIS " + "="*20)
    for s in [0, 1, 2]:
        lines.extend(format_group(stats.get('unnatural_stages', {}).get(s, {'total':0}), f"STAGE {s} + UNNATURAL VOLUME"))
    return lines


def get_trend_direction(pattern_df: pd.DataFrame, target_df: pd.DataFrame, 
                      extension_factor: float = None) -> str:
    """Get the trend direction for a specific extension factor"""
    if extension_factor is None:
        extension_factor = VIS_EXTENSION_FUTURE_LENGTH_FACTOR
    
    trend_analysis = analyze_future_trend(pattern_df, target_df, [extension_factor])
    
    if extension_factor in trend_analysis:
        trend_info = trend_analysis[extension_factor]
        trend_result = trend_info['trend']
        
        if trend_result in ['rise', 'fall']:
            if trend_info.get('insufficient_data', False):
                return f"{trend_result}_insufficient"
            else:
                return trend_result
        else:
            return trend_result
    
    return 'unknown'


# ================ Visualization Functions ================

def create_full_analysis_chart(reference_df: pd.DataFrame, window_df: pd.DataFrame, target_df: pd.DataFrame, 
                             symbol: str, reference_symbol: str, timeframe: str, reference_timeframe: str, 
                             reference_label: str, similarity: float, price_distance: float, diff_distance: float, 
                             visualization_dir: str, config: TrendAnalysisConfig = None) -> str:
    """Create comprehensive visualization with three subplots, all with volume"""
    try:
        # Calculate extension periods
        pattern_length = len(window_df)
        past_length = int(pattern_length * VIS_EXTENSION_PAST_LENGTH_FACTOR)
        future_length = int(pattern_length * VIS_EXTENSION_FUTURE_LENGTH_FACTOR)
        
        # Get pattern period information
        pattern_start_date = window_df.index[0]
        pattern_end_date = window_df.index[-1]
        
        # Get past data (before pattern)      
        past_df = target_df[target_df.index < pattern_start_date]
        if len(past_df) >= past_length:
            past_data = past_df.iloc[-past_length:]
        else:
            past_data = past_df
        
        # Get future data (after pattern)
        future_df = target_df[target_df.index > pattern_end_date]
        
        # Analyze future trend to determine file name suffix
        trend_analysis = analyze_future_trend(window_df, target_df, [VIS_EXTENSION_FUTURE_LENGTH_FACTOR])
        
        if VIS_EXTENSION_FUTURE_LENGTH_FACTOR in trend_analysis:
            trend_info = trend_analysis[VIS_EXTENSION_FUTURE_LENGTH_FACTOR]
            trend_result = trend_info['trend']
            
            if trend_result in ['rise', 'fall']:
                if trend_info.get('insufficient_data', False):
                    trend_suffix = f"_{trend_result}_insufficient"
                else:
                    trend_suffix = f"_{trend_result}"
            elif trend_result == 'no_future_data':
                trend_suffix = "_no_future"
            else:
                trend_suffix = "_unknown"
        else:
            trend_suffix = "_unknown"
        
        if len(future_df) >= future_length:
            future_data = future_df.iloc[:future_length]
        else:
            future_data = future_df
        
        # Combine all data for extended view
        extended_parts = []
        if not past_data.empty:
            extended_parts.append(past_data)
        extended_parts.append(window_df)
        if not future_data.empty:
            extended_parts.append(future_data)
        
        extended_df = pd.concat(extended_parts) if extended_parts else window_df
        
        # Normalize data independently for each subplot
        reference_normalized_df, _ = DataNormalizer.normalize_ohlc_dataframe(reference_df, include_volume=True)
        window_normalized_df, _ = DataNormalizer.normalize_ohlc_dataframe(window_df, include_volume=True)
        
        # For extended view, use pattern normalization parameters for OHLC
        pattern_ohlc = window_df[['Open', 'High', 'Low', 'Close']].values
        extended_norm_params = DataNormalizer.calculate_normalization_params(pattern_ohlc, (-1, 1))
        
        # Apply pattern normalization to extended OHLC data
        extended_ohlc = extended_df[['Open', 'High', 'Low', 'Close']].values
        extended_normalized = DataNormalizer.apply_normalization_params(extended_ohlc, extended_norm_params)
        
        extended_normalized_df = extended_df.copy()
        for i, column in enumerate(['Open', 'High', 'Low', 'Close']):
            extended_normalized_df[column] = extended_normalized[:, i]
        
        if 'Volume' in extended_df.columns:
            volume_values = extended_df['Volume'].values.reshape(-1, 1)
            volume_norm_params = DataNormalizer.calculate_normalization_params(volume_values, (0, 1))
            normalized_volume = DataNormalizer.apply_normalization_params(volume_values, volume_norm_params)
            extended_normalized_df['Volume'] = normalized_volume.flatten()
        
        # Also normalize SMA columns for extended view
        sma_columns = ['SMA_30', 'SMA_45', 'SMA_60']
        for column in sma_columns:
            if column in extended_normalized_df.columns:
                extended_normalized_df[column] = DataNormalizer.apply_normalization_params(
                    extended_normalized_df[column].values.reshape(-1, 1), 
                    extended_norm_params
                ).flatten()
        
        fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(20, 40))
        
        # Plot 1: Reference trend with volume
        plot_candlesticks_with_volume(ax1, reference_normalized_df, volume_ratio=0.12)
        ax1.plot(reference_normalized_df.index, reference_normalized_df['SMA_30'], 'blue', linewidth=1.1, alpha=0.9, label='SMA30')
        ax1.plot(reference_normalized_df.index, reference_normalized_df['SMA_45'], 'orange', linewidth=1.1, alpha=0.9, label='SMA45')
        ax1.plot(reference_normalized_df.index, reference_normalized_df['SMA_60'], 'purple', linewidth=1.1, alpha=0.9, label='SMA60')
        ax1.set_title(f'Reference Trend: {reference_symbol} ({reference_timeframe}, {reference_label})', fontsize=14)
        ax1.set_ylabel('Normalized Price [-1, 1]')
        ax1.set_ylim(-1.2, 1.2)
        ax1.legend(loc='upper left', fontsize=14)
        ax1.grid(True, alpha=0.1)
        
        # Plot 2: Target pattern with volume
        plot_candlesticks_with_volume(ax2, window_normalized_df, volume_ratio=0.12)
        ax2.plot(window_normalized_df.index, window_normalized_df['SMA_30'], 'blue', linewidth=1.1, alpha=0.9, label='SMA30')
        ax2.plot(window_normalized_df.index, window_normalized_df['SMA_45'], 'orange', linewidth=1.1, alpha=0.9, label='SMA45')
        ax2.plot(window_normalized_df.index, window_normalized_df['SMA_60'], 'purple', linewidth=1.1, alpha=0.9, label='SMA60')
        ax2.set_title(f'Target Pattern: {symbol} ({timeframe})', fontsize=14)
        ax2.set_ylabel('Normalized Price [-1, 1]')
        ax2.set_ylim(-1.2, 1.2)
        ax2.legend(loc='upper left', fontsize=14)
        ax2.grid(True, alpha=0.1)
        
        # Plot 3: Extended view (past + pattern + future) with volume
        plot_candlesticks_with_volume(ax3, extended_normalized_df, volume_ratio=0.12)
        ax3.plot(extended_normalized_df.index, extended_normalized_df['SMA_30'], 'blue', linewidth=1.1, alpha=0.9, label='SMA30')
        ax3.plot(extended_normalized_df.index, extended_normalized_df['SMA_45'], 'orange', linewidth=1.1, alpha=0.9, label='SMA45')
        ax3.plot(extended_normalized_df.index, extended_normalized_df['SMA_60'], 'purple', linewidth=1.1, alpha=0.9, label='SMA60')
        
        # Add vertical lines to mark pattern boundaries in extended view
        ax3.axvline(x=pattern_start_date, color='blue', linestyle='--', linewidth=1, alpha=0.7, label='Pattern Start')
        ax3.axvline(x=pattern_end_date, color='red', linestyle='--', linewidth=1, alpha=0.7, label='Pattern End')
        
        ax3.set_title(f'Extended Analysis: {symbol} ({timeframe}) - Past + Pattern + Future', fontsize=14)
        ax3.set_xlabel('Date')
        ax3.set_ylabel('Normalized Price (pattern range: [-1, 1])')
        ax3.legend(loc='upper left', fontsize=14)
        ax3.grid(True, alpha=0.1)
        
        # Format date ticks for all axes
        for ax in [ax1, ax2, ax3]:
            ax.tick_params(axis='x', rotation=45)
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        
        # Set appropriate number of ticks based on data length
        reference_days = (reference_df.index[-1] - reference_df.index[0]).days
        window_days = (window_df.index[-1] - window_df.index[0]).days
        extended_days = (extended_df.index[-1] - extended_df.index[0]).days
        
        if reference_days > 0:
            ax1.xaxis.set_major_locator(mdates.DayLocator(interval=max(1, reference_days // 5)))
        if window_days > 0:
            ax2.xaxis.set_major_locator(mdates.DayLocator(interval=max(1, window_days // 5)))
        if extended_days > 0:
            ax3.xaxis.set_major_locator(mdates.DayLocator(interval=max(1, extended_days // 8)))
        
        # Add info text including future data status
        actual_future_length = len(future_data)
        expected_future_length = future_length
        
        future_status = ""
        if actual_future_length == 0:
            future_status = " (No future data available)"
        elif actual_future_length < expected_future_length:
            future_status = f" (Only {actual_future_length}/{expected_future_length} available)"
        
        info_text = (
            f"Similarity Score: {similarity:.4f}\n"
            f"Price Distance: {price_distance:.4f}\n"
            f"SMA Diff Distance: {diff_distance:.4f}\n"
            f"Pattern Period: {format_dt_with_tz(pattern_start_date, TIMEZONE)} to {format_dt_with_tz(pattern_end_date, TIMEZONE)}\n"
            f"Extended View:\n"
            f"  Past Factor: {VIS_EXTENSION_PAST_LENGTH_FACTOR}x ({len(past_data)} bars)\n"
            f"  Pattern: 1.0x ({len(window_df)} bars)\n"
            f"  Future Factor: {VIS_EXTENSION_FUTURE_LENGTH_FACTOR}x ({actual_future_length} bars{future_status})"
        )
        
        plt.figtext(0.02, 0.04, info_text, fontsize=14,
                    bbox=dict(facecolor='white', alpha=0.8, boxstyle='round,pad=0.5'))
        
        # Add main title
        fig.suptitle(f"Trend Analysis: {reference_symbol} vs {symbol}", fontsize=20, y=0.95)
        
        # Adjust layout
        plt.tight_layout(rect=[0, 0.08, 1, 0.95])
        plt.subplots_adjust(hspace=0.12)
        
        # Generate output filename with trend direction
        score = similarity
        timestamp = window_df.index[0].strftime("%Y%m%d")
        output_filename = f"score_{score:.4f}_{symbol}_{timestamp}{trend_suffix}.{config.image_format if config else 'png'}"
        output_path = os.path.join(visualization_dir, output_filename)
        
        # Save figure only if enabled in config
        if config and config.save_images:
            FileManager.ensure_directories(visualization_dir)
            plt.savefig(output_path, dpi=config.image_dpi, bbox_inches='tight')
            print(f"Saved full analysis visualization to {output_path}")
        else:
            print(f"Image saving disabled, visualization created but not saved: {output_filename}")
        
        plt.close(fig)
        return output_path
        
    except Exception as e:
        print(f"Error in full analysis visualization: {e}")
        # Return a default path in case of error
        score = similarity
        timestamp = window_df.index[0].strftime("%Y%m%d") if not window_df.empty else "unknown"
        image_format = config.image_format if config else 'png'
        return os.path.join(visualization_dir, f"score_{score:.4f}_{symbol}_{timestamp}_error.{image_format}")
    

def create_visualizations_parallel(args: tuple):
    """Worker function for parallel visualization"""
    target_df, result, reference_df, symbol, timeframe, reference_symbol, reference_timeframe, reference_label, visualization_dir, config = args
    
    if result is None or result["window_data"] is None:
        return None
    
    # Create visualization directory if it doesn't exist
    FileManager.ensure_directories(visualization_dir)
    
    # Create full analysis visualization
    output_path = create_full_analysis_chart(
        reference_df,
        result["window_data"],
        target_df,
        symbol,
        reference_symbol,
        timeframe,
        reference_timeframe,
        reference_label,
        result["similarity"],
        result["price_distance"],
        result["diff_distance"],
        visualization_dir,
        config
    )
    
    return {'analysis_path': output_path}


# ================ Main Function ================

def main():
    """Main function to run the trend similarity analysis"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Find similar trends in cryptocurrency data')
    parser.add_argument('-k', '--topk', type=int, default=TOP_K, help=f'Number of top matches to keep (default: {TOP_K})')
    parser.add_argument('-s', '--sleep', type=float, default=API_SLEEP_SECONDS, help=f'Sleep time between API requests (default: {API_SLEEP_SECONDS})')
    args = parser.parse_args()
    
    # Record start time
    start_time = time.time()
    
    # Create configuration from script constants
    config = TrendAnalysisConfig()
    config.sma_periods = SMA_PERIODS
    config.dtw_window_ratio = DTW_WINDOW_RATIO
    config.dtw_window_ratio_diff = DTW_WINDOW_RATIO_FOR_DIFF
    config.dtw_max_point_distance = DTW_MAX_POINT_DISTANCE
    config.dtw_max_point_distance_diff = DTW_MAX_POINT_DISTANCE_FOR_DIFF
    config.shapedtw_balance_pd_ratio = SHAPEDTW_BALANCE_PD_RATIO
    config.price_weight = PRICE_WEIGHT
    config.diff_weight = DIFF_WEIGHT
    config.slope_window_size = SLOPE_WINDOW_SIZE
    config.paa_window_size = PAA_WINDOW_SIZE
    config.window_scale_factors = WINDOW_SCALE_FACTORS
    config.min_query_length = 60  # Not used in this script but kept for consistency
    config.api_sleep_seconds = args.sleep
    config.request_time_buffer_ratio = 1.2  # Not used in this script but kept for consistency
    
    # Create run directory with timestamp
    run_directory = create_output_directory(OUTPUT_DIR)
    
    print(f"Configuration:")
    print(f"  Past Extension Factor: {VIS_EXTENSION_PAST_LENGTH_FACTOR}x")
    print(f"  Future Extension Factor: {VIS_EXTENSION_FUTURE_LENGTH_FACTOR}x")
    print(f"  Extension Factors for Stats: {EXTENSION_FACTORS_FOR_STATS}")
    print(f"  Overlap Filtering: {'Global' if GLOBAL_OVERLAP_FILTERING else 'Per-Symbol'}")
    print(f"  Top K Results: {args.topk}")
    print(f"  API Sleep: {args.sleep}s")
    
    # Initialize data processor
    data_processor = DataProcessor(config)
    
    # Process reference trends using unified manager
    reference_trends = []
    reference_data = {}
    
    for reference_symbol, trends in REFERENCE_TRENDS.items():
        for i, trend_info in enumerate(trends):
            start_datetime, end_datetime, reference_timeframe, reference_label = trend_info
            
            # Load reference trend using unified manager
            reference_df = ReferenceDataManager.load_or_fetch_reference_data(
                reference_symbol, start_datetime, end_datetime, reference_timeframe, reference_label,
                OUTPUT_DIR, TIMEZONE, data_processor, config
            )
            
            if reference_df is not None and not reference_df.empty:
                reference_key = (reference_symbol, reference_timeframe, reference_label)
                reference_data[reference_key] = reference_df
                reference_trends.append((reference_symbol, reference_timeframe, reference_label))
                print(f"Loaded reference trend for {reference_symbol} ({reference_timeframe}, {reference_label}) with {len(reference_df)} data points")
    
    if not reference_trends:
        print("No valid reference trends found. Exiting.")
        return
    
    # Process each timeframe separately
    all_results = {}
    all_reference_statistics = {}  # Store statistics for overall summary
    
    for timeframe in TIMEFRAMES_TO_ANALYZE:
        print(f"\n{'='*80}")
        print(f"Processing timeframe: {timeframe}")
        print(f"{'='*80}\n")
        
        # Create timeframe results directory
        timeframe_results_directory = os.path.join(run_directory, f"{timeframe}_results")
        FileManager.ensure_directories(timeframe_results_directory)
        
        # Download data for this timeframe using unified cache manager
        data_dict = DataCacheManager.download_timeframe_data(timeframe, OUTPUT_DIR, config, HISTORICAL_START_DATE, data_processor)
        
        # Initialize similarity finder
        similarity_finder = DTWSimilarityFinder(config)
        
        # Dictionary to store results for this timeframe
        timeframe_results = {}
        
        # Process each reference trend - regardless of its timeframe
        for reference_symbol, reference_timeframe, reference_label in reference_trends:
            reference_key = (reference_symbol, reference_timeframe, reference_label)
            reference_df = reference_data[reference_key]
            
            print(f"\nProcessing reference trend: {reference_symbol} ({reference_timeframe}, {reference_label})")
            
            # Create result directory for this reference
            reference_result_directory = os.path.join(timeframe_results_directory, f"{reference_symbol}_{reference_timeframe}_{reference_label}")
            FileManager.ensure_directories(reference_result_directory)
            
            # Prepare arguments for parallel processing
            process_arguments = []
            valid_symbols = []
            
            for symbol, target_df in data_dict.items():
                # Skip reference symbol itself
                if symbol == reference_symbol:
                    continue
                
                # Check if we have enough data
                if target_df is not None and len(target_df) >= len(reference_df):
                    process_arguments.append((reference_df, target_df, symbol, timeframe, reference_symbol, reference_timeframe, reference_label))
                    valid_symbols.append(symbol)
            
            print(f"Processing {len(valid_symbols)} valid symbols for {timeframe}...")
            
            # Process in parallel
            with Pool(processes=min(cpu_count()-1, len(valid_symbols))) if len(valid_symbols) > 1 else Pool(processes=1) as pool:
                symbol_results = pool.map(similarity_finder.process_target, process_arguments)
            
            # Collect all valid results from all symbols
            all_symbol_results = []
            for result in symbol_results:
                symbol = result["symbol"]
                if result["result"] is not None and result["result"]["similarity"] > 0:
                    # Add symbol information to the result
                    final_result = result["result"].copy()
                    final_result["symbol"] = symbol
                    final_result["has_unnatural_volume"] = has_unnatural_volume(final_result.get("window_data"))
                    final_result["position_stage"] = determine_position_stage(final_result.get("window_data"))
                    all_symbol_results.append(final_result)
            
            print(f"Found {len(all_symbol_results)} valid results before filtering...")
            
            # Apply overlap filtering based on configuration
            filtered_results = filter_non_overlapping_results(all_symbol_results, GLOBAL_OVERLAP_FILTERING)
            print(f"After {'global' if GLOBAL_OVERLAP_FILTERING else 'per-symbol'} filtering: {len(filtered_results)} results")
            
            # Sort by similarity (descending)
            filtered_results.sort(key=lambda x: x["similarity"], reverse=True)
            
            # Get top K results
            top_results = filtered_results[:args.topk]
            
            # Calculate statistics for this reference trend in this timeframe
            timeframe_statistics = calculate_trend_statistics(top_results, data_dict, EXTENSION_FACTORS_FOR_STATS)
            
            # Store statistics for overall summary
            if reference_key not in all_reference_statistics:
                all_reference_statistics[reference_key] = []
            all_reference_statistics[reference_key].extend(top_results)
            
            # Create summary for this reference
            reference_summary = []
            reference_summary.append(f"Reference: {reference_symbol} ({reference_timeframe}, {reference_label})")
            reference_summary.append(f"Reference Period: {format_dt_with_tz(reference_df.index[0], TIMEZONE)} to {format_dt_with_tz(reference_df.index[-1], TIMEZONE)}")
            reference_summary.append(f"Number of data points: {len(reference_df)}")
            reference_summary.append(f"Filtering Strategy: {'Global' if GLOBAL_OVERLAP_FILTERING else 'Per-Symbol'}")
            reference_summary.append("-" * 50)
            
            # Add trend statistics
            trend_statistics_lines = format_trend_statistics(timeframe_statistics, f"Timeframe {timeframe}")
            reference_summary.extend(trend_statistics_lines)
            reference_summary.append("-" * 50)
            
            if top_results:
                # Generate visualizations for top results
                print(f"\nGenerating visualizations for top {len(top_results)} matches...")
                visualization_directory = os.path.join(reference_result_directory, "visualizations")
                FileManager.ensure_directories(visualization_directory)
                
                # Prepare visualization arguments
                visualization_arguments = []
                for result in top_results:
                    symbol = result["symbol"]
                    visualization_arguments.append((
                        data_dict[symbol],      # Full target dataframe
                        result,                 # Result with window data
                        reference_df,           # Reference dataframe
                        symbol,                 # Symbol
                        timeframe,              # Timeframe
                        reference_symbol,       # Reference symbol
                        reference_timeframe,    # Reference timeframe
                        reference_label,        # Reference label
                        visualization_directory,# Visualization directory
                        config                  # Configuration object
                    ))
                
                # Process visualizations in parallel
                with Pool(processes=min(cpu_count()-1, len(visualization_arguments))) if len(visualization_arguments) > 1 else Pool(processes=1) as pool:
                    pool.map(create_visualizations_parallel, visualization_arguments)
                
                # Add results to summary
                reference_summary.append("Top Results:")
                for i, result in enumerate(top_results):
                    symbol = result["symbol"]
                    score = result["similarity"]
                    price_distance = result["price_distance"]
                    diff_distance = result["diff_distance"]
                    window_data = result["window_data"]
                    
                    # Get trend direction for this result
                    target_df = data_dict.get(symbol)
                    trend_direction = get_trend_direction(window_data, target_df) if target_df is not None else 'unknown'
                    
                    window_period = (
                        f"{format_dt_with_tz(window_data.index[0], TIMEZONE)} to {format_dt_with_tz(window_data.index[-1], TIMEZONE)}"
                        if window_data is not None else "N/A"
                    )
                                        
                    reference_summary.append(f"{i+1}. {symbol} ({trend_direction.upper()})")
                    reference_summary.append(f"   Score: {score:.4f}")
                    reference_summary.append(f"   Price Distance: {price_distance:.4f}")
                    reference_summary.append(f"   SMA Diff Distance: {diff_distance:.4f}")
                    reference_summary.append(f"   Period: {window_period}")
                    reference_summary.append("")
            else:
                reference_summary.append("No matching trends found")
            
            # Save reference summary
            reference_summary_text = '\n'.join(reference_summary)
            reference_summary_file = os.path.join(reference_result_directory, "results_summary.txt")
            with open(reference_summary_file, 'w') as f:
                f.write(reference_summary_text)
            
            # Store results
            timeframe_results[reference_key] = {
                "top_results": top_results,
                "all_results": filtered_results,
                "statistics": timeframe_statistics
            }
            
            # Print summary
            print(f"\n{reference_summary_text}")
        
        # Store results for this timeframe
        all_results[timeframe] = timeframe_results
    
    # Calculate overall statistics for each reference trend
    overall_reference_statistics = {}
    for reference_key, all_reference_results in all_reference_statistics.items():
        # Combine data from all timeframes for this reference
        combined_data_dictionary = {}
        for timeframe in TIMEFRAMES_TO_ANALYZE:
            timeframe_data = DataCacheManager.download_timeframe_data(timeframe, OUTPUT_DIR, config, HISTORICAL_START_DATE, data_processor)
            combined_data_dictionary.update(timeframe_data)
        
        overall_reference_statistics[reference_key] = calculate_trend_statistics(all_reference_results, combined_data_dictionary, EXTENSION_FACTORS_FOR_STATS)
    
    # Create overall summary
    overall_summary = []
    overall_summary.append(f"Trend Similarity Analysis Report")
    overall_summary.append(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    overall_summary.append(f"Past Extension Factor: {VIS_EXTENSION_PAST_LENGTH_FACTOR}x")
    overall_summary.append(f"Future Extension Factor: {VIS_EXTENSION_FUTURE_LENGTH_FACTOR}x")
    overall_summary.append(f"Extension Factors for Stats: {EXTENSION_FACTORS_FOR_STATS}")
    overall_summary.append(f"Overlap Filtering: {'Global' if GLOBAL_OVERLAP_FILTERING else 'Per-Symbol'}")
    overall_summary.append(f"{'='*50}\n")
    
    # Add overall statistics for each reference trend
    overall_summary.append("OVERALL STATISTICS (All Timeframes Combined)")
    overall_summary.append("="*50)
    for reference_key in reference_trends:
        if reference_key in overall_reference_statistics:
            reference_symbol, reference_timeframe, reference_label = reference_key
            overall_summary.append(f"\nReference: {reference_symbol} ({reference_timeframe}, {reference_label})")
            overall_summary.append("-" * 40)
            
            overall_statistics_lines = format_trend_statistics(overall_reference_statistics[reference_key], "Overall")
            overall_summary.extend(overall_statistics_lines)
    overall_summary.append("\n" + "="*50)
    
    # Include summary for each timeframe (WITHOUT detailed top matches)
    for timeframe, timeframe_results in all_results.items():
        if not timeframe_results:
            continue
            
        overall_summary.append(f"\n{'='*50}")
        overall_summary.append(f"TIMEFRAME: {timeframe}")
        overall_summary.append(f"{'='*50}\n")
        
        for reference_key, results in timeframe_results.items():
            reference_symbol, reference_timeframe, reference_label = reference_key
            top_results = results["top_results"]
            timeframe_statistics = results["statistics"]
            
            overall_summary.append(f"Reference: {reference_symbol} ({reference_timeframe}, {reference_label})")
            overall_summary.append(f"{'-'*40}")
            
            # Add timeframe-specific statistics
            timeframe_statistics_lines = format_trend_statistics(timeframe_statistics, f"Timeframe {timeframe}")
            overall_summary.extend(timeframe_statistics_lines)
            
            # Add summary count only (no detailed matches)
            if top_results:
                overall_summary.append(f"\nFound {len(top_results)} matching patterns")
                
                # Count trends by direction
                data_dict = DataCacheManager.download_timeframe_data(timeframe, OUTPUT_DIR, config, HISTORICAL_START_DATE, data_processor)
                rise_count = 0
                fall_count = 0
                insufficient_count = 0
                no_future_count = 0
                unknown_count = 0
                
                for result in top_results:
                    symbol = result["symbol"]
                    window_data = result["window_data"]
                    target_df = data_dict.get(symbol)
                    trend_direction = get_trend_direction(window_data, target_df) if target_df is not None else 'unknown'
                    
                    if trend_direction in ['rise', 'fall']:
                        if trend_direction == 'rise':
                            rise_count += 1
                        else:
                            fall_count += 1
                    elif 'insufficient' in trend_direction:
                        insufficient_count += 1
                    elif trend_direction == 'no_future_data':
                        no_future_count += 1
                    else:
                        unknown_count += 1
                
                trend_dist_line = f"Trend Distribution: {rise_count} RISE, {fall_count} FALL"
                if insufficient_count > 0:
                    trend_dist_line += f", {insufficient_count} INSUFFICIENT"
                if no_future_count > 0:
                    trend_dist_line += f", {no_future_count} NO_FUTURE"
                if unknown_count > 0:
                    trend_dist_line += f", {unknown_count} UNKNOWN"
                
                overall_summary.append(trend_dist_line)
            else:
                overall_summary.append(f"\nNo matching trends found")
            
            overall_summary.append("")
    
    # Save overall summary
    overall_summary_text = '\n'.join(overall_summary)
    overall_summary_file = os.path.join(run_directory, "overall_summary.txt")
    with open(overall_summary_file, 'w') as f:
        f.write(overall_summary_text)
    
    # Print overall summary
    print("\n" + overall_summary_text)
    print(f"\nOverall summary saved to: {overall_summary_file}")
    print(f"Results saved to: {run_directory}")
    
    # Calculate and output runtime
    end_time = time.time()
    runtime = end_time - start_time
    print(f"\nTotal runtime: {runtime:.2f} seconds ({runtime/60:.2f} minutes)")
    
    # Send Discord notifications
    try:
        discord_notifier = get_trend_finder_discord_notifier()
        if discord_notifier.enabled:
            print("\n📱 Sending Discord notifications...")
            
            # Format and send overall summary
            formatted_summary = TrendFinderMessageFormatter.format_overall_summary(
                overall_summary_text, runtime
            )
            
            # Send quick summary first
            quick_summary = TrendFinderMessageFormatter.format_quick_summary(all_results)
            discord_notifier.send_message(quick_summary)
            
            # Send detailed results
            success = discord_notifier.send_trend_finder_results(
                overall_summary=formatted_summary,
                timeframe_results=all_results,
                summary_file_path=overall_summary_file
            )
            
            if success:
                print("✅ Discord notifications sent successfully!")
            else:
                print("❌ Some Discord notifications failed to send")
        else:
            print("📱 Discord notifications disabled in config")
    except Exception as e:
        print(f"❌ Error sending Discord notifications: {e}")


if __name__ == "__main__":
    main()
