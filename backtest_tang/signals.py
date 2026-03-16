from __future__ import annotations

import pickle
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from src.common import DTWCalculator, TrendAnalysisConfig


REFERENCE_DIR = Path("historical_trend_finder_reports") / "reference"
DEFAULT_DTW_THRESHOLD = 0.25


@dataclass(frozen=True)
class RawSignal:
    level: int
    stop_price: float


@dataclass(frozen=True)
class Signal:
    level: int
    stop_price: float
    target_price: float
    risk_distance: float
    position_size: float


@dataclass(frozen=True)
class DTWSimilarityResult:
    passed: bool
    similarity: float
    threshold: float
    reference_length: int


@dataclass(frozen=True)
class ReferencePattern:
    symbol: str
    timeframe: str
    stage: int
    label: str
    frame: pd.DataFrame


@lru_cache(maxsize=1)
def _dtw_calculator() -> DTWCalculator:
    config = TrendAnalysisConfig()
    config.dtw_window_ratio = 0.12
    config.dtw_window_ratio_diff = 0.1
    config.dtw_max_point_distance = 0.6
    config.dtw_max_point_distance_diff = 0.5
    config.shapedtw_balance_pd_ratio = 4
    config.price_weight = 0.6
    config.diff_weight = 0.4
    config.slope_window_size = 5
    config.paa_window_size = 5
    return DTWCalculator(config)


def build_indicator_frame(raw_rows: list[list]) -> pd.DataFrame:
    columns = [
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
    ]
    rows = [row[:7] for row in raw_rows]
    frame = pd.DataFrame(rows, columns=columns)
    for col in ["open", "high", "low", "close", "volume"]:
        frame[col] = frame[col].astype(float)

    frame["open_time"] = pd.to_datetime(frame["open_time"], unit="ms", utc=True)
    frame["close_time"] = pd.to_datetime(frame["close_time"], unit="ms", utc=True)

    for period in (30, 45, 60):
        frame[f"sma{period}"] = frame["close"].rolling(period, min_periods=period).mean()

    frame["fan_ratio"] = (frame["sma30"] - frame["sma60"]) / frame["sma60"]
    frame["fan_expanding"] = frame["fan_ratio"] > frame["fan_ratio"].shift(1)
    frame["abrupt_volume"] = frame["volume"] > (frame["volume"].shift(1) * 5)

    frame["prev20_low"] = frame["low"].shift(1).rolling(20, min_periods=20).min()
    frame["prev50_high"] = frame["high"].shift(1).rolling(50, min_periods=50).max()
    frame["sma30_slope_3"] = frame["sma30"] - frame["sma30"].shift(3)
    frame["sma30_slope_ratio"] = frame["sma30_slope_3"] / frame["sma30"]

    return frame


def _base_trend_ok(row: pd.Series) -> bool:
    return bool(
        pd.notna(row["sma30"])
        and pd.notna(row["sma45"])
        and pd.notna(row["sma60"])
        and row["sma30"] > row["sma45"] > row["sma60"]
        and bool(row["fan_expanding"])
        and bool(row["abrupt_volume"])
    )


def _stage_label(stage: int) -> str:
    return f"stage_{stage}"


@lru_cache(maxsize=16)
def load_reference_patterns(reference_symbol: str, timeframe: str, reference_dir: str = str(REFERENCE_DIR)) -> dict[int, ReferencePattern]:
    base_path = Path(reference_dir)
    patterns: dict[int, ReferencePattern] = {}

    for stage in (0, 1, 2):
        matches = sorted(base_path.glob(f"ref_{reference_symbol}_{timeframe}_{_stage_label(stage)}_*.pkl"))
        if not matches:
            continue

        latest = max(matches, key=lambda path: path.stat().st_mtime)
        with latest.open("rb") as file:
            payload = pickle.load(file)

        frame = payload["df"] if isinstance(payload, dict) and "df" in payload else payload
        patterns[stage] = ReferencePattern(
            symbol=reference_symbol,
            timeframe=timeframe,
            stage=stage,
            label=_stage_label(stage),
            frame=frame.copy(),
        )

    return patterns


def _prepare_dtw_frame(candles: pd.DataFrame) -> pd.DataFrame:
    frame = candles.copy()
    renamed = frame.rename(
        columns={
            "close": "Close",
            "sma30": "SMA_30",
            "sma45": "SMA_45",
            "sma60": "SMA_60",
        }
    )
    required_columns = ["Close", "SMA_30", "SMA_45", "SMA_60"]
    prepared = renamed[required_columns].copy()
    prepared["SMA30_SMA45"] = prepared["SMA_30"] - prepared["SMA_45"]
    prepared["SMA30_SMA60"] = prepared["SMA_30"] - prepared["SMA_60"]
    prepared["SMA45_SMA60"] = prepared["SMA_45"] - prepared["SMA_60"]
    return prepared.dropna()


def check_dtw_similarity(
    candles: pd.DataFrame,
    stage: int,
    reference_patterns: dict[int, ReferencePattern],
    threshold: float = DEFAULT_DTW_THRESHOLD,
) -> DTWSimilarityResult:
    """
    用 ShapeDTW + DTW 對當前走勢與對應位階 reference pattern 比對。
    回傳 similarity >= threshold 才視為通過；分數越高越相似。
    """
    reference = reference_patterns.get(stage)
    if reference is None:
        return DTWSimilarityResult(False, 0.0, threshold, 0)

    reference_prepared = _prepare_dtw_frame(reference.frame)
    reference_length = len(reference_prepared)
    if reference_length == 0 or len(candles) < reference_length:
        return DTWSimilarityResult(False, 0.0, threshold, reference_length)

    window = candles.iloc[-reference_length:]
    window_prepared = _prepare_dtw_frame(window)
    if len(window_prepared) != reference_length:
        return DTWSimilarityResult(False, 0.0, threshold, reference_length)

    dtw_calc = _dtw_calculator()
    ref_price, ref_diff = dtw_calc.normalize_features(reference_prepared)
    win_price, win_diff = dtw_calc.normalize_features(window_prepared)

    # 沿用 screener 的 DTW 粗篩參數，但在固定長度回測模式下不直接 hard reject，
    # 避免 max_step 過嚴把視覺上仍相近的走勢全部濾掉。
    _, price_distance, _ = dtw_calc.calculate_dtw_similarity(
        ref_price,
        win_price,
        dtw_calc.config.dtw_window_ratio,
        dtw_calc.config.dtw_max_point_distance,
    )

    _, diff_distance, _ = dtw_calc.calculate_dtw_similarity(
        ref_diff,
        win_diff,
        dtw_calc.config.dtw_window_ratio_diff,
        dtw_calc.config.dtw_max_point_distance_diff,
    )

    price_descriptor, diff_descriptor = dtw_calc.create_shape_descriptors()
    price_shape_distance, _ = dtw_calc.calculate_shapedtw(
        ref_price,
        win_price,
        price_descriptor,
        dtw_calc.config.dtw_window_ratio,
    )
    if np.isinf(price_shape_distance):
        return DTWSimilarityResult(False, 0.0, threshold, reference_length)

    diff_shape_distance, _ = dtw_calc.calculate_shapedtw(
        ref_diff,
        win_diff,
        diff_descriptor,
        dtw_calc.config.dtw_window_ratio_diff,
    )
    if np.isinf(diff_shape_distance):
        return DTWSimilarityResult(False, 0.0, threshold, reference_length)

    price_score = 1 / (1 + price_shape_distance)
    diff_score = 1 / (1 + diff_shape_distance * dtw_calc.config.shapedtw_balance_pd_ratio)
    similarity = (price_score * dtw_calc.config.price_weight) + (diff_score * dtw_calc.config.diff_weight)
    return DTWSimilarityResult(similarity >= threshold, similarity, threshold, reference_length)


def detect_raw_signal(
    frame: pd.DataFrame,
    index: int,
    reference_patterns: dict[int, ReferencePattern] | None = None,
    dtw_threshold: float = DEFAULT_DTW_THRESHOLD,
    use_dtw_filter: bool = True,
) -> Optional[RawSignal]:
    row = frame.iloc[index]
    if not _base_trend_ok(row):
        return None

    close = float(row["close"])
    prev20_low = row["prev20_low"]
    sma30 = row["sma30"]
    sma60 = row["sma60"]
    prev50_high = row["prev50_high"]
    slope_ratio = row["sma30_slope_ratio"]

    candidate: Optional[RawSignal] = None

    if pd.notna(prev20_low) and pd.notna(sma30):
        touch_sma30 = abs(close - float(sma30)) / float(sma30) <= 0.02
        hold_previous_low = float(row["low"]) >= float(prev20_low)
        if touch_sma30 and hold_previous_low:
            candidate = RawSignal(level=0, stop_price=float(prev20_low) * 0.995)

    if candidate is None and pd.notna(sma60) and pd.notna(slope_ratio):
        if close > float(sma30) > float(row["sma45"]) > float(sma60) and float(slope_ratio) > 0.001:
            candidate = RawSignal(level=1, stop_price=float(sma60) * 0.995)

    if candidate is None and pd.notna(prev50_high):
        if close > float(prev50_high):
            candidate = RawSignal(level=2, stop_price=float(prev50_high) * 0.995)

    if candidate is None:
        return None

    if not use_dtw_filter or not reference_patterns:
        return candidate

    history = frame.iloc[: index + 1]
    dtw_result = check_dtw_similarity(history, candidate.level, reference_patterns, dtw_threshold)
    if not dtw_result.passed:
        return None

    return candidate


def qualify_signal(raw_signal: RawSignal, row: pd.Series, capital: float = 1000.0, risk_fraction: float = 0.02) -> Optional[Signal]:
    close = float(row["close"])
    stop_price = float(raw_signal.stop_price)
    risk_distance = close - stop_price
    if risk_distance <= 0:
        return None

    reward_distance = risk_distance * 2.5
    target_price = close + reward_distance
    rr = reward_distance / risk_distance if risk_distance else 0.0
    if rr < 2.5:
        return None

    position_risk = capital * risk_fraction
    position_size = position_risk / risk_distance

    return Signal(
        level=raw_signal.level,
        stop_price=stop_price,
        target_price=target_price,
        risk_distance=risk_distance,
        position_size=position_size,
    )
