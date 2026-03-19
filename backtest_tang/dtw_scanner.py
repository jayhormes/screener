from __future__ import annotations

import pickle
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

from .signals import _dtw_calculator, _prepare_dtw_frame, build_indicator_frame


REFERENCE_DIR = Path("historical_trend_finder_reports") / "reference"
DATA_CACHE_DIR = Path("data_cache")
DEFAULT_THRESHOLD = 0.25
DEFAULT_SYMBOL = "AVAXUSDT"
DEFAULT_TIMEFRAME = "1h"
DEFAULT_LABEL = "standard"
DEFAULT_REFERENCE_START = datetime(2023, 11, 9, 12, 0, tzinfo=timezone.utc)
DEFAULT_REFERENCE_END = datetime(2023, 11, 14, 18, 0, tzinfo=timezone.utc)


@dataclass(frozen=True)
class ReferenceInfo:
    symbol: str = DEFAULT_SYMBOL
    timeframe: str = DEFAULT_TIMEFRAME
    start: datetime = DEFAULT_REFERENCE_START
    end: datetime = DEFAULT_REFERENCE_END
    label: str = DEFAULT_LABEL


@dataclass(frozen=True)
class DTWMatch:
    start: pd.Timestamp
    end: pd.Timestamp
    similarity_score: float
    price_score: float
    diff_score: float
    window_size: int

    def to_dict(self) -> dict:
        payload = asdict(self)
        payload["start"] = self.start.isoformat()
        payload["end"] = self.end.isoformat()
        return payload


@dataclass(frozen=True)
class SimilarityScores:
    similarity: float
    price_score: float
    diff_score: float


def _load_raw_rows(data_path: Path) -> list[list]:
    with Path(data_path).open("rb") as file:
        return pickle.load(file)


def load_price_frame(symbol: str = DEFAULT_SYMBOL, timeframe: str = DEFAULT_TIMEFRAME, data_path: Path | None = None) -> pd.DataFrame:
    data_path = Path(data_path or (DATA_CACHE_DIR / f"binance_{symbol}_{timeframe}.pkl"))
    raw_rows = _load_raw_rows(data_path)
    return build_indicator_frame(raw_rows).copy()


def _normalize_reference_datetime(dt: datetime | str | pd.Timestamp) -> pd.Timestamp:
    ts = pd.Timestamp(dt)
    return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")


def extract_reference(pkl_path: Path | str, start_dt: datetime | str, end_dt: datetime | str) -> pd.DataFrame:
    frame = load_price_frame(data_path=Path(pkl_path))
    start_ts = _normalize_reference_datetime(start_dt)
    end_ts = _normalize_reference_datetime(end_dt)
    mask = (frame["open_time"] >= start_ts) & (frame["open_time"] < end_ts)
    reference_frame = frame.loc[mask].copy()
    if reference_frame.empty:
        raise ValueError(f"Reference window not found: {start_ts} ~ {end_ts}")
    return reference_frame.reset_index(drop=True)


def _load_reference_from_pickle(reference_info: ReferenceInfo, reference_dir: Path = REFERENCE_DIR) -> pd.DataFrame | None:
    symbol = reference_info.symbol.replace("USDT", "")
    matches = sorted(reference_dir.glob(f"ref_{symbol}_{reference_info.timeframe}_{reference_info.label}_*.pkl"))
    expected_start = pd.Timestamp(reference_info.start)
    expected_end = pd.Timestamp(reference_info.end)

    for path in matches:
        with path.open("rb") as file:
            payload = pickle.load(file)
        frame = payload.get("df") if isinstance(payload, dict) else payload
        if not isinstance(frame, pd.DataFrame) or frame.empty:
            continue

        open_time = pd.to_datetime(frame.get("Open Time"), utc=True) if "Open Time" in frame.columns else None
        close_time = pd.to_datetime(frame.get("Close Time"), utc=True) if "Close Time" in frame.columns else None
        if open_time is None or close_time is None:
            continue
        if open_time.iloc[0] != expected_start or close_time.iloc[-1] != expected_end:
            continue

        renamed = frame.rename(
            columns={
                "Open Time": "open_time",
                "Close Time": "close_time",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume",
                "SMA_30": "sma30",
                "SMA_45": "sma45",
                "SMA_60": "sma60",
            }
        ).copy()
        return renamed.reset_index(drop=True)
    return None


def load_reference_frame(reference_info: ReferenceInfo = ReferenceInfo(), data_path: Path | None = None) -> pd.DataFrame:
    frame = _load_reference_from_pickle(reference_info)
    if frame is not None:
        return frame
    return extract_reference(data_path or (DATA_CACHE_DIR / f"binance_{reference_info.symbol}_{reference_info.timeframe}.pkl"), reference_info.start, reference_info.end)


def calculate_similarity(reference_prepared: pd.DataFrame, window_prepared: pd.DataFrame) -> SimilarityScores:
    dtw_calc = _dtw_calculator()
    ref_price, ref_diff = dtw_calc.normalize_features(reference_prepared)
    win_price, win_diff = dtw_calc.normalize_features(window_prepared)

    _, price_distance, _ = dtw_calc.calculate_dtw_similarity(
        ref_price,
        win_price,
        dtw_calc.config.dtw_window_ratio,
        dtw_calc.config.dtw_max_point_distance,
    )
    if np.isinf(price_distance):
        return SimilarityScores(0.0, 0.0, 0.0)

    _, diff_distance, _ = dtw_calc.calculate_dtw_similarity(
        ref_diff,
        win_diff,
        dtw_calc.config.dtw_window_ratio_diff,
        dtw_calc.config.dtw_max_point_distance_diff,
    )
    if np.isinf(diff_distance):
        return SimilarityScores(0.0, 0.0, 0.0)

    price_score = 1 / (1 + price_distance)
    diff_score = 1 / (1 + diff_distance)
    similarity = (price_score * 0.6) + (diff_score * 0.4)
    return SimilarityScores(float(similarity), float(price_score), float(diff_score))


def _iter_match_windows(frame: pd.DataFrame, reference_length: int) -> Iterable[tuple[int, pd.DataFrame]]:
    for end_idx in range(reference_length - 1, len(frame)):
        yield end_idx, frame.iloc[end_idx - reference_length + 1 : end_idx + 1]


def _windows_overlap(start_a: pd.Timestamp, end_a: pd.Timestamp, start_b: pd.Timestamp, end_b: pd.Timestamp) -> bool:
    return start_a <= end_b and start_b <= end_a


def _compress_matches(matches: list[DTWMatch], bar_interval: pd.Timedelta) -> list[DTWMatch]:
    if not matches:
        return []

    compressed: list[DTWMatch] = []
    current = matches[0]
    for candidate in matches[1:]:
        overlapping = _windows_overlap(candidate.start, candidate.end, current.start, current.end)
        adjacent = candidate.start <= current.end + bar_interval
        if overlapping or adjacent:
            if candidate.similarity_score >= current.similarity_score:
                current = candidate
        else:
            compressed.append(current)
            current = candidate
    compressed.append(current)
    return compressed


def scan_similar_segments(
    candles_df: pd.DataFrame,
    reference: pd.DataFrame,
    window_size: int,
    threshold: float = DEFAULT_THRESHOLD,
    exclude_reference_overlap: bool = True,
    compress: bool = True,
) -> list[DTWMatch]:
    reference_prepared = _prepare_dtw_frame(reference)
    if reference_prepared.empty:
        return []

    effective_window_size = min(window_size, len(reference))
    if effective_window_size <= 0:
        return []

    frame = candles_df.reset_index(drop=True).copy()
    bar_interval = (frame.iloc[1]["open_time"] - frame.iloc[0]["open_time"]) if len(frame) > 1 else pd.Timedelta(hours=1)
    reference_start = pd.Timestamp(reference.iloc[0]["open_time"])
    reference_end = pd.Timestamp(reference.iloc[-1]["close_time"])

    matches: list[DTWMatch] = []
    for _, window in _iter_match_windows(frame, effective_window_size):
        window_prepared = _prepare_dtw_frame(window)
        if len(window_prepared) != len(reference_prepared):
            continue

        window_start = pd.Timestamp(window.iloc[0]["open_time"])
        window_end = pd.Timestamp(window.iloc[-1]["close_time"])
        if exclude_reference_overlap and _windows_overlap(window_start, window_end, reference_start, reference_end):
            continue

        scores = calculate_similarity(reference_prepared, window_prepared)
        if scores.similarity < threshold:
            continue

        matches.append(
            DTWMatch(
                start=window_start,
                end=window_end,
                similarity_score=scores.similarity,
                price_score=scores.price_score,
                diff_score=scores.diff_score,
                window_size=effective_window_size,
            )
        )

    return _compress_matches(matches, bar_interval=bar_interval) if compress else matches


def scan_dtw_matches(
    frame: pd.DataFrame,
    reference_frame: pd.DataFrame,
    threshold: float = DEFAULT_THRESHOLD,
    compress: bool = True,
) -> list[DTWMatch]:
    return scan_similar_segments(
        candles_df=frame,
        reference=reference_frame,
        window_size=len(reference_frame),
        threshold=threshold,
        exclude_reference_overlap=True,
        compress=compress,
    )


def matches_to_dataframe(matches: list[DTWMatch]) -> pd.DataFrame:
    columns = ["start", "end", "similarity_score", "price_score", "diff_score", "window_size"]
    if not matches:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame([match.to_dict() for match in matches], columns=columns).sort_values(
        ["start", "similarity_score"], ascending=[True, False]
    ).reset_index(drop=True)
