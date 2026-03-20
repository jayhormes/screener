from __future__ import annotations

import argparse
import pickle
import re
import sqlite3
from dataclasses import dataclass
import json
from datetime import timezone, timedelta
from pathlib import Path
from statistics import median
from typing import Any, Callable, Iterable

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_REPORTS_DIR = PROJECT_ROOT / "historical_trend_finder_reports"
DEFAULT_DB_PATH = PROJECT_ROOT / "data_cache" / "binance_klines.db"
EMA_FILTER_TIMEFRAMES = ("30m", "4h")
EMA_LENGTH = 200
CHART_TIMEZONE = timezone(timedelta(hours=8), name="GMT+8")
SWING_LOOKBACK = 5
GAP_ATR_MULTIPLE = 0.5
SLOPE_ATR_MULTIPLE = 0.2
TOUCH_ATR_MULTIPLE = 0.3
ATR_PERIOD = 14
BREAKOUT_ATR_MULTIPLE = 0.25
ATR_CAP_RATIO = 0.10
DEFAULT_ATR_STOP_MULTIPLE = 1.5
DEFAULT_MAX_ABRUPTNESS = 1.8


@dataclass(frozen=True)
class ReferenceProfile:
    key: str
    description: str
    matcher: Callable[[str], bool]
    expected_stage: int | None = None


REFERENCE_PROFILES: tuple[ReferenceProfile, ...] = (
    ReferenceProfile(
        key="all",
        description="全部 reference",
        matcher=lambda _folder_name: True,
    ),
    ReferenceProfile(
        key="stage_refs",
        description="位階 reference（stage_0/1/2）",
        matcher=lambda folder_name: bool(re.search(r"(?:^|_)stage_[0-2](?:_|$)", folder_name)),
    ),
    ReferenceProfile(
        key="stage_0",
        description="位階 0 reference",
        matcher=lambda folder_name: "stage_0" in folder_name,
        expected_stage=0,
    ),
    ReferenceProfile(
        key="stage_1",
        description="位階 1 reference",
        matcher=lambda folder_name: "stage_1" in folder_name,
        expected_stage=1,
    ),
    ReferenceProfile(
        key="stage_2",
        description="位階 2 reference",
        matcher=lambda folder_name: "stage_2" in folder_name,
        expected_stage=2,
    ),
    ReferenceProfile(
        key="standard",
        description="標準均線走勢 reference（如 AVAX_1h_standard）",
        matcher=lambda folder_name: "_standard" in folder_name,
    ),
    ReferenceProfile(
        key="uptrend",
        description="上升趨勢 reference（如 CRV_4h_uptrend / uptrend_2）",
        matcher=lambda folder_name: "_uptrend" in folder_name,
    ),
)

RESULT_LINE_RE = re.compile(r"^\d+\.\s+([A-Z0-9]+)\s+\(([^)]+)\)\s*$")
PERIOD_LINE_RE = re.compile(r"^\s*Period:\s*(.+?)\s+to\s+(.+?)\s*$")
REFERENCE_RE = re.compile(r"^Reference:\s+([A-Z0-9]+)\s+\(([^,]+),\s*([^)]+)\)\s*$")
REFERENCE_PERIOD_RE = re.compile(r"^Reference Period:\s*(.+?)\s+to\s+(.+?)\s*$")
PATTERN_LENGTH_RE = re.compile(r"^Number of data points:\s*(\d+)\s*$")
TIMEFRAME_DIR_RE = re.compile(r"^(\w+)_results$")


@dataclass(frozen=True)
class SummaryJob:
    summary_path: Path
    reference_selector: str


@dataclass(frozen=True)
class MatchRecord:
    symbol: str
    timeframe: str
    trend_label: str
    period_start: pd.Timestamp
    period_end: pd.Timestamp


@dataclass(frozen=True)
class TradeResult:
    symbol: str
    timeframe: str
    trend_label: str
    reference_label: str
    reference_selector: str
    period_start: pd.Timestamp
    period_end: pd.Timestamp
    entry_time: pd.Timestamp
    exit_time: pd.Timestamp
    entry_price: float
    exit_price: float
    r_value: float
    bars_forward: int
    stage: int | None
    abruptness: float | None
    stop_loss_price: float | None
    stop_loss_pct: float | None
    stop_hit: bool


@dataclass
class StageContext:
    last_bear_cross_idx: int | None = None
    first_cross_up_done: bool = False
    ma30_test_count: int = 0
    last_confirmed_swing_high_idx: int | None = None
    last_confirmed_swing_low_idx: int | None = None
    current_stage: int | None = None
    converge_zone_start: int | None = None
    last_test_touch_idx: int | None = None
    last_breakout_swing_low_idx: int | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backtest DTW report matches by buying at match end close and selling after pattern_length * extension_factor bars."
    )
    parser.add_argument(
        "--summary",
        type=Path,
        help="Path to a single results_summary.txt. If omitted, use --run-dir/--timeframe/--reference.",
    )
    parser.add_argument(
        "--run-dir",
        type=Path,
        help="Path to one historical_trend_finder_reports run directory, e.g. historical_trend_finder_reports/20260320_155420",
    )
    parser.add_argument("--timeframe", help="Target timeframe folder, e.g. 1h")
    parser.add_argument("--reference", help="Reference folder name, alias, comma list, or all; e.g. AVAX_1h_standard / standard / CRV_4h_uptrend_2 / all")
    parser.add_argument(
        "--reports-dir",
        type=Path,
        default=DEFAULT_REPORTS_DIR,
        help=f"Reports base directory. Default: {DEFAULT_REPORTS_DIR}",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=DEFAULT_DB_PATH,
        help=f"SQLite kline DB path. Default: {DEFAULT_DB_PATH}",
    )
    parser.add_argument(
        "--extension-factor",
        type=float,
        default=2.0,
        help="Exit bars = pattern_length * extension_factor. Default: 2.0",
    )
    parser.add_argument(
        "--symbol-suffix",
        default="USDT",
        help="Suffix appended to symbols when querying DB. Default: USDT",
    )
    parser.add_argument(
        "--report-timezone",
        default="America/Los_Angeles",
        help="Timezone used by results_summary.txt period strings. Default: America/Los_Angeles",
    )
    parser.add_argument(
        "--ema200-filter",
        action="store_true",
        help="Require both 30m and 4h close prices to be above EMA200 at entry time.",
    )
    parser.add_argument(
        "--atr-stop-multiple",
        type=float,
        default=DEFAULT_ATR_STOP_MULTIPLE,
        help=f"Stop loss = entry_price - ATR * multiple. Default: {DEFAULT_ATR_STOP_MULTIPLE}",
    )
    parser.add_argument(
        "--max-abruptness",
        type=float,
        default=DEFAULT_MAX_ABRUPTNESS,
        help=f"Maximum allowed entry abruptness (true range / ATR). Default: {DEFAULT_MAX_ABRUPTNESS}",
    )
    parser.add_argument(
        "--disable-stage-confirm",
        action="store_true",
        help="Disable reference stage confirmation when using stage_* references.",
    )
    parser.add_argument(
        "--list-reference-profiles",
        action="store_true",
        help="List built-in reference selectors and exit.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable JSON in addition to the table summary.",
    )
    parser.add_argument(
        "--visualize",
        action="store_true",
        help="Generate one chart per evaluated trade under backtest_vis/ next to results_summary.txt.",
    )
    return parser.parse_args()


def list_reference_profiles() -> None:
    print("Built-in reference selectors:")
    for profile in REFERENCE_PROFILES:
        expected_stage = f" | expected_stage={profile.expected_stage}" if profile.expected_stage is not None else ""
        print(f"- {profile.key:<10} : {profile.description}{expected_stage}")


def get_reference_profile(selector: str) -> ReferenceProfile | None:
    normalized = selector.strip()
    for profile in REFERENCE_PROFILES:
        if profile.key == normalized:
            return profile
    return None


def resolve_summary_jobs(args: argparse.Namespace) -> list[SummaryJob]:
    if args.summary:
        return [SummaryJob(summary_path=args.summary.expanduser().resolve(), reference_selector="direct")]

    if not args.run_dir or not args.timeframe or not args.reference:
        raise ValueError("請提供 --summary，或同時提供 --run-dir --timeframe --reference。")

    run_dir = args.run_dir.expanduser().resolve()
    timeframe_dir = run_dir / f"{args.timeframe}_results"
    if not timeframe_dir.exists():
        raise FileNotFoundError(f"找不到 timeframe 目錄：{timeframe_dir}")

    selectors = [item.strip() for item in str(args.reference).split(",") if item.strip()]
    if not selectors:
        raise ValueError("--reference 不可為空")

    jobs: list[SummaryJob] = []
    seen_paths: set[Path] = set()
    for selector in selectors:
        profile = get_reference_profile(selector)
        if profile is None:
            summary_path = timeframe_dir / selector / "results_summary.txt"
            if not summary_path.exists():
                raise FileNotFoundError(f"找不到 reference summary：{summary_path}")
            resolved = summary_path.resolve()
            if resolved not in seen_paths:
                jobs.append(SummaryJob(summary_path=resolved, reference_selector=selector))
                seen_paths.add(resolved)
            continue

        for summary_path in sorted(timeframe_dir.glob("*/results_summary.txt")):
            folder_name = summary_path.parent.name
            if not profile.matcher(folder_name):
                continue
            resolved = summary_path.resolve()
            if resolved in seen_paths:
                continue
            jobs.append(SummaryJob(summary_path=resolved, reference_selector=selector))
            seen_paths.add(resolved)

    if not jobs:
        raise ValueError(f"找不到符合條件的 reference summary：timeframe={args.timeframe}, selector={args.reference}")
    return jobs


def _parse_report_timestamp(value: str, report_timezone: str) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    return ts.tz_localize(report_timezone).tz_convert("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")


def parse_summary(
    summary_path: Path,
    report_timezone: str,
) -> tuple[list[MatchRecord], int, str, str, str, pd.Timestamp, pd.Timestamp]:
    if not summary_path.exists():
        raise FileNotFoundError(f"找不到 summary 檔案：{summary_path}")

    timeframe_dir = summary_path.parent.parent.name
    timeframe_match = TIMEFRAME_DIR_RE.match(timeframe_dir)
    if not timeframe_match:
        raise ValueError(f"無法從資料夾名稱判斷 timeframe：{timeframe_dir}")
    timeframe = timeframe_match.group(1)

    lines = summary_path.read_text(encoding="utf-8").splitlines()
    pattern_length: int | None = None
    reference_symbol: str | None = None
    reference_timeframe: str | None = None
    reference_label: str | None = None
    reference_period_start: pd.Timestamp | None = None
    reference_period_end: pd.Timestamp | None = None
    matches: list[MatchRecord] = []

    current_symbol: str | None = None
    current_trend_label: str | None = None

    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue

        ref_match = REFERENCE_RE.match(line)
        if ref_match:
            reference_symbol, reference_timeframe, reference_label = ref_match.groups()
            continue

        reference_period_match = REFERENCE_PERIOD_RE.match(line)
        if reference_period_match:
            start_text, end_text = reference_period_match.groups()
            reference_period_start = _parse_report_timestamp(start_text, report_timezone)
            reference_period_end = _parse_report_timestamp(end_text, report_timezone)
            continue

        pattern_match = PATTERN_LENGTH_RE.match(line)
        if pattern_match:
            pattern_length = int(pattern_match.group(1))
            continue

        result_match = RESULT_LINE_RE.match(line)
        if result_match:
            current_symbol, current_trend_label = result_match.groups()
            continue

        period_match = PERIOD_LINE_RE.match(line)
        if period_match and current_symbol is not None and current_trend_label is not None:
            start_text, end_text = period_match.groups()
            matches.append(
                MatchRecord(
                    symbol=current_symbol,
                    timeframe=timeframe,
                    trend_label=current_trend_label,
                    period_start=_parse_report_timestamp(start_text, report_timezone),
                    period_end=_parse_report_timestamp(end_text, report_timezone),
                )
            )
            current_symbol = None
            current_trend_label = None

    if pattern_length is None:
        raise ValueError(f"summary 內找不到 Number of data points：{summary_path}")
    if reference_symbol is None or reference_timeframe is None or reference_label is None:
        raise ValueError(f"summary 內找不到 Reference 資訊：{summary_path}")
    if reference_period_start is None or reference_period_end is None:
        raise ValueError(f"summary 內找不到 Reference Period：{summary_path}")

    return (
        matches,
        pattern_length,
        reference_symbol,
        reference_timeframe,
        reference_label,
        reference_period_start,
        reference_period_end,
    )


def load_symbol_frame(conn: sqlite3.Connection, symbol: str, timeframe: str) -> pd.DataFrame:
    query = """
        SELECT open_time, open, high, low, close, volume, close_time
        FROM klines
        WHERE symbol = ? AND timeframe = ?
        ORDER BY open_time ASC
    """
    frame = pd.read_sql_query(query, conn, params=(symbol, timeframe))
    if frame.empty:
        raise ValueError(f"DB 查無資料：symbol={symbol}, timeframe={timeframe}")

    frame["open_time"] = pd.to_datetime(frame["open_time"], unit="ms", utc=True)
    frame["close_time"] = pd.to_datetime(frame["close_time"], unit="ms", utc=True)
    numeric_columns = ["open", "high", "low", "close", "volume"]
    frame[numeric_columns] = frame[numeric_columns].astype(float)
    return frame


def load_cached_reference_frame(
    reports_dir: Path,
    reference_symbol: str,
    reference_timeframe: str,
    reference_label: str,
    reference_period_start: pd.Timestamp,
    reference_period_end: pd.Timestamp,
) -> pd.DataFrame:
    start_ts = int(reference_period_start.timestamp())
    end_ts = int(reference_period_end.timestamp())
    cache_path = reports_dir / "reference" / (
        f"ref_{reference_symbol}_{reference_timeframe}_{reference_label}_{start_ts}_{end_ts}.pkl"
    )
    if not cache_path.exists():
        raise FileNotFoundError(f"找不到 reference cache：{cache_path}")

    with cache_path.open("rb") as file:
        cache_payload = pickle.load(file)

    frame = cache_payload.get("df")
    if frame is None or frame.empty:
        raise ValueError(f"reference cache 無有效資料：{cache_path}")

    normalized = frame.copy().reset_index().rename(
        columns={
            "datetime": "open_time",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    )
    required_columns = {"open_time", "open", "high", "low", "close"}
    if not required_columns.issubset(normalized.columns):
        raise ValueError(f"reference cache 欄位不完整：{cache_path}")

    normalized["open_time"] = pd.to_datetime(normalized["open_time"], utc=True)
    interval = normalized["open_time"].diff().dropna().median()
    if pd.isna(interval):
        interval = pd.Timedelta(0)
    normalized["close_time"] = normalized["open_time"] + interval

    for column in ["open", "high", "low", "close", "volume"]:
        if column in normalized.columns:
            normalized[column] = normalized[column].astype(float)
        elif column == "volume":
            normalized[column] = 0.0
        else:
            raise ValueError(f"reference cache 缺少 {column} 欄位：{cache_path}")

    if normalized[["open", "high", "low", "close"]].isna().any().any():
        raise ValueError(f"reference cache OHLC 含有缺值：{cache_path}")

    return normalized[["open_time", "open", "high", "low", "close", "volume", "close_time"]]


def build_ema_frame(frame: pd.DataFrame, ema_length: int = EMA_LENGTH) -> pd.DataFrame:
    ema_frame = frame[["open_time", "close_time", "close"]].copy()
    ema_frame["ema200"] = ema_frame["close"].ewm(span=ema_length, adjust=False, min_periods=ema_length).mean()
    return ema_frame


def add_stage_features(frame: pd.DataFrame) -> pd.DataFrame:
    stage_frame = frame.copy()
    stage_frame["sma30"] = stage_frame["close"].rolling(window=30, min_periods=30).mean()
    stage_frame["sma45"] = stage_frame["close"].rolling(window=45, min_periods=45).mean()
    stage_frame["sma60"] = stage_frame["close"].rolling(window=60, min_periods=60).mean()

    previous_close = stage_frame["close"].shift(1)
    previous_tr = pd.concat(
        [
            stage_frame["high"] - stage_frame["low"],
            (stage_frame["high"] - previous_close).abs(),
            (stage_frame["low"] - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    stage_frame["atr"] = previous_tr.rolling(window=ATR_PERIOD, min_periods=ATR_PERIOD).mean()
    return stage_frame


def infer_expected_stage(reference_label: str, reference_selector: str) -> int | None:
    profile = get_reference_profile(reference_selector)
    if profile is not None and profile.expected_stage is not None:
        return profile.expected_stage
    matched = re.search(r"(?:^|_)stage_([0-2])(?:_|$)", reference_label)
    return int(matched.group(1)) if matched else None


def compute_abruptness(row: pd.Series) -> float | None:
    atr = row.get("atr")
    if pd.isna(atr) or atr is None or float(atr) <= 0:
        return None
    true_range = float(row["high"]) - float(row["low"])
    return true_range / float(atr)


def compute_atr_stop_loss(entry_row: pd.Series, atr_stop_multiple: float) -> tuple[float | None, float | None]:
    atr = entry_row.get("atr")
    if pd.isna(atr) or atr is None or float(atr) <= 0:
        return None, None
    entry_price = float(entry_row["close"])
    stop_loss_price = entry_price - (float(atr) * atr_stop_multiple)
    stop_loss_pct = (entry_price - stop_loss_price) / entry_price if entry_price else None
    return stop_loss_price, stop_loss_pct


def find_exit_with_atr_stop(frame: pd.DataFrame, entry_index: int, exit_index: int, stop_loss_price: float | None) -> tuple[int, float, bool]:
    if stop_loss_price is None:
        exit_row = frame.iloc[exit_index]
        return exit_index, float(exit_row["close"]), False

    for current_index in range(entry_index + 1, exit_index + 1):
        row = frame.iloc[current_index]
        if float(row["low"]) <= stop_loss_price:
            return current_index, stop_loss_price, True

    exit_row = frame.iloc[exit_index]
    return exit_index, float(exit_row["close"]), False


def find_window_end_index(frame: pd.DataFrame, target_period_end: pd.Timestamp) -> int:
    matches = frame.index[frame["open_time"] == target_period_end]
    if len(matches) == 0:
        raise ValueError(f"找不到 open_time={target_period_end} 對應 K 線")
    return int(matches[0])


def find_close_time_index(frame: pd.DataFrame, target_close_time: pd.Timestamp) -> int:
    matches = frame.index[frame["close_time"] == target_close_time]
    if len(matches) == 0:
        raise ValueError(f"找不到 close_time={target_close_time} 對應 K 線")
    return int(matches[0])


def resolve_entry_row(frame: pd.DataFrame, entry_time: pd.Timestamp) -> pd.Series | None:
    eligible = frame.loc[frame["close_time"] <= entry_time]
    if eligible.empty:
        return None
    return eligible.iloc[-1]


def passes_ema200_filter(ema_frames: dict[str, pd.DataFrame], entry_time: pd.Timestamp) -> bool:
    for timeframe in EMA_FILTER_TIMEFRAMES:
        entry_row = resolve_entry_row(ema_frames[timeframe], entry_time)
        if entry_row is None or pd.isna(entry_row["ema200"]):
            return False
        if float(entry_row["close"]) <= float(entry_row["ema200"]):
            return False
    return True


def is_confirmed_swing_high(frame: pd.DataFrame, pivot_idx: int, lookback: int) -> bool:
    if pivot_idx - lookback < 0 or pivot_idx + lookback >= len(frame):
        return False
    pivot_high = float(frame.iloc[pivot_idx]["high"])
    left_highs = frame.iloc[pivot_idx - lookback : pivot_idx]["high"]
    right_highs = frame.iloc[pivot_idx + 1 : pivot_idx + lookback + 1]["high"]
    return bool((pivot_high > left_highs).all() and (pivot_high >= right_highs).all())


def is_confirmed_swing_low(frame: pd.DataFrame, pivot_idx: int, lookback: int) -> bool:
    if pivot_idx - lookback < 0 or pivot_idx + lookback >= len(frame):
        return False
    pivot_low = float(frame.iloc[pivot_idx]["low"])
    left_lows = frame.iloc[pivot_idx - lookback : pivot_idx]["low"]
    right_lows = frame.iloc[pivot_idx + 1 : pivot_idx + lookback + 1]["low"]
    return bool((pivot_low < left_lows).all() and (pivot_low <= right_lows).all())


def build_stage_labels(frame: pd.DataFrame) -> pd.DataFrame:
    if "sma30" not in frame.columns or "sma60" not in frame.columns or "atr" not in frame.columns:
        raise ValueError("frame 缺少 stage feature 欄位")

    stage_series: list[int | None] = [None] * len(frame)
    anchor_series: list[float | None] = [None] * len(frame)
    stop_pct_series: list[float | None] = [None] * len(frame)
    context = StageContext()

    for index in range(1, len(frame)):
        row = frame.iloc[index]
        prev_row = frame.iloc[index - 1]
        sma30 = row["sma30"]
        sma60 = row["sma60"]
        prev_sma30 = prev_row["sma30"]
        prev_sma60 = prev_row["sma60"]

        confirm_idx = index - SWING_LOOKBACK
        if confirm_idx >= SWING_LOOKBACK:
            if is_confirmed_swing_low(frame, confirm_idx, SWING_LOOKBACK):
                context.last_confirmed_swing_low_idx = confirm_idx
            if is_confirmed_swing_high(frame, confirm_idx, SWING_LOOKBACK):
                context.last_confirmed_swing_high_idx = confirm_idx
                context.last_breakout_swing_low_idx = context.last_confirmed_swing_low_idx

        close = row["close"]
        atr = row["atr"]
        if pd.isna(atr) or float(atr) <= 0 or pd.isna(close) or float(close) <= 0:
            stage_series[index] = None
            anchor_series[index] = None
            stop_pct_series[index] = None
            continue

        atr_ratio = min(float(atr) / float(close), ATR_CAP_RATIO)
        gap_threshold = atr_ratio * GAP_ATR_MULTIPLE
        slope_threshold = atr_ratio * SLOPE_ATR_MULTIPLE
        touch_threshold = atr_ratio * TOUCH_ATR_MULTIPLE
        breakout_threshold = atr_ratio * BREAKOUT_ATR_MULTIPLE

        if pd.notna(prev_sma30) and pd.notna(prev_sma60) and prev_sma30 >= prev_sma60 and sma30 < sma60:
            context.last_bear_cross_idx = index
            context.first_cross_up_done = False
            context.ma30_test_count = 0
            context.current_stage = None
            context.converge_zone_start = None
            context.last_test_touch_idx = None

        close_cross_up = pd.notna(prev_sma30) and prev_row["close"] <= prev_sma30 and row["close"] > sma30
        if context.last_bear_cross_idx is not None and not context.first_cross_up_done and pd.notna(sma30) and close_cross_up:
            context.current_stage = 0
            context.first_cross_up_done = True

        ma_gap = abs(sma30 - sma60) if pd.notna(sma30) and pd.notna(sma60) else None
        sma30_slope = abs(sma30 - prev_sma30) if pd.notna(sma30) and pd.notna(prev_sma30) else None
        sma60_slope = abs(sma60 - prev_sma60) if pd.notna(sma60) and pd.notna(prev_sma60) else None
        in_convergence = (
            ma_gap is not None
            and sma30_slope is not None
            and sma60_slope is not None
            and ma_gap <= gap_threshold
            and sma30_slope <= slope_threshold
            and sma60_slope <= slope_threshold
        )

        if in_convergence:
            if context.converge_zone_start is None:
                context.converge_zone_start = index
                context.ma30_test_count = 0
                context.last_test_touch_idx = None

            ma30_touch = abs(row["low"] - sma30) <= touch_threshold if pd.notna(sma30) else False
            if ma30_touch and context.last_test_touch_idx != index - 1:
                context.ma30_test_count += 1
                context.last_test_touch_idx = index

            if context.ma30_test_count >= 2 and close_cross_up:
                context.current_stage = 1
        else:
            context.converge_zone_start = None
            context.ma30_test_count = 0
            context.last_test_touch_idx = None

        breakout_swing_high = context.last_confirmed_swing_high_idx
        if (
            breakout_swing_high is not None
            and context.last_breakout_swing_low_idx is not None
            and breakout_swing_high > context.last_breakout_swing_low_idx
            and pd.notna(sma30)
            and pd.notna(sma60)
            and sma30 > sma60
            and ma_gap is not None
            and ma_gap > gap_threshold
            and prev_sma30 is not None
            and (sma30 - prev_sma30) >= 0
        ):
            swing_high_price = float(frame.iloc[breakout_swing_high]["high"])
            if row["close"] > swing_high_price + breakout_threshold:
                context.current_stage = 2

        stage = context.current_stage
        anchor_price: float | None = None
        if stage == 0 and context.last_confirmed_swing_low_idx is not None:
            anchor_price = float(frame.iloc[context.last_confirmed_swing_low_idx]["low"])
        elif stage == 1 and pd.notna(sma30):
            anchor_price = float(sma30)
        elif stage == 2 and context.last_breakout_swing_low_idx is not None:
            anchor_price = float(frame.iloc[context.last_breakout_swing_low_idx]["low"])

        stop_pct = None
        if anchor_price is not None:
            entry_price = float(row["close"])
            stop_pct = (entry_price - anchor_price) / entry_price if entry_price else None

        stage_series[index] = stage
        anchor_series[index] = anchor_price
        stop_pct_series[index] = stop_pct if stop_pct is None else max(stop_pct, 0.0)

    labeled = frame.copy()
    labeled["stage"] = stage_series
    labeled["stage_stop_anchor"] = anchor_series
    labeled["stage_stop_pct"] = stop_pct_series
    return labeled


def evaluate_match(
    frame: pd.DataFrame,
    match: MatchRecord,
    pattern_length: int,
    extension_factor: float,
    reference_label: str,
    reference_selector: str,
    atr_stop_multiple: float,
    max_abruptness: float,
    stage_confirm_enabled: bool,
    ema_frames: dict[str, pd.DataFrame] | None = None,
) -> TradeResult | None:
    bars_forward = int(pattern_length * extension_factor)
    if bars_forward < 1:
        raise ValueError(f"extension_factor 太小，導致 bars_forward={bars_forward}")

    end_index = find_window_end_index(frame, match.period_end)
    planned_exit_index = end_index + bars_forward
    if planned_exit_index >= len(frame):
        return None

    entry_row = frame.iloc[end_index]
    if ema_frames is not None and not passes_ema200_filter(ema_frames, pd.Timestamp(entry_row["close_time"])):
        return None

    stage = entry_row.get("stage")
    normalized_stage = None if pd.isna(stage) else int(stage)
    expected_stage = infer_expected_stage(reference_label, reference_selector)
    if stage_confirm_enabled and expected_stage is not None and normalized_stage != expected_stage:
        return None

    abruptness = compute_abruptness(entry_row)
    if abruptness is not None and abruptness > max_abruptness:
        return None

    entry_price = float(entry_row["close"])
    stop_loss_price, stop_loss_pct = compute_atr_stop_loss(entry_row, atr_stop_multiple)
    actual_exit_index, exit_price, stop_hit = find_exit_with_atr_stop(frame, end_index, planned_exit_index, stop_loss_price)
    exit_row = frame.iloc[actual_exit_index]
    r_value = (exit_price - entry_price) / entry_price

    return TradeResult(
        symbol=match.symbol,
        timeframe=match.timeframe,
        trend_label=match.trend_label,
        reference_label=reference_label,
        reference_selector=reference_selector,
        period_start=match.period_start,
        period_end=match.period_end,
        entry_time=pd.Timestamp(entry_row["close_time"]),
        exit_time=pd.Timestamp(exit_row["close_time"]),
        entry_price=entry_price,
        exit_price=exit_price,
        r_value=r_value,
        bars_forward=bars_forward,
        stage=normalized_stage,
        abruptness=abruptness,
        stop_loss_price=stop_loss_price,
        stop_loss_pct=stop_loss_pct,
        stop_hit=stop_hit,
    )


def load_matplotlib() -> Any:
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError("缺少 matplotlib，請先安裝後再使用 --visualize。") from exc
    return plt


def sanitize_filename(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("_") or "plot"


def build_reference_window(frame: pd.DataFrame, period_start: pd.Timestamp, period_end: pd.Timestamp) -> pd.DataFrame:
    window = frame.loc[(frame["open_time"] >= period_start) & (frame["open_time"] <= period_end)].copy()
    if window.empty:
        raise ValueError(f"Reference 區間無資料：{period_start} ~ {period_end}")
    return window.reset_index(drop=True)


def build_trade_window(frame: pd.DataFrame, trade: TradeResult) -> tuple[pd.DataFrame, int, int]:
    start_index = find_window_end_index(frame, trade.period_start)
    entry_index = find_window_end_index(frame, trade.period_end)
    exit_index = find_close_time_index(frame, trade.exit_time)
    window = frame.iloc[start_index : exit_index + 1].copy().reset_index(drop=True)
    return window, entry_index - start_index, exit_index - start_index


def add_sma_columns(frame: pd.DataFrame) -> pd.DataFrame:
    sma_frame = frame.copy()
    for length in (30, 45, 60):
        sma_frame[f"sma{length}"] = sma_frame["close"].rolling(window=length, min_periods=length).mean()
    return sma_frame


def build_chart_datetimes(frame: pd.DataFrame) -> pd.Series:
    return frame["open_time"].dt.tz_convert(CHART_TIMEZONE)


def build_xticks(datetimes: pd.Series, count: int = 6) -> list[pd.Timestamp]:
    if datetimes.empty:
        return []
    if len(datetimes) <= count:
        return list(datetimes)

    last_index = len(datetimes) - 1
    positions = sorted({round(step * last_index / (count - 1)) for step in range(count)})
    return [datetimes.iloc[position] for position in positions]


def configure_datetime_axis(axis: Any, datetimes: pd.Series, xlabel: str | None = None) -> None:
    ticks = build_xticks(datetimes)
    axis.set_xticks(ticks)
    axis.set_xticklabels([timestamp.strftime("%Y-%m-%d %H:%M") for timestamp in ticks], rotation=30, ha="right")
    axis.tick_params(axis="x", labelbottom=True)
    if xlabel:
        axis.set_xlabel(xlabel)


def compute_candlestick_width(datetimes: pd.Series) -> float:
    if len(datetimes) <= 1:
        return 1 / 1440

    diffs = datetimes.diff().dropna()
    if diffs.empty:
        return 1 / 1440

    width = diffs.median().total_seconds() / 86400 * 0.7
    return max(width, 1 / 1440)


def draw_candlesticks(axis: Any, datetimes: pd.Series, frame: pd.DataFrame) -> float:
    import matplotlib.dates as mdates
    from matplotlib.patches import Rectangle

    bullish_color = "#22c55e"
    bearish_color = "#ef4444"
    candle_width = compute_candlestick_width(datetimes)
    half_width = candle_width / 2
    x_values = mdates.date2num(datetimes.dt.to_pydatetime())

    for x_value, (_, row) in zip(x_values, frame.iterrows()):
        open_price = float(row["open"])
        high_price = float(row["high"])
        low_price = float(row["low"])
        close_price = float(row["close"])
        color = bullish_color if close_price >= open_price else bearish_color

        axis.vlines(x_value, low_price, high_price, color=color, linewidth=1.0, zorder=2)

        body_bottom = min(open_price, close_price)
        body_height = abs(close_price - open_price)
        if body_height == 0:
            axis.hlines(open_price, x_value - half_width, x_value + half_width, color=color, linewidth=1.2, zorder=3)
            continue

        axis.add_patch(
            Rectangle(
                (x_value - half_width, body_bottom),
                candle_width,
                body_height,
                facecolor=color,
                edgecolor=color,
                linewidth=1.0,
                zorder=3,
            )
        )

    axis.set_xlim(x_values[0] - candle_width, x_values[-1] + candle_width)
    axis.xaxis_date()
    return candle_width


def visualize_trades(
    summary_path: Path,
    reference_symbol: str,
    reference_timeframe: str,
    reference_period_start: pd.Timestamp,
    reference_period_end: pd.Timestamp,
    reference_frame: pd.DataFrame,
    symbol_frames: dict[tuple[str, str], pd.DataFrame],
    trades: list[TradeResult],
    symbol_suffix: str,
) -> int:
    plt = load_matplotlib()
    output_dir = summary_path.parent / "backtest_vis"
    output_dir.mkdir(parents=True, exist_ok=True)

    reference_window = add_sma_columns(build_reference_window(reference_frame, reference_period_start, reference_period_end))
    reference_entry_index = len(reference_window) - 1
    reference_entry_price = float(reference_window.iloc[reference_entry_index]["close"])
    generated = 0

    reference_datetimes = build_chart_datetimes(reference_window)
    for index, trade in enumerate(trades, start=1):
        db_symbol = trade.symbol if trade.symbol.endswith(symbol_suffix) else f"{trade.symbol}{symbol_suffix}"
        frame = symbol_frames[(db_symbol, trade.timeframe)]
        trade_window, entry_offset, exit_offset = build_trade_window(frame, trade)
        trade_window = add_sma_columns(trade_window)
        trade_datetimes = build_chart_datetimes(trade_window)
        trade_candle_width = compute_candlestick_width(trade_datetimes)

        fig, axes = plt.subplots(
            3,
            1,
            figsize=(14, 10),
            sharex=False,
            gridspec_kw={"height_ratios": [1.0, 1.0, 0.6]},
        )

        draw_candlesticks(axes[0], reference_datetimes, reference_window)
        axes[0].plot(reference_datetimes, reference_window["sma30"], color="#38BDF8", linewidth=1.2, label="SMA30")
        axes[0].plot(reference_datetimes, reference_window["sma45"], color="#818CF8", linewidth=1.2, label="SMA45")
        axes[0].plot(reference_datetimes, reference_window["sma60"], color="#C084FC", linewidth=1.2, label="SMA60")
        axes[0].scatter(
            [reference_datetimes.iloc[reference_entry_index]],
            [reference_entry_price],
            color="red",
            s=60,
            label="Reference Entry",
            zorder=3,
        )
        axes[0].set_title(f"Reference: {reference_symbol} ({reference_timeframe})")
        axes[0].set_ylabel("Price")
        axes[0].grid(True, alpha=0.3)
        axes[0].legend(loc="best")

        stop_loss_price = trade.stop_loss_price if trade.stop_loss_price is not None else trade.entry_price

        draw_candlesticks(axes[1], trade_datetimes, trade_window)
        axes[1].plot(trade_datetimes, trade_window["sma30"], color="#38BDF8", linewidth=1.2, label="SMA30")
        axes[1].plot(trade_datetimes, trade_window["sma45"], color="#818CF8", linewidth=1.2, label="SMA45")
        axes[1].plot(trade_datetimes, trade_window["sma60"], color="#C084FC", linewidth=1.2, label="SMA60")
        axes[1].scatter(
            [trade_datetimes.iloc[entry_offset]],
            [trade.entry_price],
            color="orange",
            marker="^",
            s=90,
            label=f"Entry (Stage {trade.stage})" if trade.stage is not None else "Entry",
            zorder=4,
        )
        axes[1].scatter(
            [trade_datetimes.iloc[exit_offset]],
            [trade.exit_price],
            color="red",
            marker="v",
            s=90,
            label="Exit",
            zorder=4,
        )
        axes[1].axhline(stop_loss_price, color="red", linestyle="--", linewidth=1.5, label="ATR Stop")
        axes[1].annotate(
            "ATR Stop",
            xy=(trade_datetimes.iloc[exit_offset], stop_loss_price),
            xytext=(-8, 6),
            textcoords="offset points",
            color="red",
            fontsize=10,
            ha="right",
            va="bottom",
        )
        axes[1].set_title(f"Trade: {trade.symbol} ({trade.timeframe}, {trade.trend_label}, ref={trade.reference_label})")
        axes[1].set_ylabel("Price")
        axes[1].grid(True, alpha=0.3)
        axes[1].legend(loc="best")

        volume_colors = ["#22c55e" if close >= open_ else "#ef4444" for open_, close in zip(trade_window["open"], trade_window["close"])]
        axes[2].bar(trade_datetimes, trade_window["volume"], color=volume_colors, width=trade_candle_width)
        axes[2].set_title("Volume")
        axes[2].set_xlabel("Datetime (GMT+8)")
        axes[2].set_ylabel("Volume")
        axes[2].grid(True, axis="y", alpha=0.3)
        axes[2].set_xlim(trade_datetimes.iloc[0], trade_datetimes.iloc[-1] + pd.Timedelta(days=trade_candle_width))

        configure_datetime_axis(axes[0], reference_datetimes, xlabel="Datetime (GMT+8)")
        configure_datetime_axis(axes[1], trade_datetimes, xlabel="Datetime (GMT+8)")
        configure_datetime_axis(axes[2], trade_datetimes, xlabel="Datetime (GMT+8)")

        outcome = "WIN" if trade.r_value > 0 else "LOSS" if trade.r_value < 0 else "FLAT"
        stage_text = f"Stage {trade.stage}" if trade.stage is not None else "Stage N/A"
        stop_text = "STOP" if trade.stop_hit else "TIME"
        abrupt_text = f" | abrupt={trade.abruptness:.2f}" if trade.abruptness is not None else ""
        fig.suptitle(f"{trade.symbol} | {stage_text} | R={trade.r_value:.4f} | {outcome} | {stop_text}{abrupt_text}", fontsize=14, fontweight="bold")
        fig.tight_layout(rect=(0, 0, 1, 0.96))

        filename = (
            f"{index:03d}_{sanitize_filename(trade.symbol)}_{sanitize_filename(trade.timeframe)}_"
            f"{trade.entry_time.strftime('%Y%m%dT%H%M%SZ')}_{outcome}.png"
        )
        fig.savefig(output_dir / filename, dpi=150)
        plt.close(fig)
        generated += 1

    return generated


def summarize_trade_results(trades: Iterable[TradeResult]) -> dict:
    trade_list = list(trades)
    r_values = [trade.r_value for trade in trade_list]
    trade_count = len(trade_list)
    win_count = sum(1 for value in r_values if value > 0)

    stage_groups: dict[str, list[TradeResult]] = {"None": []}
    for trade in trade_list:
        key = "None" if trade.stage is None else str(trade.stage)
        stage_groups.setdefault(key, []).append(trade)

    stage_stats: dict[str, dict[str, float | int | None]] = {}
    for key in ["0", "1", "2", "None"]:
        group = stage_groups.get(key, [])
        group_r_values = [item.r_value for item in group]
        group_count = len(group)
        group_win_count = sum(1 for value in group_r_values if value > 0)
        stage_stats[key] = {
            "trade_count": group_count,
            "win_rate": (group_win_count / group_count) if group_count else 0.0,
            "average_r": (sum(group_r_values) / group_count) if group_count else 0.0,
            "median_r": median(group_r_values) if group_r_values else None,
        }

    return {
        "trade_count": trade_count,
        "win_count": win_count,
        "win_rate": (win_count / trade_count) if trade_count else 0.0,
        "average_r": (sum(r_values) / trade_count) if trade_count else 0.0,
        "median_r": median(r_values) if r_values else None,
        "r_values": r_values,
        "stage_stats": stage_stats,
    }


def print_report(
    summary_path: Path,
    reference_symbol: str,
    reference_timeframe: str,
    reference_label: str,
    pattern_length: int,
    extension_factor: float,
    total_matches: int,
    skipped_matches: int,
    trades: list[TradeResult],
    ema200_filter_enabled: bool,
) -> None:
    metrics = summarize_trade_results(trades)

    print(f"Summary file : {summary_path}")
    print(f"Reference    : {reference_symbol} ({reference_timeframe}, {reference_label})")
    print(f"Pattern bars : {pattern_length}")
    print(f"Ext factor   : {extension_factor}")
    print(f"Exit bars    : {int(pattern_length * extension_factor)}")
    print(f"EMA200 filter: {'ON (30m + 4h)' if ema200_filter_enabled else 'OFF'}")
    print(f"Matches      : {total_matches}")
    print(f"Evaluated    : {metrics['trade_count']}")
    print(f"Skipped      : {skipped_matches}")
    print("-" * 72)

    if not trades:
        print("沒有可計算的交易（可能是未來資料不足，或 EMA200 過濾後全數跳過）。")
        return

    table = pd.DataFrame(
        [
            {
                "symbol": trade.symbol,
                "timeframe": trade.timeframe,
                "entry_time": trade.entry_time.isoformat(),
                "exit_time": trade.exit_time.isoformat(),
                "entry_price": round(trade.entry_price, 8),
                "exit_price": round(trade.exit_price, 8),
                "reference": trade.reference_label,
                "selector": trade.reference_selector,
                "stage": trade.stage,
                "abruptness": None if trade.abruptness is None else round(trade.abruptness, 4),
                "stop_loss_price": None if trade.stop_loss_price is None else round(trade.stop_loss_price, 8),
                "stop_loss_pct": None if trade.stop_loss_pct is None else round(trade.stop_loss_pct, 6),
                "stop_hit": trade.stop_hit,
                "R": round(trade.r_value, 6),
            }
            for trade in trades
        ]
    )
    print(table.to_string(index=False))
    print("-" * 72)
    print("R values    :", [round(value, 6) for value in metrics["r_values"]])
    print(f"Average R   : {metrics['average_r']:.6f}")
    print(f"Median R    : {metrics['median_r']:.6f}" if metrics["median_r"] is not None else "Median R    : N/A")
    print(f"Win rate    : {metrics['win_rate']:.2%}")
    print(f"Trade count : {metrics['trade_count']}")
    print("Stage stats :")
    for key in ["0", "1", "2", "None"]:
        stage_metric = metrics["stage_stats"][key]
        median_text = f"{stage_metric['median_r']:.6f}" if stage_metric["median_r"] is not None else "N/A"
        print(
            f"  Stage {key:>4} | count={stage_metric['trade_count']:>3} | avgR={stage_metric['average_r']:.6f} | "
            f"medianR={median_text} | winRate={stage_metric['win_rate']:.2%}"
        )


def main() -> None:
    args = parse_args()
    if args.list_reference_profiles:
        list_reference_profiles()
        return

    summary_jobs = resolve_summary_jobs(args)

    conn = sqlite3.connect(args.db_path.expanduser().resolve())
    symbol_frames: dict[tuple[str, str], pd.DataFrame] = {}
    ema_frames_by_symbol: dict[str, dict[str, pd.DataFrame]] = {}
    trades: list[TradeResult] = []
    skipped_matches = 0
    total_matches = 0
    generated_visualizations = 0
    visualization_requests: list[tuple[Path, str, str, str, pd.Timestamp, pd.Timestamp, str]] = []
    latest_reference_symbol = ""
    latest_reference_timeframe = ""
    latest_reference_label = ""
    latest_pattern_length = 0
    reference_frame_lookup: dict[tuple[str, str], pd.DataFrame] = {}

    try:
        for job in summary_jobs:
            (
                matches,
                pattern_length,
                reference_symbol,
                reference_timeframe,
                reference_label,
                reference_period_start,
                reference_period_end,
            ) = parse_summary(
                job.summary_path,
                report_timezone=args.report_timezone,
            )
            if not matches:
                raise ValueError(f"summary 內沒有 match 結果：{job.summary_path}")

            total_matches += len(matches)
            latest_reference_symbol = reference_symbol
            latest_reference_timeframe = reference_timeframe
            latest_reference_label = reference_label
            latest_pattern_length = pattern_length
            reference_db_symbol = reference_symbol if reference_symbol.endswith(args.symbol_suffix) else f"{reference_symbol}{args.symbol_suffix}"

            if args.visualize:
                reference_frame_key = (reference_db_symbol, reference_timeframe)
                if reference_frame_key not in symbol_frames:
                    try:
                        symbol_frames[reference_frame_key] = load_cached_reference_frame(
                            reports_dir=args.reports_dir.expanduser().resolve(),
                            reference_symbol=reference_symbol,
                            reference_timeframe=reference_timeframe,
                            reference_label=reference_label,
                            reference_period_start=reference_period_start,
                            reference_period_end=reference_period_end,
                        )
                    except (FileNotFoundError, ValueError):
                        symbol_frames[reference_frame_key] = load_symbol_frame(conn, reference_db_symbol, reference_timeframe)
                reference_frame_lookup[reference_frame_key] = symbol_frames[reference_frame_key]
                visualization_requests.append((job.summary_path, reference_symbol, reference_timeframe, reference_label, reference_period_start, reference_period_end, reference_db_symbol))

            for match in matches:
                db_symbol = match.symbol if match.symbol.endswith(args.symbol_suffix) else f"{match.symbol}{args.symbol_suffix}"
                frame_key = (db_symbol, match.timeframe)
                if frame_key not in symbol_frames:
                    symbol_frames[frame_key] = build_stage_labels(add_stage_features(load_symbol_frame(conn, db_symbol, match.timeframe)))

                ema_frames: dict[str, pd.DataFrame] | None = None
                if args.ema200_filter:
                    if db_symbol not in ema_frames_by_symbol:
                        ema_frames_by_symbol[db_symbol] = {
                            timeframe: build_ema_frame(load_symbol_frame(conn, db_symbol, timeframe))
                            for timeframe in EMA_FILTER_TIMEFRAMES
                        }
                    ema_frames = ema_frames_by_symbol[db_symbol]

                trade = evaluate_match(
                    frame=symbol_frames[frame_key],
                    match=match,
                    pattern_length=pattern_length,
                    extension_factor=args.extension_factor,
                    reference_label=reference_label,
                    reference_selector=job.reference_selector,
                    atr_stop_multiple=args.atr_stop_multiple,
                    max_abruptness=args.max_abruptness,
                    stage_confirm_enabled=not args.disable_stage_confirm,
                    ema_frames=ema_frames,
                )
                if trade is None:
                    skipped_matches += 1
                    continue
                trades.append(trade)
    finally:
        conn.close()

    if args.visualize and trades:
        for summary_path, reference_symbol, reference_timeframe, reference_label, reference_period_start, reference_period_end, reference_db_symbol in visualization_requests:
            job_trades = [trade for trade in trades if trade.reference_label == reference_label]
            if not job_trades:
                continue
            generated_visualizations += visualize_trades(
                summary_path=summary_path,
                reference_symbol=reference_symbol,
                reference_timeframe=reference_timeframe,
                reference_period_start=reference_period_start,
                reference_period_end=reference_period_end,
                reference_frame=reference_frame_lookup[(reference_db_symbol, reference_timeframe)],
                symbol_frames=symbol_frames,
                trades=job_trades,
                symbol_suffix=args.symbol_suffix,
            )

    print_report(
        summary_path=summary_jobs[0].summary_path if len(summary_jobs) == 1 else summary_jobs[0].summary_path.parent.parent,
        reference_symbol=latest_reference_symbol if len(summary_jobs) == 1 else "MULTI",
        reference_timeframe=latest_reference_timeframe if len(summary_jobs) == 1 else args.timeframe,
        reference_label=latest_reference_label if len(summary_jobs) == 1 else ",".join(job.reference_selector for job in summary_jobs),
        pattern_length=latest_pattern_length,
        extension_factor=args.extension_factor,
        total_matches=total_matches,
        skipped_matches=skipped_matches,
        trades=trades,
        ema200_filter_enabled=args.ema200_filter,
    )

    if args.visualize:
        print(f"Visualization: {generated_visualizations} chart(s)")

    if args.json:
        metrics = summarize_trade_results(trades)
        payload = {
            "summary_jobs": [
                {"summary_path": str(job.summary_path), "reference_selector": job.reference_selector}
                for job in summary_jobs
            ],
            "reference": {
                "symbol": latest_reference_symbol if len(summary_jobs) == 1 else "MULTI",
                "timeframe": latest_reference_timeframe if len(summary_jobs) == 1 else args.timeframe,
                "label": latest_reference_label if len(summary_jobs) == 1 else [job.reference_selector for job in summary_jobs],
            },
            "pattern_length": latest_pattern_length,
            "extension_factor": args.extension_factor,
            "exit_bars": int(latest_pattern_length * args.extension_factor),
            "ema200_filter": args.ema200_filter,
            "atr_stop_multiple": args.atr_stop_multiple,
            "max_abruptness": args.max_abruptness,
            "stage_confirm_enabled": not args.disable_stage_confirm,
            "visualize": args.visualize,
            "generated_visualizations": generated_visualizations,
            "total_matches": total_matches,
            "evaluated_matches": len(trades),
            "skipped_matches": skipped_matches,
            **metrics,
        }
        print("-" * 72)
        print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
