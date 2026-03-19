from __future__ import annotations

import pickle
from pathlib import Path

import pandas as pd

from .dtw_scanner import ReferenceInfo, load_reference_frame, matches_to_dataframe, scan_dtw_matches
from .engine import BacktestEngine
from .metrics import summarize_results
from .signals import DEFAULT_DTW_THRESHOLD, build_indicator_frame, detect_raw_signal, qualify_signal


DEFAULT_SYMBOL = "AVAXUSDT"
DEFAULT_TIMEFRAME = "30m"
DATA_CACHE_DIR = Path("data_cache")
OUTPUT_DIR = Path("output")
DEFAULT_MATCH_LOOKAHEAD_BARS = 5


def resolve_paths(
    symbol: str,
    timeframe: str,
    data_path: Path | None = None,
    output_path: Path | None = None,
) -> tuple[Path, Path]:
    """Auto-resolve data and output paths from symbol + timeframe if not given explicitly."""
    if data_path is None:
        data_path = DATA_CACHE_DIR / f"binance_{symbol}_{timeframe}.pkl"
    if output_path is None:
        output_path = OUTPUT_DIR / f"backtest_tang_{symbol}_{timeframe}_results.csv"
    return Path(data_path), Path(output_path)


def _resolve_dtw_output_paths(symbol: str, timeframe: str, output_path: Path | None) -> tuple[Path, Path]:
    matches_output = OUTPUT_DIR / f"dtw_matches_{symbol}_{timeframe}.csv"
    if output_path is None:
        output_path = OUTPUT_DIR / f"backtest_tang_{symbol}_{timeframe}_dtw_v2_results.csv"
    return matches_output, Path(output_path)


def _build_allowed_entry_indices(frame: pd.DataFrame, matches_df: pd.DataFrame, lookahead_bars: int) -> set[int]:
    allowed_indices: set[int] = set()
    if matches_df.empty:
        return allowed_indices

    close_times = frame["close_time"]
    for match in matches_df.itertuples(index=False):
        end_ts = pd.Timestamp(match.end)
        matched = close_times.index[close_times == end_ts]
        if len(matched) == 0:
            continue
        end_idx = int(matched[0])
        for idx in range(end_idx, min(len(frame), end_idx + lookahead_bars + 1)):
            allowed_indices.add(idx)
    return allowed_indices


def run_backtest(
    data_path: Path | None = None,
    output_path: Path | None = None,
    symbol: str = DEFAULT_SYMBOL,
    timeframe: str = DEFAULT_TIMEFRAME,
    capital: float = 1000.0,
    risk_fraction: float = 0.02,
    use_dtw_filter: bool = False,
    dtw_threshold: float = DEFAULT_DTW_THRESHOLD,
    reference_symbol: str | None = None,
    dtw_match_lookahead_bars: int = DEFAULT_MATCH_LOOKAHEAD_BARS,
) -> dict:
    data_path, default_output_path = resolve_paths(symbol, timeframe, data_path, output_path)

    with Path(data_path).open("rb") as file:
        raw_rows = pickle.load(file)

    frame = build_indicator_frame(raw_rows)
    matches_df = pd.DataFrame()
    allowed_entry_indices: set[int] | None = None
    actual_output_path = Path(output_path or default_output_path)

    if use_dtw_filter:
        reference_info = ReferenceInfo(symbol=reference_symbol or symbol, timeframe=timeframe)
        reference_frame = load_reference_frame(reference_info)
        matches = scan_dtw_matches(frame, reference_frame, threshold=dtw_threshold)
        matches_df = matches_to_dataframe(matches)
        matches_output_path, actual_output_path = _resolve_dtw_output_paths(symbol, timeframe, output_path)
        matches_output_path.parent.mkdir(parents=True, exist_ok=True)
        matches_df.to_csv(matches_output_path, index=False)
        allowed_entry_indices = _build_allowed_entry_indices(frame, matches_df, dtw_match_lookahead_bars)

    engine = BacktestEngine()
    signal_counts = {0: 0, 1: 0, 2: 0}

    for index in range(len(frame)):
        row = frame.iloc[index]
        engine.update(row)

        if engine.has_open_position():
            continue

        if allowed_entry_indices is not None and index not in allowed_entry_indices:
            continue

        raw_signal = detect_raw_signal(frame, index, use_dtw_filter=False)
        if raw_signal is None:
            continue

        signal_counts[raw_signal.level] += 1
        signal = qualify_signal(raw_signal, row, capital=capital, risk_fraction=risk_fraction)
        if signal is None:
            continue

        engine.open_position(index, row, signal)

    if engine.has_open_position():
        engine.force_close(frame.iloc[-1])

    actual_output_path.parent.mkdir(parents=True, exist_ok=True)
    trades_df = pd.DataFrame([trade.to_dict() for trade in engine.trades])
    if trades_df.empty:
        trades_df = pd.DataFrame(
            columns=[
                "entry_time",
                "level",
                "entry_price",
                "stop_price",
                "target_price",
                "exit_time",
                "exit_price",
                "pnl",
                "rr_realized",
                "exit_reason",
            ]
        )
    trades_df.to_csv(actual_output_path, index=False)

    summary = summarize_results(signal_counts, engine.trades)
    print_summary(summary, symbol=symbol, timeframe=timeframe, use_dtw_filter=use_dtw_filter, dtw_threshold=dtw_threshold)
    if use_dtw_filter:
        print(f"DTW matches={len(matches_df)} | allowed_entry_bars={len(allowed_entry_indices or set())}")
        print(f"DTW matches CSV 已輸出：{matches_output_path}")
    print(f"\nCSV 已輸出：{actual_output_path}")
    return {
        "summary": summary,
        "trades": engine.trades,
        "output_path": str(actual_output_path),
        "use_dtw_filter": use_dtw_filter,
        "dtw_threshold": dtw_threshold,
        "matches": matches_df,
        "allowed_entry_indices": sorted(allowed_entry_indices) if allowed_entry_indices is not None else None,
    }


def print_summary(
    summary: dict,
    symbol: str = DEFAULT_SYMBOL,
    timeframe: str = DEFAULT_TIMEFRAME,
    use_dtw_filter: bool = False,
    dtw_threshold: float = DEFAULT_DTW_THRESHOLD,
) -> None:
    suffix = f" | DTW={'ON' if use_dtw_filter else 'OFF'}"
    if use_dtw_filter:
        suffix += f" (threshold={dtw_threshold:.2f})"
    print(f"T桑走勢策略回測摘要（{symbol} {timeframe}{suffix}）")
    for level in (0, 1, 2):
        stats = summary["by_level"][level]
        print(
            f"位階{level} | signals={stats['signal_count']} | trades={stats['trade_count']} | "
            f"win_rate={stats['win_rate']:.2%} | avg_rr={stats['avg_rr']:.4f} | expectancy={stats['expectancy']:.4f}"
        )

    overall = summary["overall"]
    print(
        "整體 | "
        f"trades={overall['trade_count']} | "
        f"max_consecutive_losses={overall['max_consecutive_losses']} | "
        f"total_expectancy={overall['total_expectancy']:.4f} | "
        f"max_drawdown={overall['max_drawdown']:.4f}"
    )
