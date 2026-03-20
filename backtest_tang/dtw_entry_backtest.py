from __future__ import annotations

import argparse
import pickle
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_REPORTS_DIR = PROJECT_ROOT / "historical_trend_finder_reports"
DEFAULT_DB_PATH = PROJECT_ROOT / "data_cache" / "binance_klines.db"
EMA_FILTER_TIMEFRAMES = ("30m", "4h")
EMA_LENGTH = 200

RESULT_LINE_RE = re.compile(r"^\d+\.\s+([A-Z0-9]+)\s+\(([^)]+)\)\s*$")
PERIOD_LINE_RE = re.compile(r"^\s*Period:\s*(.+?)\s+to\s+(.+?)\s*$")
REFERENCE_RE = re.compile(r"^Reference:\s+([A-Z0-9]+)\s+\(([^,]+),\s*([^)]+)\)\s*$")
REFERENCE_PERIOD_RE = re.compile(r"^Reference Period:\s*(.+?)\s+to\s+(.+?)\s*$")
PATTERN_LENGTH_RE = re.compile(r"^Number of data points:\s*(\d+)\s*$")
TIMEFRAME_DIR_RE = re.compile(r"^(\w+)_results$")


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
    period_start: pd.Timestamp
    period_end: pd.Timestamp
    entry_time: pd.Timestamp
    exit_time: pd.Timestamp
    entry_price: float
    exit_price: float
    r_value: float
    bars_forward: int


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
    parser.add_argument("--reference", help="Reference folder name, e.g. AVAX_1h_standard")
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


def resolve_summary_path(args: argparse.Namespace) -> Path:
    if args.summary:
        return args.summary.expanduser().resolve()

    if not args.run_dir or not args.timeframe or not args.reference:
        raise ValueError("請提供 --summary，或同時提供 --run-dir --timeframe --reference。")

    run_dir = args.run_dir.expanduser().resolve()
    return run_dir / f"{args.timeframe}_results" / args.reference / "results_summary.txt"


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
    if "open_time" not in normalized.columns or "close" not in normalized.columns:
        raise ValueError(f"reference cache 欄位不完整：{cache_path}")

    normalized["open_time"] = pd.to_datetime(normalized["open_time"], utc=True)
    interval = normalized["open_time"].diff().dropna().median()
    if pd.isna(interval):
        interval = pd.Timedelta(0)
    normalized["close_time"] = normalized["open_time"] + interval

    for column in ["open", "high", "low", "close", "volume"]:
        if column in normalized.columns:
            normalized[column] = normalized[column].astype(float)
        else:
            normalized[column] = float("nan")

    return normalized[["open_time", "open", "high", "low", "close", "volume", "close_time"]]


def build_ema_frame(frame: pd.DataFrame, ema_length: int = EMA_LENGTH) -> pd.DataFrame:
    ema_frame = frame[["open_time", "close_time", "close"]].copy()
    ema_frame["ema200"] = ema_frame["close"].ewm(span=ema_length, adjust=False, min_periods=ema_length).mean()
    return ema_frame


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


def evaluate_match(
    frame: pd.DataFrame,
    match: MatchRecord,
    pattern_length: int,
    extension_factor: float,
    ema_frames: dict[str, pd.DataFrame] | None = None,
) -> TradeResult | None:
    bars_forward = int(pattern_length * extension_factor)
    if bars_forward < 1:
        raise ValueError(f"extension_factor 太小，導致 bars_forward={bars_forward}")

    end_index = find_window_end_index(frame, match.period_end)
    exit_index = end_index + bars_forward
    if exit_index >= len(frame):
        return None

    entry_row = frame.iloc[end_index]
    if ema_frames is not None and not passes_ema200_filter(ema_frames, pd.Timestamp(entry_row["close_time"])):
        return None

    exit_row = frame.iloc[exit_index]
    entry_price = float(entry_row["close"])
    exit_price = float(exit_row["close"])
    r_value = (exit_price - entry_price) / entry_price

    return TradeResult(
        symbol=match.symbol,
        timeframe=match.timeframe,
        trend_label=match.trend_label,
        period_start=match.period_start,
        period_end=match.period_end,
        entry_time=pd.Timestamp(entry_row["close_time"]),
        exit_time=pd.Timestamp(exit_row["close_time"]),
        entry_price=entry_price,
        exit_price=exit_price,
        r_value=r_value,
        bars_forward=bars_forward,
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

    for index, trade in enumerate(trades, start=1):
        db_symbol = trade.symbol if trade.symbol.endswith(symbol_suffix) else f"{trade.symbol}{symbol_suffix}"
        frame = symbol_frames[(db_symbol, trade.timeframe)]
        trade_window, entry_offset, exit_offset = build_trade_window(frame, trade)
        trade_window = add_sma_columns(trade_window)

        fig, axes = plt.subplots(
            3,
            1,
            figsize=(14, 10),
            sharex=False,
            gridspec_kw={"height_ratios": [1.0, 1.0, 0.6]},
        )

        axes[0].plot(reference_window.index, reference_window["close"], color="tab:blue", linewidth=1.5)
        axes[0].plot(reference_window.index, reference_window["sma30"], color="yellow", linewidth=1.2, label="SMA30")
        axes[0].plot(reference_window.index, reference_window["sma45"], color="orange", linewidth=1.2, label="SMA45")
        axes[0].plot(reference_window.index, reference_window["sma60"], color="purple", linewidth=1.2, label="SMA60")
        axes[0].scatter(
            [reference_entry_index],
            [reference_entry_price],
            color="red",
            s=60,
            label="Reference Entry",
            zorder=3,
        )
        axes[0].set_title(f"Reference: {reference_symbol} ({reference_timeframe})")
        axes[0].set_ylabel("Close")
        axes[0].grid(True, alpha=0.3)
        axes[0].legend(loc="best")

        axes[1].plot(trade_window.index, trade_window["close"], color="tab:green", linewidth=1.5)
        axes[1].plot(trade_window.index, trade_window["sma30"], color="yellow", linewidth=1.2, label="SMA30")
        axes[1].plot(trade_window.index, trade_window["sma45"], color="orange", linewidth=1.2, label="SMA45")
        axes[1].plot(trade_window.index, trade_window["sma60"], color="purple", linewidth=1.2, label="SMA60")
        axes[1].scatter([entry_offset], [trade.entry_price], color="orange", s=60, label="Entry", zorder=3)
        axes[1].scatter([exit_offset], [trade.exit_price], color="red", s=60, label="Exit", zorder=3)
        axes[1].set_title(f"Trade: {trade.symbol} ({trade.timeframe}, {trade.trend_label})")
        axes[1].set_ylabel("Close")
        axes[1].grid(True, alpha=0.3)
        axes[1].legend(loc="best")

        volume_colors = ["green" if close >= open_ else "red" for open_, close in zip(trade_window["open"], trade_window["close"])]
        axes[2].bar(trade_window.index, trade_window["volume"], color=volume_colors, width=0.8)
        axes[2].set_title("Volume")
        axes[2].set_xlabel("Bars")
        axes[2].set_ylabel("Volume")
        axes[2].grid(True, axis="y", alpha=0.3)

        outcome = "WIN" if trade.r_value > 0 else "LOSS" if trade.r_value < 0 else "FLAT"
        fig.suptitle(f"{trade.symbol} | R={trade.r_value:.4f} | {outcome}", fontsize=14, fontweight="bold")
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

    return {
        "trade_count": trade_count,
        "win_count": win_count,
        "win_rate": (win_count / trade_count) if trade_count else 0.0,
        "average_r": (sum(r_values) / trade_count) if trade_count else 0.0,
        "r_values": r_values,
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
                "R": round(trade.r_value, 6),
            }
            for trade in trades
        ]
    )
    print(table.to_string(index=False))
    print("-" * 72)
    print("R values    :", [round(value, 6) for value in metrics["r_values"]])
    print(f"Average R   : {metrics['average_r']:.6f}")
    print(f"Win rate    : {metrics['win_rate']:.2%}")
    print(f"Trade count : {metrics['trade_count']}")


def main() -> None:
    args = parse_args()
    summary_path = resolve_summary_path(args)
    (
        matches,
        pattern_length,
        reference_symbol,
        reference_timeframe,
        reference_label,
        reference_period_start,
        reference_period_end,
    ) = parse_summary(
        summary_path,
        report_timezone=args.report_timezone,
    )

    if not matches:
        raise ValueError(f"summary 內沒有 match 結果：{summary_path}")

    conn = sqlite3.connect(args.db_path.expanduser().resolve())
    symbol_frames: dict[tuple[str, str], pd.DataFrame] = {}
    ema_frames_by_symbol: dict[str, dict[str, pd.DataFrame]] = {}
    trades: list[TradeResult] = []
    skipped_matches = 0
    reference_db_symbol = reference_symbol if reference_symbol.endswith(args.symbol_suffix) else f"{reference_symbol}{args.symbol_suffix}"

    try:
        if args.visualize:
            reference_frame_key = (reference_db_symbol, reference_timeframe)
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

        for match in matches:
            db_symbol = match.symbol if match.symbol.endswith(args.symbol_suffix) else f"{match.symbol}{args.symbol_suffix}"
            frame_key = (db_symbol, match.timeframe)
            if frame_key not in symbol_frames:
                symbol_frames[frame_key] = load_symbol_frame(conn, db_symbol, match.timeframe)

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
                ema_frames=ema_frames,
            )
            if trade is None:
                skipped_matches += 1
                continue
            trades.append(trade)
    finally:
        conn.close()

    generated_visualizations = 0
    if args.visualize and trades:
        generated_visualizations = visualize_trades(
            summary_path=summary_path,
            reference_symbol=reference_symbol,
            reference_timeframe=reference_timeframe,
            reference_period_start=reference_period_start,
            reference_period_end=reference_period_end,
            reference_frame=symbol_frames[(reference_db_symbol, reference_timeframe)],
            symbol_frames=symbol_frames,
            trades=trades,
            symbol_suffix=args.symbol_suffix,
        )

    print_report(
        summary_path=summary_path,
        reference_symbol=reference_symbol,
        reference_timeframe=reference_timeframe,
        reference_label=reference_label,
        pattern_length=pattern_length,
        extension_factor=args.extension_factor,
        total_matches=len(matches),
        skipped_matches=skipped_matches,
        trades=trades,
        ema200_filter_enabled=args.ema200_filter,
    )

    if args.visualize:
        print(f"Visualization: {generated_visualizations} chart(s) -> {summary_path.parent / 'backtest_vis'}")

    if args.json:
        metrics = summarize_trade_results(trades)
        payload = {
            "summary_path": str(summary_path),
            "reference": {
                "symbol": reference_symbol,
                "timeframe": reference_timeframe,
                "label": reference_label,
            },
            "pattern_length": pattern_length,
            "extension_factor": args.extension_factor,
            "exit_bars": int(pattern_length * args.extension_factor),
            "ema200_filter": args.ema200_filter,
            "visualize": args.visualize,
            "generated_visualizations": generated_visualizations,
            "total_matches": len(matches),
            "evaluated_matches": len(trades),
            "skipped_matches": skipped_matches,
            **metrics,
        }
        print("-" * 72)
        print(pd.Series(payload).to_json(force_ascii=False, indent=2))


if __name__ == "__main__":
    main()
