from __future__ import annotations

import argparse
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_REPORTS_DIR = PROJECT_ROOT / "historical_trend_finder_reports"
DEFAULT_DB_PATH = PROJECT_ROOT / "data_cache" / "binance_klines.db"

RESULT_LINE_RE = re.compile(r"^\d+\.\s+([A-Z0-9]+)\s+\(([^)]+)\)\s*$")
PERIOD_LINE_RE = re.compile(r"^\s*Period:\s*(.+?)\s+to\s+(.+?)\s*$")
REFERENCE_RE = re.compile(r"^Reference:\s+([A-Z0-9]+)\s+\(([^,]+),\s*([^)]+)\)\s*$")
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
        "--json",
        action="store_true",
        help="Print machine-readable JSON in addition to the table summary.",
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


def parse_summary(summary_path: Path, report_timezone: str) -> tuple[list[MatchRecord], int, str, str, str]:
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

    return matches, pattern_length, reference_symbol, reference_timeframe, reference_label


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
    return frame


def find_window_end_index(frame: pd.DataFrame, target_period_end: pd.Timestamp) -> int:
    matches = frame.index[frame["open_time"] == target_period_end]
    if len(matches) == 0:
        raise ValueError(f"找不到 open_time={target_period_end} 對應 K 線")
    return int(matches[0])


def evaluate_match(frame: pd.DataFrame, match: MatchRecord, pattern_length: int, extension_factor: float) -> TradeResult | None:
    bars_forward = int(pattern_length * extension_factor)
    if bars_forward < 1:
        raise ValueError(f"extension_factor 太小，導致 bars_forward={bars_forward}")

    end_index = find_window_end_index(frame, match.period_end)
    exit_index = end_index + bars_forward
    if exit_index >= len(frame):
        return None

    entry_row = frame.iloc[end_index]
    exit_row = frame.iloc[exit_index]
    entry_price = float(entry_row["close"])
    exit_price = float(exit_row["close"])
    r_value = (exit_price - entry_price) / entry_price

    return TradeResult(
        symbol=match.symbol,
        timeframe=match.timeframe,
        entry_time=pd.Timestamp(entry_row["close_time"]),
        exit_time=pd.Timestamp(exit_row["close_time"]),
        entry_price=entry_price,
        exit_price=exit_price,
        r_value=r_value,
        bars_forward=bars_forward,
    )


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
) -> None:
    metrics = summarize_trade_results(trades)

    print(f"Summary file : {summary_path}")
    print(f"Reference    : {reference_symbol} ({reference_timeframe}, {reference_label})")
    print(f"Pattern bars : {pattern_length}")
    print(f"Ext factor   : {extension_factor}")
    print(f"Exit bars    : {int(pattern_length * extension_factor)}")
    print(f"Matches      : {total_matches}")
    print(f"Evaluated    : {metrics['trade_count']}")
    print(f"Skipped      : {skipped_matches}")
    print("-" * 72)

    if not trades:
        print("沒有可計算的交易（可能是未來資料不足）。")
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
    matches, pattern_length, reference_symbol, reference_timeframe, reference_label = parse_summary(
        summary_path,
        report_timezone=args.report_timezone,
    )

    if not matches:
        raise ValueError(f"summary 內沒有 match 結果：{summary_path}")

    conn = sqlite3.connect(args.db_path.expanduser().resolve())
    symbol_frames: dict[str, pd.DataFrame] = {}
    trades: list[TradeResult] = []
    skipped_matches = 0

    try:
        for match in matches:
            db_symbol = match.symbol if match.symbol.endswith(args.symbol_suffix) else f"{match.symbol}{args.symbol_suffix}"
            if db_symbol not in symbol_frames:
                symbol_frames[db_symbol] = load_symbol_frame(conn, db_symbol, match.timeframe)

            trade = evaluate_match(
                frame=symbol_frames[db_symbol],
                match=match,
                pattern_length=pattern_length,
                extension_factor=args.extension_factor,
            )
            if trade is None:
                skipped_matches += 1
                continue
            trades.append(trade)
    finally:
        conn.close()

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
    )

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
            "total_matches": len(matches),
            "evaluated_matches": len(trades),
            "skipped_matches": skipped_matches,
            **metrics,
        }
        print("-" * 72)
        print(pd.Series(payload).to_json(force_ascii=False, indent=2))


if __name__ == "__main__":
    main()
