from __future__ import annotations

import pickle
from pathlib import Path

import pandas as pd

from .engine import BacktestEngine
from .metrics import summarize_results
from .signals import build_indicator_frame, detect_raw_signal, qualify_signal


DEFAULT_SYMBOL = "AVAXUSDT"
DEFAULT_TIMEFRAME = "30m"
DATA_CACHE_DIR = Path("data_cache")
OUTPUT_DIR = Path("output")


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


def run_backtest(
    data_path: Path | None = None,
    output_path: Path | None = None,
    symbol: str = DEFAULT_SYMBOL,
    timeframe: str = DEFAULT_TIMEFRAME,
    capital: float = 1000.0,
    risk_fraction: float = 0.02,
) -> dict:
    data_path, output_path = resolve_paths(symbol, timeframe, data_path, output_path)

    with Path(data_path).open("rb") as file:
        raw_rows = pickle.load(file)

    frame = build_indicator_frame(raw_rows)
    engine = BacktestEngine()
    signal_counts = {0: 0, 1: 0, 2: 0}

    for index, row in frame.iterrows():
        engine.update(row)

        if engine.has_open_position():
            continue

        raw_signal = detect_raw_signal(row)
        if raw_signal is None:
            continue

        signal_counts[raw_signal.level] += 1
        signal = qualify_signal(raw_signal, row, capital=capital, risk_fraction=risk_fraction)
        if signal is None:
            continue

        engine.open_position(index, row, signal)

    if engine.has_open_position():
        engine.force_close(frame.iloc[-1])

    output_path.parent.mkdir(parents=True, exist_ok=True)
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
    trades_df.to_csv(output_path, index=False)

    summary = summarize_results(signal_counts, engine.trades)
    print_summary(summary, symbol=symbol, timeframe=timeframe)
    print(f"\nCSV 已輸出：{output_path}")
    return {
        "summary": summary,
        "trades": engine.trades,
        "output_path": str(output_path),
    }


def print_summary(summary: dict, symbol: str = DEFAULT_SYMBOL, timeframe: str = DEFAULT_TIMEFRAME) -> None:
    print(f"T桑走勢策略回測摘要（{symbol} {timeframe}）")
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
