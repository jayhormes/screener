from __future__ import annotations

import pickle
from pathlib import Path

import pandas as pd

from .engine import BacktestEngine
from .metrics import summarize_results
from .signals import build_indicator_frame, detect_raw_signal, qualify_signal


DEFAULT_DATA_PATH = Path("data_cache/binance_AVAXUSDT_30m.pkl")
DEFAULT_OUTPUT_PATH = Path("output/backtest_tang_AVAXUSDT_30m_results.csv")


def run_backtest(
    data_path: Path = DEFAULT_DATA_PATH,
    output_path: Path = DEFAULT_OUTPUT_PATH,
    capital: float = 1000.0,
    risk_fraction: float = 0.02,
) -> dict:
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

    output_path = Path(output_path)
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
    print_summary(summary)
    print(f"\nCSV 已輸出：{output_path}")
    return {
        "summary": summary,
        "trades": engine.trades,
        "output_path": str(output_path),
    }


def print_summary(summary: dict) -> None:
    print("T桑走勢策略回測摘要（AVAXUSDT 30m）")
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
