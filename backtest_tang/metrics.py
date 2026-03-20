from __future__ import annotations

from collections import defaultdict
from typing import Iterable

import pandas as pd

from .engine import TradeRecord


def summarize_results(signal_counts: dict[int, int], trades: Iterable[TradeRecord]) -> dict:
    trades = list(trades)
    grouped: dict[int, list[TradeRecord]] = defaultdict(list)
    for trade in trades:
        grouped[trade.level].append(trade)

    by_level: dict[int, dict] = {}
    for level in (0, 1, 2):
        level_trades = grouped.get(level, [])
        trade_count = len(level_trades)
        wins = [trade for trade in level_trades if trade.rr_realized > 0]
        avg_rr = sum(trade.rr_realized for trade in level_trades) / trade_count if trade_count else 0.0
        win_rate = len(wins) / trade_count if trade_count else 0.0
        expectancy = avg_rr
        by_level[level] = {
            "signal_count": signal_counts.get(level, 0),
            "trade_count": trade_count,
            "win_rate": win_rate,
            "avg_rr": avg_rr,
            "expectancy": expectancy,
        }

    rr_series = [trade.rr_realized for trade in trades]
    max_consecutive_losses = _max_consecutive_losses(rr_series)
    total_expectancy = sum(rr_series) / len(rr_series) if rr_series else 0.0
    max_drawdown = _max_drawdown(trades)

    return {
        "by_level": by_level,
        "overall": {
            "trade_count": len(trades),
            "max_consecutive_losses": max_consecutive_losses,
            "total_expectancy": total_expectancy,
            "max_drawdown": max_drawdown,
        },
    }


def _max_consecutive_losses(rr_values: list[float]) -> int:
    max_losses = 0
    current_losses = 0
    for rr in rr_values:
        if rr <= 0:
            current_losses += 1
            max_losses = max(max_losses, current_losses)
        else:
            current_losses = 0
    return max_losses


def _max_drawdown(trades: list[TradeRecord]) -> float:
    if not trades:
        return 0.0
    df = pd.DataFrame([trade.to_dict() for trade in trades])
    equity = df["pnl"].cumsum()
    peak = equity.cummax()
    drawdown = equity - peak
    return float(drawdown.min()) if not drawdown.empty else 0.0
