from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Optional

import pandas as pd

from .signals import Signal


@dataclass
class Position:
    level: int
    entry_index: int
    entry_time: pd.Timestamp
    entry_price: float
    initial_stop_price: float
    stop_price: float
    target_price: float
    position_size: float
    risk_distance: float
    moved_to_breakeven: bool = False


@dataclass
class TradeRecord:
    entry_time: str
    level: int
    entry_price: float
    stop_price: float
    target_price: float
    exit_time: str
    exit_price: float
    pnl: float
    rr_realized: float
    exit_reason: str

    def to_dict(self) -> dict:
        return asdict(self)


class BacktestEngine:
    def __init__(self) -> None:
        self.position: Optional[Position] = None
        self.trades: list[TradeRecord] = []

    def has_open_position(self) -> bool:
        return self.position is not None

    def open_position(self, index: int, row: pd.Series, signal: Signal) -> None:
        self.position = Position(
            level=signal.level,
            entry_index=index,
            entry_time=row["open_time"],
            entry_price=float(row["close"]),
            initial_stop_price=signal.stop_price,
            stop_price=signal.stop_price,
            target_price=signal.target_price,
            position_size=signal.position_size,
            risk_distance=signal.risk_distance,
        )

    def update(self, row: pd.Series) -> Optional[TradeRecord]:
        if self.position is None:
            return None

        pos = self.position
        high = float(row["high"])
        low = float(row["low"])
        close = float(row["close"])
        sma60 = row.get("sma60")

        if not pos.moved_to_breakeven and high >= pos.target_price:
            pos.stop_price = pos.entry_price
            pos.moved_to_breakeven = True

        if low <= pos.stop_price:
            return self._close_position(row, pos.stop_price, "stop_loss")

        if pd.notna(sma60) and close < float(sma60):
            return self._close_position(row, close, "close_below_sma60")

        return None

    def force_close(self, row: pd.Series) -> Optional[TradeRecord]:
        if self.position is None:
            return None
        return self._close_position(row, float(row["close"]), "end_of_data")

    def _close_position(self, row: pd.Series, exit_price: float, reason: str) -> TradeRecord:
        pos = self.position
        assert pos is not None

        pnl = (exit_price - pos.entry_price) * pos.position_size
        rr_realized = (exit_price - pos.entry_price) / pos.risk_distance if pos.risk_distance else 0.0
        trade = TradeRecord(
            entry_time=pos.entry_time.isoformat(),
            level=pos.level,
            entry_price=round(pos.entry_price, 6),
            stop_price=round(pos.initial_stop_price, 6),
            target_price=round(pos.target_price, 6),
            exit_time=row["close_time"].isoformat(),
            exit_price=round(exit_price, 6),
            pnl=round(pnl, 6),
            rr_realized=round(rr_realized, 6),
            exit_reason=reason,
        )
        self.trades.append(trade)
        self.position = None
        return trade
