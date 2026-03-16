from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd


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


def detect_raw_signal(row: pd.Series) -> Optional[RawSignal]:
    if not _base_trend_ok(row):
        return None

    close = float(row["close"])
    prev20_low = row["prev20_low"]
    sma30 = row["sma30"]
    sma60 = row["sma60"]
    prev50_high = row["prev50_high"]
    slope_ratio = row["sma30_slope_ratio"]

    if pd.notna(prev20_low) and pd.notna(sma30):
        touch_sma30 = abs(close - float(sma30)) / float(sma30) <= 0.02
        hold_previous_low = float(row["low"]) >= float(prev20_low)
        if touch_sma30 and hold_previous_low:
            return RawSignal(level=0, stop_price=float(prev20_low) * 0.995)

    if pd.notna(sma60) and pd.notna(slope_ratio):
        if close > float(sma30) > float(row["sma45"]) > float(sma60) and float(slope_ratio) > 0.001:
            return RawSignal(level=1, stop_price=float(sma60) * 0.995)

    if pd.notna(prev50_high):
        if close > float(prev50_high):
            return RawSignal(level=2, stop_price=float(prev50_high) * 0.995)

    return None


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
