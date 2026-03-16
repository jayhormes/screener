from __future__ import annotations

import pickle
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
import pandas as pd

from .runner import resolve_paths
from .signals import build_indicator_frame

OUTPUT_DIR = Path("output")
CHARTS_DIR = OUTPUT_DIR / "charts"


@dataclass(frozen=True)
class TradeWindow:
    trade_id: int
    trade: pd.Series
    frame: pd.DataFrame
    entry_idx: int
    exit_idx: int


def plot_candlesticks_with_volume(ax: plt.Axes, df: pd.DataFrame, width_factor: float = 0.6, volume_ratio: float = 0.15) -> None:
    """Match the existing historical_trend_finder_reports style with candle + volume overlay."""
    if len(df) <= 1:
        return

    required_cols = ["open", "high", "low", "close"]
    if not all(col in df.columns for col in required_cols):
        return

    time_diff = (df.index[1] - df.index[0]).total_seconds() / 86400
    width = time_diff * width_factor

    up_color = "green"
    down_color = "red"

    price_min = df[["low"]].min().iloc[0]
    price_max = df[["high"]].max().iloc[0]
    price_range = price_max - price_min if price_max > price_min else max(price_max * 0.01, 1e-9)

    has_volume = "volume" in df.columns
    if has_volume:
        volume_max = float(df["volume"].max())
        if volume_max > 0:
            volume_height = price_range * volume_ratio
            volume_base = price_min - price_range * 0.1
            scaled_volume = (df["volume"] / volume_max) * volume_height
        else:
            has_volume = False

    for timestamp, row in df.iterrows():
        open_price = float(row["open"])
        high_price = float(row["high"])
        low_price = float(row["low"])
        close_price = float(row["close"])

        is_upward_candle = close_price >= open_price
        color = up_color if is_upward_candle else down_color
        ax.plot([timestamp, timestamp], [low_price, high_price], color=color, linewidth=1, alpha=0.8, zorder=2)

        half_width_timedelta = pd.Timedelta(days=width / 2)
        rect_bottom = min(open_price, close_price)
        rect_height = abs(close_price - open_price)
        if rect_height == 0:
            rect_height = price_range * 0.002

        rect = Rectangle(
            (timestamp - half_width_timedelta, rect_bottom),
            pd.Timedelta(days=width),
            rect_height,
            facecolor=color,
            edgecolor=color,
            alpha=0.8,
            zorder=3,
        )
        ax.add_patch(rect)

        if has_volume:
            volume_rect = Rectangle(
                (timestamp - half_width_timedelta, volume_base),
                pd.Timedelta(days=width),
                float(scaled_volume.loc[timestamp]),
                facecolor=color,
                edgecolor=color,
                alpha=0.35,
                zorder=1,
            )
            ax.add_patch(volume_rect)

    y_bottom = (volume_base - volume_height * 0.05) if has_volume else (price_min - price_range * 0.05)
    y_top = price_max + price_range * 0.08
    ax.set_ylim(y_bottom, y_top)


def load_price_frame(data_path: Path) -> pd.DataFrame:
    with Path(data_path).open("rb") as file:
        raw_rows = pickle.load(file)

    frame = build_indicator_frame(raw_rows).copy()
    frame = frame.set_index("open_time", drop=False)
    frame.index.name = "timestamp"
    return frame


def load_trades(trades_path: Path) -> pd.DataFrame:
    trades = pd.read_csv(trades_path)
    if trades.empty:
        return trades

    trades["entry_time"] = pd.to_datetime(trades["entry_time"], utc=True)
    trades["exit_time"] = pd.to_datetime(trades["exit_time"], utc=True)
    trades["trade_id"] = range(1, len(trades) + 1)
    trades["result"] = trades["rr_realized"].apply(lambda v: "Win" if float(v) > 0 else "Loss")
    return trades


def _find_bar_index(frame: pd.DataFrame, timestamp: pd.Timestamp, side: str) -> int:
    idx = frame.index.searchsorted(timestamp, side=side)
    if side == "right":
        idx -= 1
    return max(0, min(idx, len(frame) - 1))


def iter_trade_windows(
    frame: pd.DataFrame,
    trades: pd.DataFrame,
    bars_before: int = 50,
    bars_after: int = 20,
) -> Iterable[TradeWindow]:
    for trade in trades.itertuples(index=False):
        trade_series = pd.Series(trade._asdict())
        entry_idx = _find_bar_index(frame, trade.entry_time, side="left")
        exit_idx = _find_bar_index(frame, trade.exit_time, side="right")
        start_idx = max(0, entry_idx - bars_before)
        end_idx = min(len(frame) - 1, exit_idx + bars_after)
        yield TradeWindow(
            trade_id=int(trade.trade_id),
            trade=trade_series,
            frame=frame.iloc[start_idx : end_idx + 1].copy(),
            entry_idx=entry_idx,
            exit_idx=exit_idx,
        )


def render_trade_chart(
    trade_window: TradeWindow,
    output_dir: Path,
    symbol: str,
    timeframe: str,
) -> Path:
    trade = trade_window.trade
    window = trade_window.frame
    entry_time = pd.Timestamp(trade["entry_time"])
    exit_time = pd.Timestamp(trade["exit_time"])
    entry_price = float(trade["entry_price"])
    exit_price = float(trade["exit_price"])
    stop_price = float(trade["stop_price"])
    target_price = float(trade["target_price"])
    level = int(trade["level"])
    result = str(trade["result"])
    rr_realized = float(trade["rr_realized"])

    fig, ax = plt.subplots(1, 1, figsize=(20, 9))
    plot_candlesticks_with_volume(ax, window, volume_ratio=0.12)

    ax.plot(window.index, window["sma30"], color="blue", linewidth=2, alpha=0.8, label="SMA30", zorder=4)
    ax.plot(window.index, window["sma45"], color="orange", linewidth=2, alpha=0.8, label="SMA45", zorder=4)
    ax.plot(window.index, window["sma60"], color="purple", linewidth=2, alpha=0.8, label="SMA60", zorder=4)

    ax.scatter([entry_time], [entry_price], marker="^", s=180, color="limegreen", edgecolors="black", linewidths=0.8, label="Entry", zorder=6)
    ax.scatter([exit_time], [exit_price], marker="v", s=180, color="red", edgecolors="black", linewidths=0.8, label="Exit", zorder=6)

    ax.hlines(stop_price, xmin=entry_time, xmax=exit_time, colors="crimson", linestyles="--", linewidth=1.6, label="Stop Loss", zorder=5)
    ax.hlines(target_price, xmin=entry_time, xmax=exit_time, colors="teal", linestyles="--", linewidth=1.6, label="Target", zorder=5)
    ax.axvline(entry_time, color="limegreen", linestyle=":", linewidth=1.2, alpha=0.7, zorder=2)
    ax.axvline(exit_time, color="red", linestyle=":", linewidth=1.2, alpha=0.7, zorder=2)

    label_y = max(window["high"].max(), target_price) * 1.015
    ax.annotate(
        f"Stage {level} | {result}\nRR {rr_realized:.2f}",
        xy=(entry_time, entry_price),
        xytext=(entry_time, label_y),
        textcoords="data",
        fontsize=11,
        ha="left",
        va="bottom",
        bbox=dict(facecolor="white", alpha=0.85, boxstyle="round,pad=0.35"),
        arrowprops=dict(arrowstyle="->", color="dimgray", alpha=0.8),
        zorder=7,
    )

    info_text = (
        f"Symbol: {symbol}\n"
        f"Timeframe: {timeframe}\n"
        f"Trade ID: #{trade_window.trade_id}\n"
        f"Entry: {entry_time.isoformat()} @ {entry_price:.4f}\n"
        f"Exit: {exit_time.isoformat()} @ {exit_price:.4f}\n"
        f"Stop: {stop_price:.4f}\n"
        f"Target: {target_price:.4f}\n"
        f"Result: {result} ({trade['exit_reason']})"
    )
    ax.text(
        0.012,
        0.02,
        info_text,
        transform=ax.transAxes,
        fontsize=10,
        bbox=dict(facecolor="white", alpha=0.8, boxstyle="round,pad=0.4"),
        va="bottom",
    )

    ax.set_title(f"Backtest Trade Visualization: {symbol} ({timeframe}) - Trade #{trade_window.trade_id}", fontsize=14)
    ax.set_ylabel("Price", fontsize=12)
    ax.grid(True, alpha=0.25)
    ax.legend(loc="upper left", ncol=2)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d\n%H:%M"))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=35, ha="right")
    fig.subplots_adjust(left=0.06, right=0.99, top=0.92, bottom=0.15)

    output_dir.mkdir(parents=True, exist_ok=True)
    entry_tag = entry_time.strftime("%Y%m%d_%H%M")
    filename = f"trade_{trade_window.trade_id:03d}_stage_{level}_{result.lower()}_{entry_tag}.png"
    output_path = output_dir / filename
    fig.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    return output_path


def generate_trade_charts(
    symbol: str = "AVAXUSDT",
    timeframe: str = "1h",
    data_path: Path | None = None,
    trades_path: Path | None = None,
    output_dir: Path | None = None,
    bars_before: int = 50,
    bars_after: int = 20,
) -> list[Path]:
    data_path, default_trades_path = resolve_paths(symbol, timeframe, data_path, trades_path)
    trades_path = Path(trades_path or default_trades_path)
    output_dir = Path(output_dir or (CHARTS_DIR / f"{symbol}_{timeframe}"))

    frame = load_price_frame(data_path)
    trades = load_trades(trades_path)
    if trades.empty:
        return []

    outputs: list[Path] = []
    for trade_window in iter_trade_windows(frame, trades, bars_before=bars_before, bars_after=bars_after):
        outputs.append(render_trade_chart(trade_window, output_dir=output_dir, symbol=symbol, timeframe=timeframe))
    return outputs


if __name__ == "__main__":
    generated = generate_trade_charts()
    print(f"Generated {len(generated)} chart(s).")
    for path in generated[:5]:
        print(path)
