from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

from backtest_tang import run_backtest
from backtest_tang.visualizer import generate_trade_charts


MULTI_TF_OUTPUT = Path("output/backtest_tang_AVAX_multi_tf.csv")


def run_multi_timeframe(symbol: str, timeframes: list[str]) -> None:
    rows = []
    for tf in timeframes:
        print(f"\n{'='*50}")
        print(f"Running: {symbol} {tf}")
        print('='*50)
        result = run_backtest(symbol=symbol, timeframe=tf)
        summary = result["summary"]
        bl = summary["by_level"]
        ov = summary["overall"]
        rows.append({
            "時框": tf,
            "位階0勝率": f"{bl[0]['win_rate']:.2%}",
            "位階1勝率": f"{bl[1]['win_rate']:.2%}",
            "位階2勝率": f"{bl[2]['win_rate']:.2%}",
            "整體期望值": f"{ov['total_expectancy']:.4f}",
            "最大回撤": f"{ov['max_drawdown']:.4f}",
            "總交易數": ov["trade_count"],
        })

    MULTI_TF_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    df.to_csv(MULTI_TF_OUTPUT, index=False, encoding="utf-8-sig")

    print(f"\n{'='*60}")
    print(f"AVAXUSDT 多時框回測比較表")
    print('='*60)
    print(df.to_string(index=False))
    print(f"\n比較表 CSV 已輸出：{MULTI_TF_OUTPUT}")


def main() -> None:
    parser = argparse.ArgumentParser(description="T桑走勢策略回測")
    parser.add_argument("--symbol", default="AVAXUSDT", help="交易對 (e.g. AVAXUSDT)")
    parser.add_argument("--timeframe", default=None, help="時框 (e.g. 30m, 1h)")
    parser.add_argument(
        "--multi-tf",
        action="store_true",
        help="批次跑多時框 (15m/30m/1h/2h/4h)",
    )
    parser.add_argument("--capital", type=float, default=1000.0, help="初始資金")
    parser.add_argument("--risk-fraction", type=float, default=0.02, help="每筆風險比例")
    parser.add_argument("--dtw", action="store_true", help="啟用 DTW v2 掃描模式")
    parser.add_argument("--dtw-threshold", type=float, default=0.25, help="DTW 相似度門檻")
    parser.add_argument("--dtw-lookahead-bars", type=int, default=5, help="match 結束點後允許進場的 K 棒數")
    parser.add_argument("--charts", action="store_true", help="產生交易圖")
    args = parser.parse_args()

    if args.multi_tf:
        run_multi_timeframe(args.symbol, ["15m", "30m", "1h", "2h", "4h"])
        return

    result = run_backtest(
        symbol=args.symbol,
        timeframe=args.timeframe or "30m",
        capital=args.capital,
        risk_fraction=args.risk_fraction,
        use_dtw_filter=args.dtw,
        dtw_threshold=args.dtw_threshold,
        dtw_match_lookahead_bars=args.dtw_lookahead_bars,
    )

    if args.charts:
        chart_dir = Path("output/charts") / f"{args.symbol}_{args.timeframe or '30m'}{'_dtw_v2' if args.dtw else ''}"
        generated = generate_trade_charts(
            symbol=args.symbol,
            timeframe=args.timeframe or "30m",
            trades_path=Path(result["output_path"]),
            output_dir=chart_dir,
        )
        print(f"Generated {len(generated)} chart(s)")


if __name__ == "__main__":
    if len(sys.argv) == 1:
        run_backtest()
    else:
        main()
