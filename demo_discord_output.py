#!/usr/bin/env python3
"""
Demo script to show Discord message formatting for similarity analysis results
"""
import os
from datetime import datetime
from src.message_formatter import TrendSimilarityMessageFormatter


def demo_discord_messages():
    """Demo Discord message formatting with current similarity results"""
    
    # Read the actual similarity report
    similarity_report_path = "similarity_output/20250825_180720/similarity_search_report.txt"
    
    if not os.path.exists(similarity_report_path):
        print(f"❌ File not found: {similarity_report_path}")
        return
    
    with open(similarity_report_path, 'r', encoding='utf-8') as f:
        results_text = f.read()
    
    print("📱 DISCORD MESSAGE PREVIEW")
    print("=" * 80)
    
    # Message 1: Summary
    print("\n🟦 MESSAGE 1: ANALYSIS SUMMARY")
    print("-" * 50)
    summary_message = TrendSimilarityMessageFormatter.format_similarity_results_summary(
        results_text, 
        total_timeframes=5,  # 15m, 30m, 1h, 2h, 4h
        total_references=8   # Based on the reference trends
    )
    print(summary_message)
    
    print("\n" + "-" * 50)
    print(f"📏 Message Length: {len(summary_message)} characters")
    
    # Message 2: Top Matches
    print("\n\n🟩 MESSAGE 2: TOP MATCHES BY TIMEFRAME")
    print("-" * 50)
    top_matches_message = TrendSimilarityMessageFormatter.format_top_matches_by_timeframe(
        results_text, 
        max_per_timeframe=5
    )
    print(top_matches_message)
    
    print("\n" + "-" * 50)
    print(f"📏 Message Length: {len(top_matches_message)} characters")
    
    # Message 3: File Upload Simulation
    print("\n\n🟨 MESSAGE 3: FILE UPLOAD")
    print("-" * 50)
    tv_message = (f"📋 **TradingView Watchlist File**\n"
                 f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                 f"Contains ready-to-import symbols for TradingView")
    print(tv_message)
    print("\n[FILE ATTACHMENT: 2025-08-25_18-36_similar_trend_tradingview.txt]")
    
    print("\n" + "-" * 50)
    print(f"📏 Message Length: {len(tv_message)} characters")
    
    # Summary stats
    print("\n\n📊 MESSAGING STATISTICS")
    print("=" * 80)
    total_chars = len(summary_message) + len(top_matches_message) + len(tv_message)
    print(f"Total Messages: 3")
    print(f"Total Characters: {total_chars}")
    print(f"Average Message Length: {total_chars//3} characters")
    print(f"Discord Character Limit: 2000 per message")
    print(f"Status: {'✅ All within limits' if max(len(summary_message), len(top_matches_message), len(tv_message)) < 2000 else '⚠️ Some messages may be too long'}")
    
    # Parse and show key data
    print(f"\n🎯 KEY ANALYSIS DATA")
    print("=" * 80)
    
    lines = results_text.strip().split('\n')
    total_matches = 0
    unique_symbols = set()
    timeframe_data = {}
    
    current_timeframe = None
    for line in lines:
        if 'TIMEFRAME:' in line:
            # Extract timeframe from lines like "TIMEFRAME: 15m"
            timeframe = line.split('TIMEFRAME:')[1].strip()
            current_timeframe = timeframe
            timeframe_data[timeframe] = 0
        elif ':' in line and 'Score=' in line and current_timeframe and not line.startswith('---'):
            # This is a match line like "QTUM: Score=0.4678, Price Dist=2.0844..."
            symbol = line.split(':')[0].strip()
            if symbol and symbol != 'Top Similarity Scores':  # Filter out header lines
                unique_symbols.add(symbol)
                total_matches += 1
                timeframe_data[current_timeframe] += 1
    
    print(f"📈 Total Matches Found: {total_matches}")
    print(f"💎 Unique Symbols: {len(unique_symbols)}")
    print(f"⏰ Active Timeframes: {sum(1 for v in timeframe_data.values() if v > 0)}/5")
    
    print(f"\n📊 Matches by Timeframe:")
    for tf, count in timeframe_data.items():
        status = "🔥" if count > 0 else "💤"
        print(f"  {status} {tf}: {count} matches")
    
    print(f"\n💡 Top Symbols:")
    sorted_symbols = sorted(list(unique_symbols))
    for i, symbol in enumerate(sorted_symbols[:10], 1):
        print(f"  {i}. {symbol}")
    if len(unique_symbols) > 10:
        print(f"  ... and {len(unique_symbols) - 10} more")


if __name__ == "__main__":
    demo_discord_messages()
