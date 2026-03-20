"""
Test script for Discord trend similarity notifications
"""
import os
from datetime import datetime
from src.discord_notifier import DiscordNotifier
from src.message_formatter import TrendSimilarityMessageFormatter


def test_discord_similarity_notification():
    """Test Discord notification for similarity analysis results"""
    
    # Read the similarity search report
    similarity_report_path = "similarity_output/20250825_180720/similarity_search_report.txt"
    tradingview_file_path = "similarity_output/20250825_180720/2025-08-25_18-36_similar_trend_tradingview.txt"
    
    if not os.path.exists(similarity_report_path):
        print(f"Similarity report not found: {similarity_report_path}")
        return
    
    if not os.path.exists(tradingview_file_path):
        print(f"TradingView file not found: {tradingview_file_path}")
        return
    
    # Read the results
    with open(similarity_report_path, 'r', encoding='utf-8') as f:
        results_text = f.read()
    
    try:
        # Initialize Discord notifier
        notifier = DiscordNotifier(use_trend_finder=True)
        
        if not notifier.enabled:
            print("Discord notifications are disabled in config")
            return
        
        print("Testing Discord similarity notifications...")
        
        # Test 1: Send summary message
        print("1. Sending summary message...")
        summary_message = TrendSimilarityMessageFormatter.format_similarity_results_summary(
            results_text, 
            total_timeframes=5,  # 15m, 30m, 1h, 2h, 4h
            total_references=8   # Based on the reference trends
        )
        
        message_sent = notifier.send_message(summary_message)
        if message_sent:
            print("✅ Summary message sent successfully!")
        else:
            print("❌ Failed to send summary message")
        
        # Test 2: Send top matches
        print("2. Sending top matches...")
        top_matches_message = TrendSimilarityMessageFormatter.format_top_matches_by_timeframe(
            results_text, 
            max_per_timeframe=5
        )
        
        matches_sent = notifier.send_message(top_matches_message)
        if matches_sent:
            print("✅ Top matches message sent successfully!")
        else:
            print("❌ Failed to send top matches message")
        
        # Test 3: Send TradingView file
        print("3. Sending TradingView file...")
        tv_message = f"📋 **TradingView Watchlist File**\nGenerated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        file_sent = notifier.send_file(tradingview_file_path, tv_message)
        
        if file_sent:
            print("✅ TradingView file sent successfully!")
        else:
            print("❌ Failed to send TradingView file")
        
        # Summary
        if message_sent and matches_sent and file_sent:
            print("\n🎉 All Discord notifications sent successfully!")
        else:
            print(f"\n⚠️ Some notifications failed. Summary: {message_sent}, Matches: {matches_sent}, File: {file_sent}")
            
    except Exception as e:
        print(f"Error during Discord notification test: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    test_discord_similarity_notification()
