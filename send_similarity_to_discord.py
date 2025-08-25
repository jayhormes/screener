#!/usr/bin/env python3
"""
Send existing similarity analysis results to Discord
"""
import os
import sys
from datetime import datetime
from src.discord_notifier import DiscordNotifier
from src.message_formatter import TrendSimilarityMessageFormatter


def cleanup_local_files(*file_paths) -> bool:
    """
    Clean up local files after successful Discord transmission
    
    Args:
        *file_paths: Variable number of file paths to delete
        
    Returns:
        bool: True if all files were deleted successfully, False otherwise
    """
    success = True
    deleted_count = 0
    
    # Delete specified files
    for file_path in file_paths:
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
                print(f"  🗑️ Deleted: {os.path.basename(file_path)}")
                deleted_count += 1
            except Exception as e:
                print(f"  ❌ Failed to delete {os.path.basename(file_path)}: {e}")
                success = False
        else:
            print(f"  ⚠️ File not found: {os.path.basename(file_path)}")
    
    # Clean up the similarity output directory comprehensively
    try:
        similarity_dir = os.path.dirname(file_paths[0]) if file_paths else None
        if similarity_dir and os.path.exists(similarity_dir):
            
            # Remove all visualization directories (vis_*)
            for item in os.listdir(similarity_dir):
                item_path = os.path.join(similarity_dir, item)
                if os.path.isdir(item_path) and item.startswith('vis_'):
                    try:
                        import shutil
                        shutil.rmtree(item_path)
                        print(f"  🗂️ Removed visualization directory: {item}")
                        deleted_count += 1
                    except Exception as e:
                        print(f"  ❌ Failed to remove directory {item}: {e}")
                        success = False
            
            # Remove any remaining TradingView files in the directory
            for item in os.listdir(similarity_dir):
                item_path = os.path.join(similarity_dir, item)
                if os.path.isfile(item_path) and item.endswith('_similar_trend_tradingview.txt'):
                    try:
                        os.remove(item_path)
                        print(f"  🗑️ Deleted remaining TradingView file: {item}")
                        deleted_count += 1
                    except Exception as e:
                        print(f"  ❌ Failed to delete {item}: {e}")
                        success = False
            
            # Check if directory is now empty and remove it
            try:
                remaining_items = os.listdir(similarity_dir)
                if not remaining_items:
                    os.rmdir(similarity_dir)
                    print(f"  🗂️ Removed empty directory: {os.path.basename(similarity_dir)}")
                    deleted_count += 1
                else:
                    print(f"  📁 Directory not empty, keeping: {os.path.basename(similarity_dir)} ({len(remaining_items)} items remaining)")
            except Exception as e:
                print(f"  ⚠️ Could not remove directory: {e}")
    
    except Exception as e:
        print(f"  ❌ Error during directory cleanup: {e}")
        success = False
    
    print(f"  📊 Cleaned up {deleted_count} items")
    return success


def send_similarity_results_to_discord():
    """Send the latest similarity analysis results to Discord"""
    
    # Path to the latest results
    similarity_report_path = "similarity_output/20250825_180720/similarity_search_report.txt"
    tradingview_file_path = "similarity_output/20250825_180720/2025-08-25_18-36_similar_trend_tradingview.txt"
    
    print("🚀 Sending Crypto Trend Similarity Results to Discord")
    print("=" * 60)
    
    # Check if files exist
    if not os.path.exists(similarity_report_path):
        print(f"❌ Similarity report not found: {similarity_report_path}")
        return False
    
    if not os.path.exists(tradingview_file_path):
        print(f"❌ TradingView file not found: {tradingview_file_path}")
        return False
    
    # Read the results
    print("📖 Reading analysis results...")
    with open(similarity_report_path, 'r', encoding='utf-8') as f:
        results_text = f.read()
    
    try:
        # Initialize Discord notifier
        print("🔗 Connecting to Discord...")
        notifier = DiscordNotifier(use_trend_finder=True)
        
        if not notifier.enabled:
            print("⚠️ Discord notifications are disabled in config")
            return False
        
        print("✅ Discord connection established")
        
        # Generate messages
        print("📝 Formatting messages...")
        
        # Summary message
        summary_message = TrendSimilarityMessageFormatter.format_similarity_results_summary(
            results_text, 
            total_timeframes=5,  # 15m, 30m, 1h, 2h, 4h
            total_references=8   # Based on the reference trends
        )
        
        # Top matches message
        top_matches_message = TrendSimilarityMessageFormatter.format_top_matches_by_timeframe(
            results_text, 
            max_per_timeframe=5
        )
        
        # Send messages
        print("📤 Sending messages to Discord...")
        
        # 1. Send summary
        print("  📊 Sending summary...")
        summary_sent = notifier.send_message(summary_message)
        if summary_sent:
            print("  ✅ Summary sent successfully!")
        else:
            print("  ❌ Failed to send summary")
        
        # 2. Send top matches
        print("  🎯 Sending top matches...")
        matches_sent = notifier.send_message(top_matches_message)
        if matches_sent:
            print("  ✅ Top matches sent successfully!")
        else:
            print("  ❌ Failed to send top matches")
        
        # 3. Send TradingView file
        print("  📋 Sending TradingView file...")
        tv_message = (f"📋 **TradingView Watchlist File**\n"
                     f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                     f"Contains ready-to-import symbols for TradingView")
        
        file_sent = notifier.send_file(tradingview_file_path, tv_message)
        if file_sent:
            print("  ✅ TradingView file sent successfully!")
        else:
            print("  ❌ Failed to send TradingView file")
        
        # Results summary
        print("\n" + "=" * 60)
        success_count = sum([summary_sent, matches_sent, file_sent])
        
        if success_count == 3:
            print("🎉 ALL NOTIFICATIONS SENT SUCCESSFULLY!")
            print("   ✅ Summary message")
            print("   ✅ Top matches")
            print("   ✅ TradingView file")
            
            # Clean up local files after successful transmission
            print("\n🧹 Cleaning up local files...")
            cleanup_success = cleanup_local_files(similarity_report_path, tradingview_file_path)
            if cleanup_success:
                print("✅ Local files cleaned up successfully!")
            else:
                print("⚠️ Some files could not be cleaned up")
        else:
            print(f"⚠️  {success_count}/3 notifications sent successfully")
            print(f"   {'✅' if summary_sent else '❌'} Summary message")
            print(f"   {'✅' if matches_sent else '❌'} Top matches") 
            print(f"   {'✅' if file_sent else '❌'} TradingView file")
            print("📁 Files preserved due to incomplete transmission")
        
        return success_count == 3
        
    except Exception as e:
        print(f"❌ Error sending Discord notifications: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = send_similarity_results_to_discord()
    sys.exit(0 if success else 1)
