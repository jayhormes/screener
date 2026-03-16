#!/usr/bin/env python3
"""
Test comprehensive cleanup functionality
"""
import os
import tempfile
import shutil
from datetime import datetime


def create_test_similarity_structure():
    """Create a test similarity output structure"""
    
    # Create main directory structure like: similarity_output/20250825_180720/
    base_dir = "test_similarity_output"
    timestamp_dir = os.path.join(base_dir, "20250825_180720")
    os.makedirs(timestamp_dir, exist_ok=True)
    
    # Create main files
    report_file = os.path.join(timestamp_dir, "similarity_search_report.txt")
    tv_file = os.path.join(timestamp_dir, "2025-08-25_18-36_similar_trend_tradingview.txt")
    other_tv_file = os.path.join(timestamp_dir, "2025-08-25_18-30_similar_trend_tradingview.txt")
    
    with open(report_file, 'w') as f:
        f.write("Test similarity report content")
    
    with open(tv_file, 'w') as f:
        f.write("Test TradingView content 1")
        
    with open(other_tv_file, 'w') as f:
        f.write("Test TradingView content 2")
    
    # Create vis directories
    vis_dirs = [
        "vis_15m_CRV_4h_uptrend",
        "vis_1h_AVAX_1h_standard", 
        "vis_2h_GMT_4h_uptrend",
        "vis_30m_LQTY_30m_standard"
    ]
    
    for vis_dir in vis_dirs:
        vis_path = os.path.join(timestamp_dir, vis_dir)
        os.makedirs(vis_path, exist_ok=True)
        
        # Add some files in vis directories
        with open(os.path.join(vis_path, "alignment.png"), 'w') as f:
            f.write("Test visualization")
        with open(os.path.join(vis_path, "data.pkl"), 'w') as f:
            f.write("Test data")
    
    return timestamp_dir, report_file, tv_file


def test_comprehensive_cleanup():
    """Test the comprehensive cleanup functionality"""
    
    print("🧪 Testing Comprehensive Cleanup Functionality")
    print("=" * 60)
    
    # Create test structure
    timestamp_dir, report_file, tv_file = create_test_similarity_structure()
    
    # Show initial structure
    print(f"📁 Created test structure: {timestamp_dir}")
    total_items = 0
    for root, dirs, files in os.walk(timestamp_dir):
        total_items += len(dirs) + len(files)
        if root == timestamp_dir:
            print(f"📊 Main directory items: {len(dirs) + len(files)}")
            for item in dirs + files:
                print(f"  - {item}")
    
    print(f"📈 Total items in structure: {total_items}")
    
    # Test the cleanup function
    try:
        from send_similarity_to_discord import cleanup_local_files
        
        print(f"\n🧹 Testing comprehensive cleanup...")
        print(f"Target files: {os.path.basename(report_file)}, {os.path.basename(tv_file)}")
        
        success = cleanup_local_files(report_file, tv_file)
        
        print(f"\nCleanup result: {'✅ Success' if success else '❌ Failed'}")
        
        # Check what remains
        if os.path.exists(timestamp_dir):
            remaining = []
            for root, dirs, files in os.walk(timestamp_dir):
                for d in dirs:
                    remaining.append(f"DIR: {os.path.relpath(os.path.join(root, d), timestamp_dir)}")
                for f in files:
                    remaining.append(f"FILE: {os.path.relpath(os.path.join(root, f), timestamp_dir)}")
            
            print(f"📊 Remaining items: {len(remaining)}")
            for item in remaining:
                print(f"  - {item}")
        else:
            print("📁 Entire directory structure was removed!")
            
    except Exception as e:
        print(f"❌ Error testing cleanup: {e}")
        import traceback
        traceback.print_exc()
    
    # Cleanup test directory
    try:
        base_dir = os.path.dirname(timestamp_dir)
        if os.path.exists(base_dir):
            shutil.rmtree(base_dir)
            print(f"\n🗑️ Final cleanup: removed {base_dir}")
    except Exception as e:
        print(f"⚠️ Could not clean up test directory: {e}")


if __name__ == "__main__":
    test_comprehensive_cleanup()
