#!/usr/bin/env python3
"""
Test script to verify cleanup functionality works correctly
"""
import os
import tempfile
import shutil
from datetime import datetime


def test_cleanup_functionality():
    """Test the cleanup functions"""
    
    print("🧪 Testing File Cleanup Functionality")
    print("=" * 50)
    
    # Create temporary test files
    test_dir = "test_similarity_output"
    os.makedirs(test_dir, exist_ok=True)
    
    # Create test files
    test_report = os.path.join(test_dir, "test_similarity_report.txt")
    test_tv_file = os.path.join(test_dir, "test_tradingview.txt")
    test_vis_dir = os.path.join(test_dir, "vis_test_pattern")
    
    # Write test content
    with open(test_report, 'w') as f:
        f.write("Test similarity report content")
    
    with open(test_tv_file, 'w') as f:
        f.write("Test TradingView content")
    
    os.makedirs(test_vis_dir, exist_ok=True)
    with open(os.path.join(test_vis_dir, "test_vis.png"), 'w') as f:
        f.write("Test visualization")
    
    print(f"📁 Created test directory: {test_dir}")
    print(f"📄 Created test files: {len(os.listdir(test_dir))} items")
    
    # Import and test the cleanup function
    try:
        from send_similarity_to_discord import cleanup_local_files
        
        print("\n🧹 Testing cleanup_local_files function...")
        success = cleanup_local_files(test_report, test_tv_file)
        
        print(f"Cleanup result: {'✅ Success' if success else '❌ Failed'}")
        
        # Check what remains
        if os.path.exists(test_dir):
            remaining = os.listdir(test_dir)
            print(f"📊 Remaining items: {len(remaining)}")
            for item in remaining:
                print(f"  - {item}")
        else:
            print("📁 Test directory was completely removed")
            
    except Exception as e:
        print(f"❌ Error testing cleanup: {e}")
        import traceback
        traceback.print_exc()
    
    # Cleanup test directory
    try:
        if os.path.exists(test_dir):
            shutil.rmtree(test_dir)
            print(f"\n🗑️ Cleaned up test directory: {test_dir}")
    except Exception as e:
        print(f"⚠️ Could not clean up test directory: {e}")


if __name__ == "__main__":
    test_cleanup_functionality()
