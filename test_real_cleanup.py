#!/usr/bin/env python3
"""
Test cleanup with real similarity output directory
"""
import os
from send_similarity_to_discord import cleanup_local_files


def test_real_cleanup():
    """Test cleanup with a real similarity output directory"""
    
    # Use an existing file from the real directory
    real_dir = "similarity_output/20250825_180720"
    test_files = [
        os.path.join(real_dir, "2025-08-25_18-13_similar_trend_tradingview.txt"),
        os.path.join(real_dir, "2025-08-25_18-18_similar_trend_tradingview.txt")
    ]
    
    # Check what exists before cleanup
    print("📁 Before cleanup:")
    if os.path.exists(real_dir):
        items = os.listdir(real_dir)
        print(f"Items in {real_dir}: {len(items)}")
        for item in items[:5]:  # Show first 5 items
            print(f"  - {item}")
        if len(items) > 5:
            print(f"  ... and {len(items) - 5} more items")
    
    print(f"\n🧹 Testing cleanup with real files...")
    
    # Test cleanup with existing files
    existing_files = [f for f in test_files if os.path.exists(f)]
    if existing_files:
        print(f"Found {len(existing_files)} files to test with")
        
        success = cleanup_local_files(*existing_files)
        print(f"Cleanup result: {'✅ Success' if success else '❌ Failed'}")
        
        # Check what remains
        if os.path.exists(real_dir):
            remaining = os.listdir(real_dir)
            print(f"\n📊 After cleanup: {len(remaining)} items remaining")
            for item in remaining[:5]:
                print(f"  - {item}")
            if len(remaining) > 5:
                print(f"  ... and {len(remaining) - 5} more items")
        else:
            print("\n📁 Directory was completely removed!")
    else:
        print("No existing files found to test with")


if __name__ == "__main__":
    test_real_cleanup()
