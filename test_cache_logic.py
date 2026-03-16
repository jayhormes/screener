import os
import sys
import time
sys.path.append(os.getcwd())

from src.downloader import CryptoDownloader

def test_cache_logic():
    d = CryptoDownloader()
    symbol = 'BTCUSDT'
    timeframe = '1h'
    end_ts = int(time.time())
    start_ts = end_ts - 3600 * 24
    
    print("--- First run (should fetch/save) ---")
    s = time.time()
    d.get_data(symbol, start_ts=start_ts, end_ts=end_ts, timeframe=timeframe)
    print(f"Time: {time.time()-s:.4f}s")
    
    print("\n--- Second run (should be cache hit, no save) ---")
    s = time.time()
    # Use exact same end_ts to ensure perfect match
    d.get_data(symbol, start_ts=start_ts, end_ts=end_ts, timeframe=timeframe)
    print(f"Time: {time.time()-s:.4f}s")

    print("\n--- Third run (slightly later end_ts) ---")
    time.sleep(1)
    new_end_ts = int(time.time())
    s = time.time()
    d.get_data(symbol, start_ts=start_ts, end_ts=new_end_ts, timeframe=timeframe)
    print(f"Time: {time.time()-s:.4f}s")

if __name__ == "__main__":
    test_cache_logic()
