import pandas as pd
import numpy as np
import time

def benchmark_processing():
    # 11639 points
    n = 11639
    df = pd.DataFrame({
        'timestamp': np.arange(n),
        'open': np.random.rand(n),
        'high': np.random.rand(n),
        'low': np.random.rand(n),
        'close': np.random.rand(n),
        'volume': np.random.rand(n)
    })
    
    # Simulate SMA calculation
    sma_periods = [30, 45, 60]
    
    start = time.time()
    for p in sma_periods:
        df[f'sma_{p}'] = df['close'].rolling(window=p).mean()
    
    # Simulate further processing in TimeSeriesProcessor
    df['SMA30_SMA45'] = df['sma_30'] - df['sma_45']
    df['SMA30_SMA60'] = df['sma_30'] - df['sma_60']
    df['SMA45_SMA60'] = df['sma_45'] - df['sma_60']
    
    duration = time.time() - start
    print(f"Processing {n} points took {duration:.4f}s")

if __name__ == "__main__":
    benchmark_processing()
