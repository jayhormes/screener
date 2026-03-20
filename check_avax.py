import pandas as pd
from binance.client import Client
from datetime import datetime
import pytz

def get_avax_prices():
    client = Client()
    symbol = "AVAXUSDT"
    interval = Client.KLINE_INTERVAL_1HOUR
    
    # Timezone conversion
    la_tz = pytz.timezone("America/Los_Angeles")
    
    # Reference times
    start_dt = la_tz.localize(datetime(2023, 11, 9, 12, 0))
    end_dt = la_tz.localize(datetime(2023, 11, 14, 18, 0))
    
    # Get timestamps in ms
    start_ts = int(start_dt.timestamp() * 1000)
    end_ts = int(end_dt.timestamp() * 1000)
    
    # Fetch klines
    klines = client.get_historical_klines(symbol, interval, start_ts, end_ts)
    
    if not klines:
        print("No data found")
        return
        
    df = pd.DataFrame(klines, columns=['time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'qav', 'num_trades', 'taker_base_vol', 'taker_quote_vol', 'ignore'])
    df['time'] = pd.to_datetime(df['time'], unit='ms')
    
    start_close = float(df.iloc[0]['close'])
    end_close = float(df.iloc[-1]['close'])
    max_high = float(df['high'].max())
    
    print(f"Start Time (PST): {start_dt}")
    print(f"End Time (PST): {end_dt}")
    print(f"Start Close: {start_close}")
    print(f"End Close: {end_close}")
    print(f"Max High in period: {max_high}")
    print(f"Total bars: {len(df)}")

if __name__ == "__main__":
    get_avax_prices()
