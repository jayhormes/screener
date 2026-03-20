from downloader import CryptoDownloader
import time

def test_binance_api_exception():
    downloader = CryptoDownloader()
    start_ts = int(time.time()) - 7 * 24 * 3600  # 過去一週
    end_ts = int(time.time())

    try:
        print("=== Start Binance API Test ===")
        success, df, _ = downloader.get_data("BTCUSDT", start_ts=start_ts, end_ts=end_ts, timeframe="1h")
        print(f"Success: {success}")
        if not df.empty:
            print(df.head())
        else:
            print("No Data Retrieved.")
    except Exception as e:
        print(f"[Exception Caught]")
        print(f"Type: {type(e)}")
        print(f"Content: {e}")
    print("=== Test Complete ===")

if __name__ == "__main__":
    test_binance_api_exception()
