import argparse
import time
import os
import numpy as np
import json
import random
from datetime import datetime, timedelta
from src.downloader import CryptoDownloader
from src.discord_notifier import get_discord_notifier
from src.message_formatter import CryptoMessageFormatter


class RateLimiter:
    """智能請求頻率控制器"""
    
    def __init__(self, config=None):
        self.request_times = []
        self.consecutive_errors = 0
        
        # 從配置文件讀取參數，如果沒有則使用默認值
        rate_limit_config = config.get('crypto_screener', {}).get('rate_limiter', {}) if config else {}
        
        self.base_delay = rate_limit_config.get('base_delay', 0.25)  # 基礎延遲（秒）
        self.max_delay = rate_limit_config.get('max_delay', 10.0)   # 最大延遲（秒）
        self.max_requests_per_minute = rate_limit_config.get('max_requests_per_minute', 2200)  # 保守的每分鐘請求限制
        self.error_backoff_multiplier = rate_limit_config.get('error_backoff_multiplier', 2.0)  # 錯誤時的延遲倍數
        
    def wait_if_needed(self):
        """在發送請求前檢查是否需要等待"""
        current_time = time.time()
        
        # 清理超過1分鐘的舊記錄
        self.request_times = [t for t in self.request_times if current_time - t < 60]
        
        # 如果過去1分鐘內的請求數接近限制，則等待
        if len(self.request_times) >= self.max_requests_per_minute:
            wait_time = 60 - (current_time - self.request_times[0]) + 1
            print(f"Rate limit approaching, waiting {wait_time:.1f} seconds...")
            time.sleep(wait_time)
            # 重新清理記錄
            current_time = time.time()
            self.request_times = [t for t in self.request_times if current_time - t < 60]
        
        # 計算動態延遲
        delay = self.calculate_dynamic_delay()
        
        if delay > 0:
            time.sleep(delay)
        
        # 記錄這次請求時間
        self.request_times.append(time.time())
    
    def calculate_dynamic_delay(self):
        """計算動態延遲時間"""
        # 基礎延遲
        delay = self.base_delay
        
        # 根據連續錯誤數增加延遲
        if self.consecutive_errors > 0:
            delay *= (self.error_backoff_multiplier ** min(self.consecutive_errors, 5))
        
        # 根據當前請求頻率調整延遲
        current_time = time.time()
        recent_requests = len([t for t in self.request_times if current_time - t < 10])
        
        if recent_requests > 20:  # 最近10秒內超過20個請求
            delay *= 1.5
        elif recent_requests > 15:  # 最近10秒內超過15個請求
            delay *= 1.2
        
        # 添加隨機抖動避免同步請求
        jitter = random.uniform(0.8, 1.2)
        delay *= jitter
        
        return min(delay, self.max_delay)
    
    def on_error(self, error_msg):
        """處理API錯誤"""
        self.consecutive_errors += 1
        
        # 檢查是否是頻率限制錯誤
        if "Too many requests" in error_msg or "rate limit" in error_msg.lower():
            print(f"Rate limit detected! Consecutive errors: {self.consecutive_errors}")
            # 對於頻率限制錯誤，等待更長時間
            backoff_time = min(5.0 * (2 ** min(self.consecutive_errors - 1, 4)), 60.0)
            print(f"Backing off for {backoff_time:.1f} seconds...")
            time.sleep(backoff_time)
        elif "APIError" in error_msg:
            # 對於其他API錯誤，較短的等待時間
            backoff_time = min(1.0 * self.consecutive_errors, 10.0)
            print(f"API error detected, backing off for {backoff_time:.1f} seconds...")
            time.sleep(backoff_time)
    
    def on_success(self):
        """處理成功的請求"""
        # 逐漸減少連續錯誤計數
        if self.consecutive_errors > 0:
            self.consecutive_errors = max(0, self.consecutive_errors - 1)


def load_config(config_path="config.json"):
    """Load configuration from config.json"""
    try:
        if os.path.exists(config_path):
            with open(config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        else:
            print(f"Config file {config_path} not found, using defaults")
            return {}
    except Exception as e:
        print(f"Error loading config: {e}")
        return {}


def cleanup_old_folders(base_folder="output", days_to_keep=7):
    """
    Clean up old date folders in the output directory
    
    Args:
        base_folder: Base output folder path
        days_to_keep: Number of days to keep folders for
    """
    if not os.path.exists(base_folder):
        return
    
    try:
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        deleted_folders = []
        
        for folder_name in os.listdir(base_folder):
            folder_path = os.path.join(base_folder, folder_name)
            
            # Skip if not a directory
            if not os.path.isdir(folder_path):
                continue
                
            # Try to parse folder name as date (YYYY-MM-DD format)
            try:
                folder_date = datetime.strptime(folder_name, "%Y-%m-%d")
                
                # If folder is older than cutoff date, delete it
                if folder_date < cutoff_date:
                    # Check if folder is empty or has old files
                    try:
                        files_in_folder = os.listdir(folder_path)
                        if not files_in_folder:
                            # Empty folder, safe to delete
                            os.rmdir(folder_path)
                            deleted_folders.append(folder_name)
                        else:
                            # Has files, delete files older than cutoff and then folder if empty
                            files_deleted = []
                            for file_name in files_in_folder:
                                file_path = os.path.join(folder_path, file_name)
                                file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                                if file_mtime < cutoff_date:
                                    os.remove(file_path)
                                    files_deleted.append(file_name)
                            
                            # Check if folder is now empty
                            if not os.listdir(folder_path):
                                os.rmdir(folder_path)
                                deleted_folders.append(folder_name)
                                
                    except Exception as e:
                        print(f"⚠️ Warning: Could not process folder {folder_path}: {e}")
                        
            except ValueError:
                # Not a date folder, skip
                continue
                
        if deleted_folders:
            print(f"🗑️ Cleaned up old folders: {', '.join(deleted_folders)}")
            
    except Exception as e:
        print(f"⚠️ Warning: Error during folder cleanup: {e}")


def calc_total_bars(time_interval, days):
    bars_dict = {
        "5m": 12 * 24 * days,
        "15m": 4 * 24 * days,
        "30m": 2 * 24 * days,
        "1h":  24 * days,
        "2h": 12 * days,
        "4h": 6 * days,
        "8h": 3 * days,
    }
    return bars_dict.get(time_interval)


def calculate_rs_score(crypto_data, required_bars):
    """
    Calculate RS score for cryptocurrency
    
    Args:
        crypto_data: DataFrame with cryptocurrency data
        required_bars: Number of bars required for calculation
        
    Returns:
        tuple[bool, float, str]: Success flag, RS score, error message
    """
    # Check if we have enough data
    if len(crypto_data) < required_bars:
        return False, 0, f"Insufficient data: {len(crypto_data)} < {required_bars}"
    
    # Create a copy to avoid modifying the original data
    data = crypto_data.copy()
    
    # Take the most recent required_bars data points
    data = data.tail(required_bars).reset_index(drop=True)
    
    # Calculate RS Score
    rs_score = 0.0
    total_weight = 0.0
    
    # Calculate for each data point
    for i in range(required_bars):
        # Current data point values
        current_close = data['close'].iloc[i]
        moving_average_30 = data['sma_30'].iloc[i]
        moving_average_45 = data['sma_45'].iloc[i]
        moving_average_60 = data['sma_60'].iloc[i]
        current_atr = data['atr'].iloc[i]
        
        # Calculate relative strength numerator
        numerator = ((current_close - moving_average_30) +
                     (current_close - moving_average_45) +
                     (current_close - moving_average_60) +
                     (moving_average_30 - moving_average_45) +
                     (moving_average_30 - moving_average_60) +
                     (moving_average_45 - moving_average_60))
        
        # Use ATR as denominator with small epsilon to avoid division by zero
        denominator = current_atr + 0.0000000000000000001
        # denominator = (moving_average_30 + moving_average_45 + moving_average_60) / 3
        
        # Calculate relative strength for this point
        relative_strength = numerator / denominator
        
        # Gives higher importance to newer data
        # weight = i 
        k = 2 * np.log(2) / required_bars   
        weight = np.exp(k * i)              # Exponential weight where w(L/2) * 2 = w(L)
        
        
        # Add to weighted sum
        rs_score += relative_strength * weight
        total_weight += weight
    
    # Normalize the final score by total weight
    if total_weight > 0:
        rs_score = rs_score / total_weight
    else:
        return False, 0, "Weight calculation error"

    return True, rs_score, ""


def process_crypto(symbol, timeframe, days, rate_limiter):
    """Process a single cryptocurrency and calculate its RS score"""
    try:
        # 使用智能頻率控制器
        rate_limiter.wait_if_needed()
        
        cd = CryptoDownloader()
        
        # Calculate required bars
        required_bars = calc_total_bars(timeframe, days)
        
        # Calculate start timestamp with some buffer (20% more time to ensure we get enough data)
        buffer_factor = 1.2
        now = int(time.time())
        
        # Estimate interval seconds based on timeframe
        if "m" in timeframe:
            minutes = int(timeframe.replace("m", ""))
            interval_seconds = minutes * 60
        elif "h" in timeframe:
            hours = int(timeframe.replace("h", ""))
            interval_seconds = hours * 3600
        elif "d" in timeframe:
            days = int(timeframe.replace("d", ""))
            interval_seconds = days * 24 * 3600
        else:
            # Default to 1h if unknown format
            interval_seconds = 3600
        
        start_ts = now - int(required_bars * interval_seconds * buffer_factor)
        
        # Get crypto data
        success, data = cd.get_data(symbol, start_ts=start_ts, end_ts=now, timeframe=timeframe, atr=True)
        
        if not success or data.empty:
            error_msg = "Failed to get data or empty dataset"
            print(f"{symbol} -> Error: {error_msg}")
            rate_limiter.on_error(error_msg)
            return {"crypto": symbol, "status": "failed", "reason": error_msg}
        
        # Calculate RS score
        success, rs_score, error = calculate_rs_score(data, required_bars)
        if not success:
            print(f"{symbol} -> Error: {error}")
            rate_limiter.on_error(error)
            return {"crypto": symbol, "status": "failed", "reason": error}
        
        print(f"{symbol} -> Successfully calculated RS Score: {rs_score}")
        rate_limiter.on_success()
        return {
            "crypto": symbol,
            "status": "success",
            "rs_score": rs_score
        }
        
    except Exception as e:
        error_msg = str(e)
        print(f"{symbol} -> Error: {error_msg}")
        rate_limiter.on_error(error_msg)
        return {"crypto": symbol, "status": "failed", "reason": error_msg}


if __name__ == '__main__':
    # Load configuration
    config = load_config()
    
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--timeframe', type=str, help='Time frame (5m, 15m, 30m, 1h, 2h, 4h, 8h, 1d)', 
                       default=config.get('crypto_screener', {}).get('default_timeframe', '15m'))
    parser.add_argument('-d', '--days', type=int, help='Calculation duration in days (default 3 days)', 
                       default=config.get('crypto_screener', {}).get('default_days', 3))
    args = parser.parse_args()
    timeframe = args.timeframe
    days = args.days
    
    # Initialize crypto downloader and rate limiter
    crypto_downloader = CryptoDownloader()
    rate_limiter = RateLimiter(config)
    
    # Get list of all symbols
    all_cryptos = crypto_downloader.get_all_symbols()
    print(f"Total cryptos to process: {len(all_cryptos)}")
    
    # Process all cryptos sequentially with intelligent rate limiting
    print("Using intelligent rate limiting to avoid API limits")
    print(f"Base delay: {rate_limiter.base_delay}s, Max requests per minute: {rate_limiter.max_requests_per_minute}")
    results = []
    
    for i, crypto in enumerate(all_cryptos, 1):
        print(f"Processing {i}/{len(all_cryptos)}: {crypto}")
        try:
            result = process_crypto(crypto, timeframe, days, rate_limiter)
            results.append(result)
        except KeyboardInterrupt:
            print("\nProcessing interrupted by user. Saving partial results...")
            break
        except Exception as e:
            print(f"{crypto} -> Error: {str(e)}")
            rate_limiter.on_error(str(e))
            results.append({"crypto": crypto, "status": "failed", "reason": str(e)})
    
    # Process results
    failed_targets = []     # Failed to download data or error happened
    target_score = {}
    
    for result in results:
        if result["status"] == "success":
            target_score[result["crypto"]] = result["rs_score"]
        else:
            failed_targets.append((result["crypto"], result["reason"]))
    
    # Sort by RS score
    targets = [x for x in target_score.keys()]
    targets.sort(key=lambda x: target_score[x], reverse=True)
    
    # Print results
    print(f"\nAnalysis Results:")
    print(f"Total cryptos processed: {len(all_cryptos)}")
    print(f"Failed cryptos: {len(failed_targets)}")
    print(f"Successfully calculated: {len(targets)}")
    
    print("\n=========================== Target : Score (TOP 20) ===========================")
    for idx, crypto in enumerate(targets[:20], 1):
        score = target_score[crypto]
        # Remove USDT suffix if present, keep other suffixes like USDC
        display_symbol = crypto
        if crypto.endswith('USDT'):
            display_symbol = crypto[:-4]  # Remove 'USDT'
        print(f"{idx}. {display_symbol}: {score:.6f}")
    print("===============================================================================")
    
    # Save results
    full_date_str = datetime.now().strftime("%Y-%m-%d_%H-%M")
    date_str = datetime.now().strftime("%Y-%m-%d")
    txt_content = "###Targets (Sort by score)\n"
    
    # Add only TOP 20 targets
    if targets:
        top_20_targets = targets[:20]
        txt_content += ",".join([f"BINANCE:{crypto}.P" for crypto in top_20_targets])
    
    # Create output/<date> directory structure
    base_folder = "output"
    date_folder = os.path.join(base_folder, date_str)
    os.makedirs(date_folder, exist_ok=True)
    
    # Save the file with full timestamp in filename
    output_file = f"{full_date_str}_crypto_{timeframe}_strong_targets.txt"
    file_path = os.path.join(date_folder, output_file)
    with open(file_path, "w") as f:
        f.write(txt_content)
    
    # Save failed cryptos for analysis
    # failed_file = f"{full_date_str}_failed_cryptos_{timeframe}.txt"
    # failed_path = os.path.join(date_folder, failed_file)
    # with open(failed_path, "w") as f:
    #     for crypto, reason in failed_targets:
    #         f.write(f"{crypto}: {reason}\n")
    
    print(f"\nResults saved to {file_path}")
    
    # Send to Discord if enabled in config
    discord_notifier = get_discord_notifier()
    if targets and discord_notifier.enabled:
        print("\nSending results to Discord...")
        
        # Format the message using the message formatter
        formatted_message = CryptoMessageFormatter.format_crypto_results(
            targets=targets,
            target_scores=target_score,
            timeframe=timeframe,
            days=days,
            total_processed=len(all_cryptos),
            failed_count=len(failed_targets),
            timestamp=full_date_str,
            max_targets=20
        )
        
        # Format file message
        file_message = CryptoMessageFormatter.format_file_message(
            timeframe=timeframe,
            days=days
        )
        
        # Send message and file separately
        message_success, file_success = discord_notifier.send_crypto_results_with_file(
            message=formatted_message,
            file_path=file_path,
            file_message=file_message
        )
        
        if message_success:
            print("✅ Successfully sent message to Discord!")
        else:
            print("❌ Failed to send message to Discord")
            
        if file_success:
            print("✅ Successfully sent file to Discord!")
            # Delete the file after successful upload to save space (if enabled in config)
            config = load_config()
            delete_after_upload = config.get("discord", {}).get("delete_files_after_upload", True)
            
            if delete_after_upload:
                try:
                    os.remove(file_path)
                    print(f"🗑️ Deleted file after successful upload: {file_path}")
                    
                    # Check if the date folder is now empty and remove it
                    date_folder_path = os.path.dirname(file_path)
                    try:
                        # Check if folder is empty
                        if not os.listdir(date_folder_path):
                            os.rmdir(date_folder_path)
                            print(f"🗑️ Deleted empty date folder: {date_folder_path}")
                    except Exception as e:
                        print(f"⚠️ Warning: Could not delete empty folder {date_folder_path}: {e}")
                        
                except Exception as e:
                    print(f"⚠️ Warning: Could not delete file {file_path}: {e}")
            else:
                print(f"📁 File preserved (delete_files_after_upload=false): {file_path}")
        else:
            print("❌ Failed to send file to Discord")
            
    elif not discord_notifier.enabled:
        print("Discord notifications are disabled in config")
    
    # Clean up old folders if configured
    config = load_config()
    cleanup_days = config.get("discord", {}).get("cleanup_old_folders_days", 7)
    if cleanup_days > 0:
        print(f"\nCleaning up folders older than {cleanup_days} days...")
        cleanup_old_folders("output", cleanup_days)
    
    # print(f"Failed cryptos saved to {failed_path}")
