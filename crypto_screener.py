import argparse
import asyncio
import json
import os
import time
from datetime import datetime, timedelta

import numpy as np

from src.discord_notifier import get_discord_notifier
from src.downloader import CryptoDownloader
from src.message_formatter import CryptoMessageFormatter


def load_config(config_path="config.json"):
    try:
        if os.path.exists(config_path):
            with open(config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        print(f"Config file {config_path} not found, using defaults")
        return {}
    except Exception as e:
        print(f"Error loading config: {e}")
        return {}


def cleanup_old_folders(base_folder="output", days_to_keep=7):
    if not os.path.exists(base_folder):
        return

    try:
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        deleted_folders = []

        for folder_name in os.listdir(base_folder):
            folder_path = os.path.join(base_folder, folder_name)
            if not os.path.isdir(folder_path):
                continue

            try:
                folder_date = datetime.strptime(folder_name, "%Y-%m-%d")
                if folder_date < cutoff_date:
                    files_in_folder = os.listdir(folder_path)
                    if not files_in_folder:
                        os.rmdir(folder_path)
                        deleted_folders.append(folder_name)
                    else:
                        for file_name in files_in_folder:
                            file_path = os.path.join(folder_path, file_name)
                            file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                            if file_mtime < cutoff_date:
                                os.remove(file_path)
                        if not os.listdir(folder_path):
                            os.rmdir(folder_path)
                            deleted_folders.append(folder_name)
            except ValueError:
                continue
            except Exception as e:
                print(f"Warning: Could not process folder {folder_path}: {e}")

        if deleted_folders:
            print(f"Cleaned up old folders: {', '.join(deleted_folders)}")

    except Exception as e:
        print(f"Warning: Error during folder cleanup: {e}")


def calc_total_bars(time_interval, days):
    bars_dict = {
        "5m": 12 * 24 * days,
        "15m": 4 * 24 * days,
        "30m": 2 * 24 * days,
        "1h": 24 * days,
        "2h": 12 * days,
        "4h": 6 * days,
        "8h": 3 * days,
        "1d": days,
    }
    return bars_dict.get(time_interval)


def timeframe_to_seconds(timeframe: str) -> int:
    if timeframe.endswith("m"):
        return int(timeframe[:-1]) * 60
    if timeframe.endswith("h"):
        return int(timeframe[:-1]) * 3600
    if timeframe.endswith("d"):
        return int(timeframe[:-1]) * 86400
    return 3600


def calculate_rs_score(crypto_data, required_bars):
    if len(crypto_data) < required_bars:
        return False, 0, f"Insufficient data: {len(crypto_data)} < {required_bars}"

    data = crypto_data.copy().tail(required_bars).reset_index(drop=True)

    rs_score = 0.0
    total_weight = 0.0

    for i in range(required_bars):
        current_close = data['close'].iloc[i]
        moving_average_30 = data['sma_30'].iloc[i]
        moving_average_45 = data['sma_45'].iloc[i]
        moving_average_60 = data['sma_60'].iloc[i]
        current_atr = data['atr'].iloc[i]

        numerator = ((current_close - moving_average_30) +
                     (current_close - moving_average_45) +
                     (current_close - moving_average_60) +
                     (moving_average_30 - moving_average_45) +
                     (moving_average_30 - moving_average_60) +
                     (moving_average_45 - moving_average_60))

        denominator = current_atr + 1e-19
        relative_strength = numerator / denominator

        k = 2 * np.log(2) / required_bars
        weight = np.exp(k * i)

        rs_score += relative_strength * weight
        total_weight += weight

    if total_weight <= 0:
        return False, 0, "Weight calculation error"

    return True, rs_score / total_weight, ""


async def process_crypto_async(symbol, timeframe, days, downloader: CryptoDownloader):
    try:
        required_bars = calc_total_bars(timeframe, days)
        if required_bars is None:
            return {"crypto": symbol, "status": "failed", "reason": f"Unsupported timeframe: {timeframe}"}

        buffer_factor = 1.2
        now = int(time.time())
        interval_seconds = timeframe_to_seconds(timeframe)
        start_ts = now - int(required_bars * interval_seconds * buffer_factor)

        success, data, _ = await downloader.get_data_async(
            symbol,
            start_ts=start_ts,
            end_ts=now,
            timeframe=timeframe,
            atr=True,
            validate=True,
        )

        if not success or data.empty:
            return {"crypto": symbol, "status": "failed", "reason": "Failed to get data or empty dataset"}

        success, rs_score, error = calculate_rs_score(data, required_bars)
        if not success:
            return {"crypto": symbol, "status": "failed", "reason": error}

        return {"crypto": symbol, "status": "success", "rs_score": rs_score}
    except Exception as e:
        return {"crypto": symbol, "status": "failed", "reason": str(e)}


async def run_rest_mode(all_cryptos, timeframe, days, downloader, max_concurrency=40):
    sem = asyncio.Semaphore(max_concurrency)

    async def guarded(symbol):
        async with sem:
            return await process_crypto_async(symbol, timeframe, days, downloader)

    tasks = [asyncio.create_task(guarded(symbol)) for symbol in all_cryptos]
    results = []
    for idx, task in enumerate(asyncio.as_completed(tasks), 1):
        result = await task
        results.append(result)
        symbol = result.get("crypto", "UNKNOWN")
        status = result.get("status", "failed")
        print(f"[{idx}/{len(all_cryptos)}] {symbol}: {status}")
    return results


async def run_websocket_mode(all_cryptos, timeframe, days, downloader, max_concurrency=40):
    print("WebSocket mode: 先以 REST 回填 + 計算，再啟動 WebSocket 持續更新")
    results = await run_rest_mode(all_cryptos, timeframe, days, downloader, max_concurrency=max_concurrency)

    # 進階模式：背景維持 WS 更新（此腳本先示範短時間運行）
    ws_duration_sec = downloader.ws_config.get("bootstrap_ws_seconds", 0)
    if ws_duration_sec > 0:
        stop_event = asyncio.Event()

        async def stop_later():
            await asyncio.sleep(ws_duration_sec)
            stop_event.set()

        print(f"啟動 WebSocket {ws_duration_sec} 秒進行即時更新...")
        ws_task = asyncio.create_task(downloader.stream_klines(all_cryptos[:1024], timeframe, stop_event=stop_event))
        stop_task = asyncio.create_task(stop_later())
        await asyncio.gather(ws_task, stop_task, return_exceptions=True)

    return results


def save_and_notify(results, all_cryptos, timeframe, days, config):
    failed_targets = []
    target_score = {}

    for result in results:
        if result.get("status") == "success":
            target_score[result["crypto"]] = result["rs_score"]
        else:
            failed_targets.append((result.get("crypto", "UNKNOWN"), result.get("reason", "unknown")))

    targets = sorted(target_score.keys(), key=lambda x: target_score[x], reverse=True)

    print(f"\nAnalysis Results:")
    print(f"Total cryptos processed: {len(all_cryptos)}")
    print(f"Failed cryptos: {len(failed_targets)}")
    print(f"Successfully calculated: {len(targets)}")

    print("\n=========================== Target : Score (TOP 20) ===========================")
    for idx, crypto in enumerate(targets[:20], 1):
        score = target_score[crypto]
        display_symbol = crypto[:-4] if crypto.endswith('USDT') else crypto
        print(f"{idx}. {display_symbol}: {score:.6f}")
    print("===============================================================================")

    full_date_str = datetime.now().strftime("%Y-%m-%d_%H-%M")
    date_str = datetime.now().strftime("%Y-%m-%d")
    txt_content = "###Targets (Sort by score)\n"

    if targets:
        top_20_targets = targets[:20]
        txt_content += ",".join([f"BINANCE:{crypto}.P" for crypto in top_20_targets])

    base_folder = "output"
    date_folder = os.path.join(base_folder, date_str)
    os.makedirs(date_folder, exist_ok=True)

    output_file = f"{full_date_str}_crypto_{timeframe}_strong_targets.txt"
    file_path = os.path.join(date_folder, output_file)
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(txt_content)

    print(f"\nResults saved to {file_path}")

    discord_notifier = get_discord_notifier()
    if targets and discord_notifier.enabled:
        print("\nSending results to Discord...")

        formatted_message = CryptoMessageFormatter.format_crypto_results(
            targets=targets,
            target_scores=target_score,
            timeframe=timeframe,
            days=days,
            total_processed=len(all_cryptos),
            failed_count=len(failed_targets),
            timestamp=full_date_str,
            max_targets=20,
        )

        file_message = CryptoMessageFormatter.format_file_message(timeframe=timeframe, days=days)
        message_success, file_success = discord_notifier.send_crypto_results_with_file(
            message=formatted_message,
            file_path=file_path,
            file_message=file_message,
        )

        print("Successfully sent message to Discord!" if message_success else "Failed to send message to Discord")
        print("Successfully sent file to Discord!" if file_success else "Failed to send file to Discord")

        if file_success and config.get("discord", {}).get("delete_files_after_upload", True):
            try:
                os.remove(file_path)
                date_folder_path = os.path.dirname(file_path)
                if not os.listdir(date_folder_path):
                    os.rmdir(date_folder_path)
            except Exception as e:
                print(f"Warning: Could not delete file/folder: {e}")

    elif not discord_notifier.enabled:
        print("Discord notifications are disabled in config")

    cleanup_days = config.get("discord", {}).get("cleanup_old_folders_days", 7)
    if cleanup_days > 0:
        print(f"\nCleaning up folders older than {cleanup_days} days...")
        cleanup_old_folders("output", cleanup_days)


async def async_main():
    config = load_config()
    cs_config = config.get("crypto_screener", {})
    async_conf = cs_config.get("async", {})

    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--timeframe', type=str, default=cs_config.get('default_timeframe', '15m'))
    parser.add_argument('-d', '--days', type=int, default=cs_config.get('default_days', 3))
    parser.add_argument('--mode', type=str, choices=['rest', 'websocket'], default=cs_config.get('mode', 'rest'))
    parser.add_argument('--max-concurrency', type=int, default=async_conf.get('max_concurrency', 40))
    parser.add_argument('--limit-symbols', type=int, default=0, help='測試用途：只跑前 N 檔，0 表示全部')
    args = parser.parse_args()

    timeframe = args.timeframe
    days = args.days

    downloader = CryptoDownloader(config=config)

    try:
        all_cryptos = await downloader.get_all_symbols_async()
        if args.limit_symbols and args.limit_symbols > 0:
            all_cryptos = all_cryptos[:args.limit_symbols]

        print(f"Total cryptos to process: {len(all_cryptos)}")
        print(f"Mode: {args.mode}, Max concurrency: {args.max_concurrency}")

        start = time.time()
        if args.mode == 'websocket':
            results = await run_websocket_mode(all_cryptos, timeframe, days, downloader, args.max_concurrency)
        else:
            results = await run_rest_mode(all_cryptos, timeframe, days, downloader, args.max_concurrency)
        elapsed = time.time() - start
        print(f"Total elapsed time: {elapsed:.2f}s")

        save_and_notify(results, all_cryptos, timeframe, days, config)
    finally:
        await downloader.aclose()


if __name__ == '__main__':
    asyncio.run(async_main())
