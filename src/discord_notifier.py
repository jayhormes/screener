"""
Discord notification module for sending crypto screener results and other notifications
"""
import json
import requests
import os
from datetime import datetime
from typing import List, Dict, Optional, Tuple


class DiscordNotifier:
    """
    Discord notification class for sending messages via webhook
    """
    
    def __init__(self, config_path: str = "config.json", use_trend_finder: bool = False):
        """
        Initialize Discord notifier with config
        
        Args:
            config_path: Path to config.json file
            use_trend_finder: Whether to use trend finder Discord config instead of regular config
        """
        self.config_path = config_path
        self.use_trend_finder = use_trend_finder
        self.webhook_url = ""
        self.enabled = False
        self.send_detailed_results = True
        self.send_overall_summary = True
        self.max_results_per_message = 10
        self.delete_files_after_upload = True
        # Chart-specific settings for trend finder
        self.send_charts = False
        self.chart_min_score = 0.80
        self.max_charts_per_timeframe = 3
        self.chart_image_quality = "medium"
        self._load_config()
    
    def _load_config(self):
        """Load Discord configuration from config.json"""
        try:
            if os.path.exists(self.config_path):
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    
                    if self.use_trend_finder:
                        # Use trend finder specific config
                        discord_config = config.get('discord_trend_finder', {})
                        self.send_detailed_results = discord_config.get('send_detailed_results', True)
                        self.send_overall_summary = discord_config.get('send_overall_summary', True)
                        self.max_results_per_message = discord_config.get('max_results_per_message', 10)
                        self.delete_files_after_upload = discord_config.get('delete_files_after_upload', True)
                        # Chart settings
                        self.send_charts = discord_config.get('send_charts', False)
                        self.chart_min_score = discord_config.get('chart_min_score', 0.80)
                        self.max_charts_per_timeframe = discord_config.get('max_charts_per_timeframe', 3)
                        self.chart_image_quality = discord_config.get('chart_image_quality', 'medium')
                    else:
                        # Use regular config
                        discord_config = config.get('discord', {})
                    
                    self.webhook_url = discord_config.get('webhook_url', '')
                    self.enabled = discord_config.get('enabled', False)
            else:
                print(f"Config file {self.config_path} not found")
        except Exception as e:
            print(f"Error loading config: {e}")
    
    def send_message(self, message: str) -> bool:
        """
        Send a message to Discord webhook
        
        Args:
            message: Message content to send
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.enabled:
            print("Discord notifications are disabled in config")
            return False
            
        if not self.webhook_url:
            print("Discord webhook URL not configured")
            return False
            
        try:
            payload = {
                "content": message
            }
            response = requests.post(self.webhook_url, json=payload)
            return response.status_code == 204
        except Exception as e:
            print(f"Error sending Discord webhook: {e}")
            return False
    
    def send_file(self, file_path: str, message: str = "") -> bool:
        """
        Send a file to Discord webhook
        
        Args:
            file_path: Path to the file to upload
            message: Optional message to send with the file
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.enabled:
            print("Discord notifications are disabled in config")
            return False
            
        if not self.webhook_url:
            print("Discord webhook URL not configured")
            return False
            
        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            return False
            
        try:
            # Prepare the data
            data = {}
            if message:
                data['content'] = message
            
            # Prepare the file
            with open(file_path, 'rb') as f:
                files = {
                    'file': (os.path.basename(file_path), f, 'text/plain')
                }
                
                # Send the file
                response = requests.post(self.webhook_url, data=data, files=files)
                return response.status_code == 200
                
        except Exception as e:
            print(f"Error sending file to Discord: {e}")
            return False
    
    def send_crypto_results(self, message: str) -> bool:
        """
        Send crypto screener results message to Discord
        
        Args:
            message: Formatted message content to send
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.enabled:
            return False
            
        if not message:
            print("No message content to send")
            return False
        
        return self.send_message(message)
    
    def send_crypto_results_with_file(self, 
                                    message: str,
                                    file_path: str = None,
                                    file_message: str = "") -> Tuple[bool, bool]:
        """
        Send crypto screener results to Discord with optional file attachment
        
        Args:
            message: Formatted message content to send
            file_path: Optional path to file to upload
            file_message: Optional message to send with the file
            
        Returns:
            Tuple[bool, bool]: (message_success, file_success)
        """
        if not self.enabled:
            return False, False
        
        # Send the text message first
        message_success = self.send_crypto_results(message)
        
        # Send the file if provided
        file_success = True  # Default to True if no file
        if file_path and os.path.exists(file_path):
            file_success = self.send_file(file_path, file_message)
        
        return message_success, file_success
    
    def send_trend_finder_results(self, 
                                 overall_summary: str,
                                 timeframe_results: Dict = None,
                                 summary_file_path: str = None,
                                 visualization_paths: Dict = None) -> bool:
        """
        Send trend finder results to Discord
        
        Args:
            overall_summary: Overall summary text
            timeframe_results: Dictionary of timeframe results
            summary_file_path: Path to summary file
            visualization_paths: Dictionary of visualization file paths by timeframe and symbol
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.enabled:
            print("Discord trend finder notifications are disabled in config")
            return False
            
        if not self.webhook_url:
            print("Discord trend finder webhook URL not configured")
            return False
        
        success = True
        
        # Send overall summary if enabled
        if self.send_overall_summary and overall_summary:
            # Split summary into chunks if too long (Discord limit is 2000 characters)
            summary_chunks = self._split_message(overall_summary, 1800)
            
            for i, chunk in enumerate(summary_chunks):
                if i == 0:
                    message = "🔍 **Crypto Trend Finder Results**\n\n" + chunk
                else:
                    message = f"📄 **Report (Part {i+1}/{len(summary_chunks)})**\n\n" + chunk
                
                if not self.send_message(message):
                    success = False
        
        # Send detailed results if enabled
        if self.send_detailed_results and timeframe_results:
            for timeframe, results in timeframe_results.items():
                if not results:
                    continue
                    
                timeframe_message = f"📊 **{timeframe} Timeframe Results**\n\n"
                
                for reference_key, result_data in results.items():
                    reference_symbol, reference_timeframe, reference_label = reference_key
                    top_results = result_data.get("top_results", [])
                    statistics = result_data.get("statistics", {})
                    
                    if not top_results:
                        continue
                    
                    # Create message for this reference
                    ref_message = f"**Reference: {reference_symbol} ({reference_timeframe}, {reference_label})**\n"
                    
                    # Add statistics summary
                    if statistics:
                        ref_message += self._format_statistics_summary(statistics)
                    
                    # Add top results
                    ref_message += f"\n**Top {min(len(top_results), self.max_results_per_message)} Matches:**\n"
                    
                    for i, result in enumerate(top_results[:self.max_results_per_message]):
                        symbol = result["symbol"]
                        score = result["similarity"]
                        ref_message += f"{i+1}. **{symbol}** - Score: {score:.4f}\n"
                    
                    # Send this reference's results
                    full_message = timeframe_message + ref_message
                    if len(full_message) > 1800:
                        # Split into multiple messages if too long
                        chunks = self._split_message(full_message, 1800)
                        for chunk in chunks:
                            if not self.send_message(chunk):
                                success = False
                    else:
                        if not self.send_message(full_message):
                            success = False
                    
                    # Send high-score charts if enabled
                    if self.send_charts and visualization_paths:
                        self._send_high_score_charts(timeframe, reference_key, top_results, visualization_paths)
        
        # Send summary file if provided
        if summary_file_path and os.path.exists(summary_file_path):
            file_message = "📁 **Complete Analysis Report**"
            if not self.send_file(summary_file_path, file_message):
                success = False
        
        return success
    
    def _send_high_score_charts(self, timeframe: str, reference_key: tuple, 
                               top_results: list, visualization_paths: Dict):
        """
        Send charts for high-scoring matches
        
        Args:
            timeframe: Current timeframe
            reference_key: Reference trend key
            top_results: List of top results
            visualization_paths: Dictionary of visualization paths
        """
        if not self.send_charts:
            return
            
        charts_sent = 0
        reference_symbol, reference_timeframe, reference_label = reference_key
        
        print(f"📊 Sending charts for {timeframe} timeframe (min score: {self.chart_min_score})...")
        
        for result in top_results:
            if charts_sent >= self.max_charts_per_timeframe:
                break
                
            symbol = result["symbol"]
            score = result["similarity"]
            
            # Only send charts for high-scoring matches
            if score >= self.chart_min_score:
                # Look for chart file
                chart_path = self._find_chart_file(timeframe, symbol, score, visualization_paths)
                
                if chart_path and os.path.exists(chart_path):
                    chart_message = (f"📈 **Chart: {symbol}** (Score: {score:.4f})\n"
                                   f"Reference: {reference_symbol} ({reference_timeframe}, {reference_label})")
                    
                    if self.send_file(chart_path, chart_message):
                        charts_sent += 1
                        print(f"✅ Sent chart for {symbol} (score: {score:.4f})")
                    else:
                        print(f"❌ Failed to send chart for {symbol}")
                else:
                    print(f"⚠️  Chart file not found for {symbol}")
        
        if charts_sent > 0:
            print(f"📊 Sent {charts_sent} charts for {timeframe} timeframe")
        else:
            print(f"📊 No charts sent for {timeframe} timeframe (no matches above {self.chart_min_score})")
    
    def _find_chart_file(self, timeframe: str, symbol: str, score: float, 
                        visualization_paths: Dict) -> str:
        """
        Find the chart file for a given symbol and score
        
        Args:
            timeframe: Timeframe
            symbol: Symbol name
            score: Similarity score
            visualization_paths: Dictionary of visualization paths
            
        Returns:
            str: Path to chart file if found, None otherwise
        """
        if not visualization_paths:
            return None
            
        # Look for visualization paths for this timeframe
        timeframe_paths = visualization_paths.get(timeframe, {})
        
        # Look for exact symbol match
        if symbol in timeframe_paths:
            return timeframe_paths[symbol]
        
        # Look for files with score in the name
        for path_symbol, path in timeframe_paths.items():
            if symbol in path_symbol and f"{score:.4f}" in path:
                return path
        
        return None
    
    def _split_message(self, message: str, max_length: int = 1800) -> List[str]:
        """
        Split a long message into chunks that fit Discord's character limit
        
        Args:
            message: Message to split
            max_length: Maximum length per chunk
            
        Returns:
            List[str]: List of message chunks
        """
        if len(message) <= max_length:
            return [message]
        
        chunks = []
        lines = message.split('\n')
        current_chunk = ""
        
        for line in lines:
            if len(current_chunk) + len(line) + 1 <= max_length:
                current_chunk += line + '\n' if current_chunk else line
            else:
                if current_chunk:
                    chunks.append(current_chunk.rstrip())
                    current_chunk = line
                else:
                    # Line itself is too long, split it
                    while len(line) > max_length:
                        chunks.append(line[:max_length])
                        line = line[max_length:]
                    current_chunk = line
        
        if current_chunk:
            chunks.append(current_chunk.rstrip())
        
        return chunks
    
    def _format_statistics_summary(self, statistics: Dict) -> str:
        """
        Format trend statistics for Discord message
        
        Args:
            statistics: Statistics dictionary
            
        Returns:
            str: Formatted statistics summary
        """
        if not statistics:
            return ""
        
        summary = "📈 **Statistics:**\n"
        
        # Get key statistics
        total_patterns = statistics.get('total_patterns', 0)
        summary += f"• Total Patterns: {total_patterns}\n"
        
        # Future trend analysis
        future_trends = statistics.get('future_trend_analysis', {})
        if future_trends:
            for factor, analysis in future_trends.items():
                rise_pct = analysis.get('rise_percentage', 0)
                fall_pct = analysis.get('fall_percentage', 0)
                summary += f"• {factor}x Future: {rise_pct:.1f}% Rise, {fall_pct:.1f}% Fall\n"
        
        return summary
    
    def test_connection(self) -> bool:
        """
        Test Discord webhook connection
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        if not self.enabled:
            print("Discord notifications are disabled")
            return False
            
        if not self.webhook_url:
            print("Discord webhook URL not configured")
            return False
        
        test_message = "🔔 Discord webhook test - Connection successful!"
        success = self.send_message(test_message)
        
        if success:
            print("✅ Discord webhook test successful!")
        else:
            print("❌ Discord webhook test failed!")
            
        return success


def get_discord_notifier(config_path: str = "config.json") -> DiscordNotifier:
    """
    Factory function to get a Discord notifier instance
    
    Args:
        config_path: Path to config.json file
        
    Returns:
        DiscordNotifier: Configured Discord notifier instance
    """
    return DiscordNotifier(config_path)


def get_trend_finder_discord_notifier(config_path: str = "config.json") -> DiscordNotifier:
    """
    Factory function to get a Discord notifier instance for trend finder
    
    Args:
        config_path: Path to config.json file
        
    Returns:
        DiscordNotifier: Configured Discord notifier instance for trend finder
    """
    return DiscordNotifier(config_path, use_trend_finder=True)
