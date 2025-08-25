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
    
    def __init__(self, config_path: str = "config.json"):
        """
        Initialize Discord notifier with config
        
        Args:
            config_path: Path to config.json file
        """
        self.config_path = config_path
        self.webhook_url = ""
        self.enabled = False
        self._load_config()
    
    def _load_config(self):
        """Load Discord configuration from config.json"""
        try:
            if os.path.exists(self.config_path):
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
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
