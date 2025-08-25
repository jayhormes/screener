"""
Message formatting utilities for Discord notifications
"""
from datetime import datetime
from typing import List, Dict, Optional


class CryptoMessageFormatter:
    """
    Utility class for formatting crypto screener messages
    """
    
    @staticmethod
    def format_crypto_results(targets: List[str], 
                            target_scores: Dict[str, float],
                            timeframe: str,
                            days: int,
                            total_processed: int,
                            failed_count: int,
                            timestamp: str = None,
                            max_targets: int = 20) -> str:
        """
        Format crypto screener results into Discord message
        
        Args:
            targets: List of crypto symbols sorted by score
            target_scores: Dictionary mapping symbols to their scores
            timeframe: Trading timeframe used
            days: Number of days analyzed
            total_processed: Total number of cryptos processed
            failed_count: Number of failed calculations
            timestamp: Optional timestamp string
            max_targets: Maximum number of targets to show (default 20)
            
        Returns:
            str: Formatted Discord message
        """
        # Use current time if timestamp not provided
        if timestamp is None:
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        
        # Create Discord message with results
        message = f"**🚀 Crypto Screener Results ({timeframe}, {days} days)**\n"
        message += f"**📊 TOP {max_targets} Strong Targets ({timestamp})**\n\n"
        
        # Add targets
        display_targets = targets[:max_targets]
        for idx, crypto in enumerate(display_targets, 1):
            score = target_scores[crypto]
            # Remove USDT suffix if present, keep other suffixes like USDC
            display_symbol = crypto
            if crypto.endswith('USDT'):
                display_symbol = crypto[:-4]  # Remove 'USDT'
            message += f"{idx}. {display_symbol}: {score:.6f}\n"
        
        # Add summary statistics
        message += f"\n📈 Total processed: {total_processed}"
        message += f"\n✅ Successfully calculated: {len(targets)}"
        message += f"\n❌ Failed: {failed_count}"
        
        return message
    
    @staticmethod
    def format_crypto_simple_list(targets: List[str], 
                                target_scores: Dict[str, float],
                                max_targets: int = 20) -> str:
        """
        Format crypto results as a simple numbered list
        
        Args:
            targets: List of crypto symbols sorted by score
            target_scores: Dictionary mapping symbols to their scores
            max_targets: Maximum number of targets to show
            
        Returns:
            str: Simple formatted list
        """
        message = f"**📊 TOP {max_targets} Crypto Targets**\n\n"
        
        display_targets = targets[:max_targets]
        for idx, crypto in enumerate(display_targets, 1):
            score = target_scores[crypto]
            # Remove USDT suffix if present, keep other suffixes like USDC
            display_symbol = crypto
            if crypto.endswith('USDT'):
                display_symbol = crypto[:-4]  # Remove 'USDT'
            message += f"{idx}. {display_symbol}: {score:.6f}\n"
        
        return message
    
    @staticmethod
    def format_crypto_summary(total_processed: int,
                            successful_count: int,
                            failed_count: int,
                            timeframe: str,
                            days: int) -> str:
        """
        Format crypto screening summary
        
        Args:
            total_processed: Total number of cryptos processed
            successful_count: Number of successful calculations
            failed_count: Number of failed calculations
            timeframe: Trading timeframe used
            days: Number of days analyzed
            
        Returns:
            str: Formatted summary message
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        message = f"**📊 Crypto Screening Summary**\n"
        message += f"**Time:** {timestamp}\n"
        message += f"**Timeframe:** {timeframe} | **Days:** {days}\n\n"
        message += f"📈 Total processed: {total_processed}\n"
        message += f"✅ Successfully calculated: {successful_count}\n"
        message += f"❌ Failed: {failed_count}\n"
        
        if total_processed > 0:
            success_rate = (successful_count / total_processed) * 100
            message += f"📊 Success rate: {success_rate:.1f}%"
        
        return message
    
    @staticmethod
    def format_file_message(timeframe: str, 
                          days: int, 
                          file_type: str = "Results File") -> str:
        """
        Format file attachment message
        
        Args:
            timeframe: Trading timeframe used
            days: Number of days analyzed
            file_type: Type of file being sent
            
        Returns:
            str: Formatted file message
        """
        return f"📎 **Crypto Screener {file_type}**\nTimeframe: {timeframe} | Days: {days}"


class GeneralMessageFormatter:
    """
    General purpose message formatter for various notifications
    """
    
    @staticmethod
    def format_error(error_message: str, script_name: str = "script") -> str:
        """
        Format error notification message
        
        Args:
            error_message: Error message
            script_name: Name of the script that encountered the error
            
        Returns:
            str: Formatted error message
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        message = f"🚨 **Error in {script_name}**\n"
        message += f"**Time:** {timestamp}\n"
        message += f"**Error:** {error_message}"
        return message
    
    @staticmethod
    def format_status(status: str, details: str = "") -> str:
        """
        Format status update message
        
        Args:
            status: Status message
            details: Optional additional details
            
        Returns:
            str: Formatted status message
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        message = f"ℹ️ **Status Update**\n"
        message += f"**Time:** {timestamp}\n"
        message += f"**Status:** {status}"
        
        if details:
            message += f"\n**Details:** {details}"
        
        return message
    
    @staticmethod
    def format_alert(title: str, content: str, alert_type: str = "info") -> str:
        """
        Format alert message with different types
        
        Args:
            title: Alert title
            content: Alert content
            alert_type: Type of alert (info, warning, success, error)
            
        Returns:
            str: Formatted alert message
        """
        icons = {
            "info": "ℹ️",
            "warning": "⚠️", 
            "success": "✅",
            "error": "🚨"
        }
        
        icon = icons.get(alert_type, "ℹ️")
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        message = f"{icon} **{title}**\n"
        message += f"**Time:** {timestamp}\n"
        message += f"{content}"
        
        return message
