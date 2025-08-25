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


class TrendFinderMessageFormatter:
    """
    Utility class for formatting trend finder messages for Discord
    """
    
    @staticmethod
    def format_overall_summary(overall_summary: str, runtime: float = None) -> str:
        """
        Format overall trend finder summary for Discord
        
        Args:
            overall_summary: Raw overall summary text
            runtime: Optional runtime in seconds
            
        Returns:
            str: Formatted Discord message
        """
        # Extract key information from summary
        lines = overall_summary.split('\n')
        
        message = "🔍 **Crypto Trend Finder - Analysis Complete**\n\n"
        
        # Add timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        message += f"**📅 Analysis Time:** {timestamp}\n"
        
        if runtime:
            message += f"**⏱️ Runtime:** {runtime:.1f}s ({runtime/60:.1f}min)\n"
        
        # Extract configuration info
        config_lines = []
        stats_started = False
        
        for line in lines:
            if line.startswith("Past Extension Factor:"):
                config_lines.append(line)
            elif line.startswith("Future Extension Factor:"):
                config_lines.append(line)
            elif line.startswith("Overlap Filtering:"):
                config_lines.append(line)
            elif "OVERALL STATISTICS" in line:
                stats_started = True
                break
        
        if config_lines:
            message += "\n**⚙️ Configuration:**\n"
            for line in config_lines:
                message += f"• {line}\n"
        
        # Add brief statistics summary
        if stats_started:
            message += "\n**📊 Analysis Results:**\n"
            message += "• Pattern matching completed across multiple timeframes\n"
            message += "• Statistical analysis generated\n"
            message += "• Detailed results available in full report\n"
        
        return message
    
    @staticmethod
    def format_timeframe_results(timeframe: str, results: dict, max_results: int = 5) -> str:
        """
        Format timeframe results for Discord
        
        Args:
            timeframe: Timeframe string (e.g., "15m", "1h")
            results: Results dictionary for the timeframe
            max_results: Maximum number of results to show per reference
            
        Returns:
            str: Formatted Discord message
        """
        message = f"📊 **{timeframe} Timeframe Analysis**\n\n"
        
        if not results:
            message += "No results found for this timeframe."
            return message
        
        for reference_key, result_data in results.items():
            reference_symbol, reference_timeframe, reference_label = reference_key
            top_results = result_data.get("top_results", [])
            statistics = result_data.get("statistics", {})
            
            message += f"**🎯 Reference: {reference_symbol} ({reference_timeframe}, {reference_label})**\n"
            
            # Add statistics
            if statistics:
                total_patterns = statistics.get('total_patterns', 0)
                message += f"• **Total Patterns:** {total_patterns}\n"
                
                # Future trend analysis summary
                future_trends = statistics.get('future_trend_analysis', {})
                if future_trends:
                    # Get the most relevant extension factor (usually 1.0x)
                    key_factor = "1.0x" if "1.0x" in future_trends else list(future_trends.keys())[0] if future_trends else None
                    if key_factor:
                        analysis = future_trends[key_factor]
                        rise_pct = analysis.get('rise_percentage', 0)
                        fall_pct = analysis.get('fall_percentage', 0)
                        message += f"• **Future Trends ({key_factor}):** {rise_pct:.1f}% Rise, {fall_pct:.1f}% Fall\n"
            
            # Add top matches
            if top_results:
                message += f"\n**🏆 Top {min(len(top_results), max_results)} Matches:**\n"
                for i, result in enumerate(top_results[:max_results]):
                    symbol = result["symbol"]
                    score = result["similarity"]
                    # Remove USDT suffix for cleaner display
                    display_symbol = symbol[:-4] if symbol.endswith('USDT') else symbol
                    message += f"{i+1}. **{display_symbol}** - Score: {score:.4f}\n"
            else:
                message += "\n• No matching patterns found\n"
            
            message += "\n"
        
        return message
    
    @staticmethod
    def format_reference_summary(reference_symbol: str, 
                                reference_timeframe: str, 
                                reference_label: str,
                                statistics: dict,
                                top_results: list,
                                max_results: int = 10) -> str:
        """
        Format reference trend summary for Discord
        
        Args:
            reference_symbol: Reference symbol
            reference_timeframe: Reference timeframe
            reference_label: Reference label
            statistics: Statistics dictionary
            top_results: List of top results
            max_results: Maximum results to show
            
        Returns:
            str: Formatted Discord message
        """
        message = f"🎯 **Reference Analysis: {reference_symbol}**\n"
        message += f"**📈 Pattern:** {reference_timeframe}, {reference_label}\n\n"
        
        # Statistics summary
        if statistics:
            total = statistics.get('total_patterns', 0)
            message += f"**📊 Found {total} Similar Patterns**\n\n"
            
            # Future trend analysis
            future_trends = statistics.get('future_trend_analysis', {})
            if future_trends:
                message += "**🔮 Future Trend Predictions:**\n"
                for factor, analysis in list(future_trends.items())[:3]:  # Show top 3 factors
                    rise_pct = analysis.get('rise_percentage', 0)
                    fall_pct = analysis.get('fall_percentage', 0)
                    total_analyzed = analysis.get('total_analyzed', 0)
                    
                    if total_analyzed > 0:
                        message += f"• **{factor} Extension:** {rise_pct:.1f}% Rise, {fall_pct:.1f}% Fall ({total_analyzed} patterns)\n"
        
        # Top matches
        if top_results:
            message += f"\n**🏆 Top {min(len(top_results), max_results)} Matches:**\n"
            for i, result in enumerate(top_results[:max_results]):
                symbol = result["symbol"]
                score = result["similarity"]
                display_symbol = symbol[:-4] if symbol.endswith('USDT') else symbol
                message += f"{i+1}. **{display_symbol}** - {score:.4f}\n"
        
        return message
    
    @staticmethod
    def format_quick_summary(all_results: dict) -> str:
        """
        Format a quick summary of all results for Discord
        
        Args:
            all_results: Dictionary of all results by timeframe
            
        Returns:
            str: Formatted quick summary
        """
        message = "⚡ **Quick Summary - Trend Finder Results**\n\n"
        
        total_patterns = 0
        total_references = 0
        timeframes_processed = 0
        
        for timeframe, timeframe_results in all_results.items():
            if timeframe_results:
                timeframes_processed += 1
                message += f"**{timeframe}:** "
                
                timeframe_patterns = 0
                references_in_timeframe = 0
                
                for reference_key, result_data in timeframe_results.items():
                    references_in_timeframe += 1
                    top_results = result_data.get("top_results", [])
                    timeframe_patterns += len(top_results)
                
                message += f"{timeframe_patterns} patterns from {references_in_timeframe} references\n"
                total_patterns += timeframe_patterns
                total_references += references_in_timeframe
        
        message += f"\n**📊 Total:** {total_patterns} patterns across {timeframes_processed} timeframes\n"
        message += f"**🎯 References:** {total_references // timeframes_processed if timeframes_processed > 0 else 0} reference trends analyzed\n"
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        message += f"**⏰ Completed:** {timestamp}"
        
        return message


class TrendSimilarityMessageFormatter:
    """
    Utility class for formatting trend similarity analysis messages for Discord
    """
    
    @staticmethod
    def format_similarity_results_summary(results_text: str, 
                                         total_timeframes: int = None,
                                         total_references: int = None) -> str:
        """
        Format trend similarity analysis results summary for Discord (simplified)
        """
        message = "🔍 **Crypto Trend Similarity Analysis - Complete**\n\n"
        
        # Add timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        message += f"📅 **Analysis Time:** {timestamp}\n\n"
        
        # Parse results to extract key information
        lines = results_text.strip().split('\n')
        timeframe_results = {}
        current_timeframe = None
        total_matches = 0
        unique_symbols = set()
        
        for line in lines:
            if 'TIMEFRAME:' in line:
                timeframe = line.split('TIMEFRAME:')[1].strip()
                current_timeframe = timeframe
                timeframe_results[timeframe] = []
            
            elif '--- ' in line and 'Reference #' in line and current_timeframe:
                ref_info = line.replace('---', '').strip()
                timeframe_results[current_timeframe].append({'ref': ref_info, 'matches': []})
            
            elif 'Top Similarity Scores:' in line and current_timeframe:
                continue
            
            elif ':' in line and 'Score=' in line and current_timeframe and not line.startswith('---'):
                if timeframe_results[current_timeframe]:
                    symbol = line.split(':')[0].strip()
                    if symbol and symbol != 'Top Similarity Scores':
                        timeframe_results[current_timeframe][-1]['matches'].append(line.strip())
                        total_matches += 1
                        unique_symbols.add(symbol)
        
        # Format summary by timeframe (simplified)
        for timeframe, refs in timeframe_results.items():
            timeframe_matches = sum(len(ref['matches']) for ref in refs)
            if timeframe_matches > 0:
                message += f"⏰ **{timeframe}:** {timeframe_matches} matches\n"
        
        message += f"\n🎯 **Total:** {total_matches} matches, {len(unique_symbols)} symbols\n"
        
        # Add top symbols preview
        if unique_symbols:
            top_symbols = sorted(list(unique_symbols))[:5]
            symbols_preview = ', '.join(top_symbols)
            if len(unique_symbols) > 5:
                symbols_preview += f" +{len(unique_symbols) - 5} more"
            message += f"💎 **Symbols:** {symbols_preview}"
        
        return message
    
    @staticmethod
    def format_top_matches_by_timeframe(results_text: str, max_per_timeframe: int = 5) -> str:
        """
        Format top matches by timeframe for Discord (simplified)
        """
        message = "🔥 **Top Matches by Timeframe**\n\n"
        
        # Parse results
        lines = results_text.strip().split('\n')
        timeframe_matches = {}
        current_timeframe = None
        current_ref = None
        
        for line in lines:
            if 'TIMEFRAME:' in line:
                timeframe = line.split('TIMEFRAME:')[1].strip()
                current_timeframe = timeframe
                timeframe_matches[timeframe] = []
            
            elif '--- ' in line and 'Reference #' in line and current_timeframe:
                current_ref = line.replace('---', '').strip()
            
            elif ':' in line and 'Score=' in line and current_timeframe and current_ref and not line.startswith('---'):
                parts = line.split(':')
                if len(parts) >= 2:
                    symbol = parts[0].strip()
                    if symbol and symbol != 'Top Similarity Scores':
                        score_part = parts[1].split(',')[0].strip()
                        if 'Score=' in score_part:
                            score = float(score_part.split('Score=')[1])
                            timeframe_matches[current_timeframe].append({
                                'symbol': symbol,
                                'score': score,
                                'reference': current_ref,
                                'full_line': line.strip()
                            })
        
        # Sort and display top matches for each timeframe
        for timeframe, matches in timeframe_matches.items():
            if matches:
                sorted_matches = sorted(matches, key=lambda x: x['score'], reverse=True)
                top_matches = sorted_matches[:max_per_timeframe]
                
                message += f"⏰ **{timeframe}:**\n"
                for i, match in enumerate(top_matches, 1):
                    score = match['score']
                    symbol = match['symbol']
                    ref_name = match['reference'].split('(')[0].strip() if '(' in match['reference'] else match['reference']
                    message += f"  {i}. **{symbol}** ({score:.4f}) - {ref_name}\n"
                message += "\n"
        
        return message
