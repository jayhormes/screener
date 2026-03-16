#!/usr/bin/env python3
"""
Debug parsing of similarity results
"""
import os

def debug_parsing():
    similarity_report_path = "similarity_output/20250825_180720/similarity_search_report.txt"
    
    with open(similarity_report_path, 'r', encoding='utf-8') as f:
        results_text = f.read()
    
    lines = results_text.strip().split('\n')
    
    print("🔍 DEBUGGING SIMILARITY RESULTS PARSING")
    print("=" * 60)
    
    current_timeframe = None
    current_ref = None
    match_count = 0
    
    for i, line in enumerate(lines):
        line = line.strip()
        
        if 'TIMEFRAME:' in line:
            # Extract timeframe from lines like "TIMEFRAME: 15m"
            timeframe = line.split('TIMEFRAME:')[1].strip()
            current_timeframe = timeframe
            print(f"\n📊 Found timeframe: {timeframe}")
            
        elif '--- ' in line and 'Reference #' in line:
            current_ref = line.replace('---', '').strip()
            print(f"  🔗 Reference: {current_ref}")
            
        elif line == 'Top Similarity Scores:':
            print(f"  📈 Found similarity scores section")
            
        elif ':' in line and 'Score=' in line and not line.startswith('---'):
            if current_timeframe and current_ref:
                symbol = line.split(':')[0].strip()
                print(f"    ✅ Match found: {symbol} (TF: {current_timeframe})")
                match_count += 1
            else:
                print(f"    ❌ Skipped (TF:{current_timeframe}, REF:{bool(current_ref)}): {line[:50]}...")
    
    print(f"\n📊 PARSING SUMMARY")
    print(f"Total matches found: {match_count}")

if __name__ == "__main__":
    debug_parsing()
