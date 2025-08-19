#!/usr/bin/env python3
"""
Performance monitoring script for CNPJ Scraper
"""

import os
import time
import json
from datetime import datetime, timedelta

def analyze_logs():
    """Analyze scraper logs for performance metrics"""
    log_file = "logs.txt"
    
    if not os.path.exists(log_file):
        print("❌ No log file found. Run the scraper first to generate logs.")
        return
    
    print("📊 CNPJ Scraper Performance Analysis")
    print("=" * 50)
    
    # Read log file
    with open(log_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    # Parse logs
    start_time = None
    end_time = None
    total_cnpjs = 0
    web_scraping_count = 0
    api_calls = 0
    errors = 0
    
    for line in lines:
        if "Starting Optimized CNPJ Scraper" in line:
            start_time = line.split(' - ')[0]
        elif "Scraping completed successfully" in line:
            end_time = line.split(' - ')[0]
        elif "Loaded" in line and "valid CNPJs" in line:
            # Extract number of CNPJs
            try:
                total_cnpjs = int(line.split('Loaded ')[1].split(' valid')[0])
            except:
                pass
        elif "Running web scraping" in line:
            web_scraping_count += 1
        elif "Found" in line and ("phone via web scraping" in line or "email via web scraping" in line):
            api_calls += 1
        elif "ERROR" in line:
            errors += 1
    
    # Calculate metrics
    if start_time and end_time:
        try:
            start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
            duration = end_dt - start_dt
            duration_seconds = duration.total_seconds()
            
            print(f"⏱️  Duration: {duration}")
            print(f"📈 CNPJs processed: {total_cnpjs}")
            print(f"🌐 Web scraping attempts: {web_scraping_count}")
            print(f"📞 Contact data found: {api_calls}")
            print(f"❌ Errors: {errors}")
            
            if duration_seconds > 0 and total_cnpjs > 0:
                rate = total_cnpjs / duration_seconds
                print(f"⚡ Processing rate: {rate:.2f} CNPJs/second")
                
                if web_scraping_count > 0:
                    web_scraping_rate = web_scraping_count / total_cnpjs * 100
                    print(f"🔍 Web scraping rate: {web_scraping_rate:.1f}% of CNPJs")
                
                if api_calls > 0:
                    success_rate = api_calls / web_scraping_count * 100 if web_scraping_count > 0 else 0
                    print(f"✅ Web scraping success rate: {success_rate:.1f}%")
            
        except Exception as e:
            print(f"❌ Error parsing timestamps: {e}")
    else:
        print("❌ Could not determine start/end times from logs")
    
    print("\n📋 Recent Activity:")
    # Show last 10 log entries
    recent_lines = lines[-10:] if len(lines) >= 10 else lines
    for line in recent_lines:
        timestamp = line.split(' - ')[0] if ' - ' in line else ''
        message = line.split(' - ', 2)[-1].strip() if ' - ' in line else line.strip()
        print(f"  {timestamp}: {message}")

def check_files():
    """Check status of output files"""
    print("\n📁 File Status:")
    print("=" * 30)
    
    files = [
        ("input.txt", "Input CNPJs"),
        ("result.txt", "Results"),
        ("done.txt", "Completed CNPJs"),
        ("errors.txt", "Errors"),
        ("logs.txt", "Log file")
    ]
    
    for filename, description in files:
        if os.path.exists(filename):
            size = os.path.getsize(filename)
            if size > 0:
                print(f"✅ {description}: {filename} ({size} bytes)")
            else:
                print(f"⚠️  {description}: {filename} (empty)")
        else:
            print(f"❌ {description}: {filename} (not found)")

if __name__ == "__main__":
    analyze_logs()
    check_files() 