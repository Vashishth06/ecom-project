"""
Simple logging utility
"""
from datetime import datetime

def log_info(message):
    """Log informational message"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[INFO] {timestamp} - {message}")

def log_error(message):
    """Log error message"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[ERROR] {timestamp} - {message}")

def log_metric(metric_name, value):
    """Log metric with name and value"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # Convert value to string to handle any type
    print(f"[METRIC] {timestamp} - {metric_name}: {value}")  # ← No col() here