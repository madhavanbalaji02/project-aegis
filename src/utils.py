"""
Project Aegis - Utility Functions
Detects execution environment and provides cross-platform helpers.
"""
import os
from typing import Literal

def get_execution_mode() -> Literal["cloud", "local"]:
    """
    Determines if the application is running on Render (Cloud) or Local environment.
    
    Returns:
        "cloud" if running on Render, "local" otherwise
    """
    # Check for Render-specific environment variables
    render_indicators = [
        "RENDER",
        "RENDER_SERVICE_ID",
        "RENDER_INSTANCE_ID",
        "IS_PULL_REQUEST"
    ]
    
    for indicator in render_indicators:
        if os.getenv(indicator):
            return "cloud"
    
    # Additional check for common cloud platform indicators
    if os.getenv("DYNO") or os.getenv("PORT"):  # Heroku or generic cloud
        return "cloud"
    
    return "local"


def format_bytes(bytes_size: int) -> str:
    """
    Format bytes into human-readable string.
    
    Args:
        bytes_size: Size in bytes
        
    Returns:
        Formatted string (e.g., "1.5 GB")
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.2f} PB"


def get_memory_limit() -> int:
    """
    Get memory limit based on execution mode.
    
    Returns:
        Memory limit in MB
    """
    mode = get_execution_mode()
    if mode == "cloud":
        return 512  # Render free tier limit
    else:
        return 8192  # Assume 8GB for local development
