"""
Config settings for the trading data monitor
"""

class Config:
    DATA_SOURCES = {
        'symbols': ['AAPL', 'GOOGL', 'TSLA', 'SPY', 'QQQ'],
        'period': '5d',
        'interval': '1m',
        'timeout': '30'
    }

    QUALITY_THRESHOLD = {
        'max_missing_pct' = 5.0
    }
