"""
Config settings for the trading data monitor
"""

class Config:
    DATA_SOURCES = {
        'mode': 'local_csv', # yfinance or local_csv
        'symbols': ['AAPL'],
        'period': '5d',
        'interval': '1h',
        'timeout': '30',
        'local_paths': {
            'AAPL': './data/AAPL_1m.csv'
        }
    }

    PATHS = {
        'data_dir': './data/'
    }

    QUALITY_THRESHOLDS = {
        'max_missing_pct': 5.0,
        'max_price_changes_pct': 10.0,
        'min_volume': 100,
    }
