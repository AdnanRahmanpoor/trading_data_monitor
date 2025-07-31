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

    QUALITY_THRESHOLD = {
        'max_missing_pct': 5.0,
    }
