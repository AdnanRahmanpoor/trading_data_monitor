"""
Data ingestion module for fetching the data
"""

import numpy as np
import pandas as pd
import yfinance as yf

from datetime import datetime, timedelta
from typing import Dict, List


class DataIngestion:
    def __init__(self, config):
        self.config = config
        
    def fetch_market_data(self, symbols: List[str]) -> Dict[str, pd.DataFrame]:
        if self.config.DATA_SOURCES['mode'] == 'yfinance':
            return self._fetch_from_yfinance(symbols)
        elif self.config.DATA_SOURCES['mode'] == 'local_csv':
            return self._fetch_from_local(symbols)
        else:
            raise ValueError('Invalid data source mode')

    def _fetch_from_yfinance(self, symbols: List[str]) -> Dict[str, pd.DataFrame]:
        market_data = {}
        failed_symbols = []

        for symbol in symbols:
            try:
                ticker = yf.Ticker(symbol)
                data = ticker.history(
                    period = self.config.DATA_SOURCES['period'],
                    interval = self.config.DATA_SOURCES['interval'],
                    timeout = self.config.DATA_SOURCES['timeout']
                )
                    
                if data.empty:
                    failed_symbols.append(symbol)
                    continue

                market_data[symbol] = self._process_raw_data(data, symbol)

            except Exception as e:
                failed_symbols.append(symbol)
                print(f"Failed to fetch {symbol}: {str(e)}")

        return market_data
    
    def _fetch_from_local(self, symbols: List[str]) -> Dict[str, pd.DataFrame]:
        market_data = {}

        for symbol in symbols:
            try:
                path = self.config.DATA_SOURCES['local_paths'].get(symbol)
                if not path:
                    continue

                data = pd.read_csv(
                    path,
                    parse_dates=['Date'],
                    index_col='Date'
                )

                # making sure that the required cols exist
                required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']

                if not all(col in data.columns for col in required_cols):
                    raise ValueError(f"Missing required columns in {symbol} data")

                market_data[symbol] = self._process_raw_data(data, symbol)

            except Exception as e:
                print(f'Failed to load {symbol}: {str(e)}')

        return market_data

    def _process_raw_data(self, data: pd.DataFrame, symbol: str) -> pd.DataFrame:
        data['symbol'] = symbol
        data['Fetch_Time'] = datetime.now()
        data['Mid_Price'] = (data['High'] + data['Low']) / 2
        data['Price_Range'] = data['High'] - data['Low']
        data['Returns'] = data['Close'].pct_change()
        return data
    
    def validate_data_completeness(self, data: pd.DataFrame, symbol: str) -> Dict[str, float]:
        completeness_metrics = {}

        total_records = len(data)

        # Check missing values for each column
        for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
            if col in data.columns:
                missing_count = data[col].isnull().sum()
                missing_pct = (missing_count / total_records) * 100

                completeness_metrics[f'{col}_missing_pct'] = missing_pct

                if missing_pct > self.config.QUALITY_THRESHOLD['max_missing_pct']:
                    print(f'{symbol}: {col} has {missing_pct:.2f} % missing values ')
                    print(f'threshold: {self.config.QUALITY_THRESHOLD["max_missing_pct"]}%')

        # Check for gaps in time
        if len(data) > 1:
            time_diff = data.index.to_series().diff()
            expected_interval = pd.Timedelta(minutes=60) # change based on data interval in config.py
            gaps = time_diff > expected_interval * 2 # tolerance
            gap_count = gaps.sum()
            gap_pct = (gap_count / len(data)) * 100

            completeness_metrics['timestamp_gaps_pct'] = gap_pct

            if gap_count > 0:
                print(f'{symbol}: Found {gap_count} timestamp gaps')

        return completeness_metrics