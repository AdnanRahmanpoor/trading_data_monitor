import numpy as np
import pandas as pd
from scipy import stats

from config import Config

class DataCleaning:
    def __init__(self, config: Config):
        self.config = config
        self.cleaning_stats = {}

    def detect_outliers_iqr(self, data: pd.Series, multiplier: float = 1.5) -> pd.Series:
        Q1 = data.quantile(0.25)
        Q3 = data.quantile(0.75)
        IQR = Q3 - Q1

        lower_bound = Q1 - multiplier * IQR
        upper_bound = Q3 - multiplier * IQR

        outliers = (data < lower_bound) | (data > upper_bound)

        print(f"Found {outliers.sum()} outliers in {data.name} (Bounds: {lower_bound:.2f}-{upper_bound:.2f})")

        return outliers
    
    # zscore
    def detect_outliers_zscore(self, data: pd.Series, threshold: float = 3.0) -> pd.Series:
        z_scores = np.abs(stats.zscore(data.dropna()))

        outliers = pd.Series(False, index=data.index)
        outliers.loc[data.dropna().index] = z_scores > threshold

        return outliers
    
    def clean_price_data(self, data: pd.DataFrame, symbol: str) -> pd.DataFrame:
        cleaned_data = data.copy()
        cleaning_actions = []

        price_cols = ['Open', 'High', 'Low', 'Close']

        for col in price_cols:
            if col in cleaned_data.columns:
                missing_before = cleaned_data[col].isnull().sum()
                
                if missing_before > 0:
                    # Forward Fill
                    cleaned_data[col] = cleaned_data[col].ffill(inplace=True)
                    cleaned_data[col] = cleaned_data[col].bfill(inplace=True)

                    missing_after = cleaned_data[col].isnull().sum()
                    cleaning_actions.append(f"Filled {missing_before - missing_after} missing values in {col} ")

        # volume data
        if 'Volume' in cleaned_data.columns:
            volume_missing = cleaned_data['Volume'].isnull().sum()

            if volume_missing > 0:
                median_volume = cleaned_data['Volume'].median()
                cleaned_data['Volume'] = cleaned_data['Volume'].fillna(median_volume)
                cleaning_actions.append(f"Filled {volume_missing} missing volume values with media")

        # Outliers
        for col in price_cols:
            if col in cleaned_data.columns and len(cleaned_data[col].dropna() > 10):
                iqr_outliers = self.detect_outliers_iqr(cleaned_data[col])
                zscore_outliers = self.detect_outliers_zscore(cleaned_data[col])

                outliers = iqr_outliers & zscore_outliers                
                outliers_count = outliers.sum()

                if outliers_count > 0:
                    print(f"{symbol}: Found {outliers_count} outliers in {col}")

                    cleaned_data.loc[outliers, col] = np.nan
                    cleaned_data[col] = cleaned_data[col].interpolate(method='linear')

                    cleaning_actions.append(f"Replaced {outliers_count} outliers in {col}")
                
                
        # stats
        self.cleaning_stats[symbol] = {
            'actions': cleaning_actions,
            'original_rows': len(data),
            'cleaned_rows': len(cleaned_data)
        }

        return cleaned_data