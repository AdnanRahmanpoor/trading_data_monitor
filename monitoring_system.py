from datetime import datetime
from typing import Dict
import numpy as np
import pandas as pd
from config import Config


class AlertManager:
    pass

class DataQualityMonitor:
    def __init__(self, config: Config, alert_manager: AlertManager):
        self.config = config
        self.alert_manager = alert_manager
        
    def calculate_system_health_score(self, data_dict: Dict[str, pd.DataFrame]) -> float:
        if not data_dict:
            return 0.0
        
        symbol_scores = []

        for symbol, data in data_dict.items():
            health_factors = {}

            if not data.empty:
                latest_time = data.index.max()
                time_diff = datetime.now() - latest_time.to_pydatetime()
                freshness_hours = time_diff.total_seconds() / 3600
                health_factors['freshness'] = max(0, 100 - (freshness_hours * 100))
            else:
                health_factors['freshness'] = 0

            # data completeness
            total_possible_records = len(data)
            
            if total_possible_records > 0:
                complete_records = data.dropna().shape[0]
                health_factors['completeness'] = (complete_records / total_possible_records) * 100
            else:
                health_factors['completeness'] = 0

            # price
            if 'Close' in data.columns and len(data) > 1:
                returns = data['Close'].pct_change().dropna()

                if len(returns) > 0:
                    extreme_moves = (np.abs(returns) > 0.1).sum() # 10% move
                    consistency_score = max(0, 100 - (extreme_moves / len(returns)) * 100)
                    health_factors['consistency'] = consistency_score
                else:
                    health_factors['consistency'] = 100
            else:
                health_factors['consistency'] = 100

            symbol_score = (
                health_factors['freshness'] * 0.4 +
                health_factors['completeness'] * 0.4 +
                health_factors['consistency'] * 0.2
            )

            symbol_scores.append(symbol_score)
            
        return float(np.mean(symbol_scores)) if symbol_scores else 0.0
    
    def monitor_data_quality(self, data_dict: Dict[str, pd.DataFrame]):
        
        monitoring_results = {
            'timestamp': datetime.now(),
            'system_health_score': self.calculate_system_health_score(data_dict),
            'symbol_details': {}
        }

        for symbol, data in data_dict.items():
            symbol_metrics = {}

            if not data.empty:
                latest_time = data.index.max()
                time_diff = datetime.now() - latest_time.to_pydatetime()
                minutes_old = time_diff.total_seconds() / 60
                symbol_metrics['minutes_old'] = minutes_old

                if minutes_old > 10:
                    # send alert
                    print(f'Data is {minutes_old:.1f} minutes old')

            