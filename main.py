from config import Config
from data_cleaning import DataCleaning
from data_ingestion import DataIngestion


def main():
    config = Config()
    
    data_ingestion = DataIngestion(config)

    symbols = config.DATA_SOURCES['symbols']
    raw_data = data_ingestion.fetch_market_data(symbols)
    
    data_cleaning = DataCleaning(config)
    cleaned_data = {}

    for symbol, data in raw_data.items():
        cleaned = data_cleaning.clean_price_data(data, symbol)
        cleaned_data[symbol] = cleaned

        data_ingestion.validate_data_completeness(data, symbol)
        data_ingestion.validate_price_consistency(data, symbol)

        # cleaning summary
        stats = data_cleaning.cleaning_stats[symbol]
        print(f"> Cleaned {symbol}: {len(stats['actions'])} actions")
        for action in stats['actions']:
            print(f"| {action}")
        

if __name__ == "__main__":
    main()