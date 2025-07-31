from config import Config
from data_ingestion import DataIngestion


def main():
    config = Config()
    
    data_ingestion = DataIngestion(config)

    symbols = config.DATA_SOURCES['symbols']
    raw_data = data_ingestion.fetch_market_data(symbols)

    for symbol, data in raw_data.items():
        print(symbol, data)

if __name__ == "__main__":
    main()