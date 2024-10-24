import yfinance as yf
import csv
from datetime import datetime
import os
import warnings
warnings.filterwarnings("ignore")

class YFinanceDataDownloader:
    def __init__(self, ticker, start_date, end_date=None, interval='1d'):
        self.ticker = ticker
        self.start_date = start_date
        self.end_date = end_date or datetime.today().strftime('%Y-%m-%d')  # Default to today's date if not provided
        self.interval = interval
    
    def _download_data(self):
        try:
            data = yf.download(self.ticker, start=self.start_date, end=self.end_date, interval=self.interval)
            return data
        except Exception as e:
            print(f"Error downloading data: {e}")
            return None
    
    def _save_to_csv(self, data, filename=None):
        folder = "Data"
        # Create the folder if it doesn't exist
        if not os.path.exists(folder):
            os.makedirs(folder)
            
        if filename is None:
            # Automatically generate the filename based on ticker, start date, and end date
            filename = f"{self.ticker}_daily.csv"
        
        filepath = os.path.join(folder, filename)
        try:
            data.to_csv(filepath)
            print(f"Data successfully written to {filepath}")
        except Exception as e:
            print(f"Error writing data to CSV: {e}")
            
    def run_download_and_save(self, filename=None):
        # Step 1: Download the data
        data = self._download_data()
        # Step 2: Save the data to a CSV file
        if data is not None:
            self._save_to_csv(data, filename)

if __name__ == "__main__":
    ticker = 'SPY'
    start_date = '2001-01-01'
    interval = "1d"
    
    downloader = YFinanceDataDownloader(ticker, start_date, interval=interval)
    # Use the new method to download and save data in one call
    downloader.run_download_and_save()
