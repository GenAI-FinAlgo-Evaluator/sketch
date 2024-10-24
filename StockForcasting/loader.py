import pandas as pd
from io import StringIO
# from file_manager import FileManager
import warnings 
warnings.filterwarnings('ignore')

class StockDataLoader:
    def __init__(self, file_path):
        self.file_path = file_path
        self.data = None

    def LoadData(self):
        """
        Loads the stock data from a CSV file, checks for missing values, 
        validates the data, cleans it, converts the 'Date' column to datetime,
        and provides a summary.
        """
        # Step 1: Load Data
        try:
            self.data = pd.read_csv(self.file_path)
            print("✅ Data loaded successfully.")
        except FileNotFoundError:
            print(f"\n❌ Error: The file '{self.file_path}' does not exist.")
            return
        except pd.errors.EmptyDataError:
            print(f"\n❌ Error: The file '{self.file_path}' is empty.")
            return
        except pd.errors.ParserError:
            print(f"\n❌ Error: There was an issue parsing '{self.file_path}'. Please check the file format.")
            return
        except Exception as e:
            print(f"\n❌ Error loading file: {e}")
            return
        
        # Step 2: Convert 'Date' column to datetime
        try:
            self.data['Date'] = pd.to_datetime(self.data['Date'], errors='coerce')
            if self.data['Date'].isnull().any():
                print("\n Warning: Some 'Date' values couldn't be converted to datetime.")
        except KeyError:
            print("\n❌ Error: 'Date' column is missing.")
            return

        # Step 3: Check for Missing Values
        missing_values = self.data.isnull().sum()
        if missing_values.any():
            print("\n Missing values detected in the following columns:")
            print(missing_values[missing_values > 0])
        else:
            None

        # Step 4: Data Validation
        try:
            # Convert relevant columns to numeric
            columns_to_check = ['Open', 'Close', 'High', 'Low', 'Volume']
            for col in columns_to_check:
                if col in self.data.columns:
                    self.data[col] = pd.to_numeric(self.data[col], errors='coerce')
                else:
                    print(f"\n❌ Error: Expected column '{col}' is missing in the dataset.")
                    return

            # Check for negative values in price and volume
            negative_values = (self.data[columns_to_check] < 0).any()
            if negative_values.any():
                print("\n Warning: Negative values found in the dataset for the following columns:")
                print(negative_values[negative_values == True])
            else:
                None

            # Check for duplicates
            if self.data.duplicated().any():
                print("\n Warning: Duplicate rows detected. Cleaning duplicates.")
                self.data.drop_duplicates(inplace=True)
            else:
                None

        except KeyError as e:
            print(f"\n❌ Error: Missing expected columns in the data - {e}")
            return
        except Exception as e:
            print(f"\n❌ Error during validation: {e}")
            return

        # Step 5: Data Cleaning - Fill Missing Values with Moving Average
        initial_row_count = len(self.data)
        # Fill missing values with moving average 
        for col in ['Open', 'Close', 'High', 'Low', 'Volume']:
            if col in self.data.columns:
                self.data[col].fillna(self.data[col].rolling(window=5, min_periods=1).mean(), inplace=True)

        cleaned_row_count = len(self.data)
        removed_rows = initial_row_count - cleaned_row_count

        if removed_rows > 0:
            print(f"Warning: Data cleaning: {removed_rows} rows with missing values were removed.")
        else:
            None


        # Step 6: Data Summary
        print(f"✅ Data validation successful")

        return self.data  # Return the cleaned dataset for further use if necessary

def test_load_and_clean_stock_data():
    """
    Function to test load_and_clean_stock_data functionality, including datetime conversion.
    """
    # This CSV data simulates stock data with some missing values, duplicates, negative value, and a bad 'Volume' entry
    test_csv = StringIO(
        """Date,Open,High,Low,Close,Volume
        2024-10-01,100.0,110.0,90.0,105.0,10000
        2024-10-02,105.0,115.0,95.0,110.0,15000
        2024-10-03,110.0,,100.0,115.0,20000
        2024-10-04,115.0,125.0,105.0,120.0,25000
        2024-10-04,115.0,125.0,105.0,120.0,25000  # Duplicate row
        2024-10-05,-120.0,130.0,110.0,125.0,30000  # Negative value
        Invalid-Date,130.0,140.0,120.0,135.0,"Not a number"
        """
    )

    # Simulate reading from CSV by using StringIO as file_path
    loader = StockDataLoader(test_csv)

    # Call the LoadData method
    cleaned_data = loader.LoadData()

    # Validate the outcome
    assert cleaned_data is not None, "The cleaned data should not be None."
    assert cleaned_data['High'].isnull().sum() == 0, "Missing values should be removed."
    assert not cleaned_data.duplicated().any(), "Duplicate rows should be removed."
    assert (cleaned_data[['Open', 'Close', 'High', 'Low', 'Volume']] >= 0).all().all(), "Negative values should be handled."
    assert pd.api.types.is_numeric_dtype(cleaned_data['Volume']), "Volume column should contain numeric values."
    assert pd.api.types.is_datetime64_any_dtype(cleaned_data['Date']), "Date column should be converted to datetime."

    print("\n✅ All test cases passed!")

if __name__ == "__main__":
    # Example usage
    file_manager = FileManager()
    spy = file_manager.find_file('SPY_daily.csv')
    print(spy)
    loader = StockDataLoader(spy)
    cleaned_data = loader.LoadData()
    
    # Run the test function
    test_load_and_clean_stock_data()
