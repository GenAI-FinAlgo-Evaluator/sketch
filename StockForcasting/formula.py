import talib
import pandas as pd
import loader

class Formula():
    def __init__(self, data):
        self.data = data
    
    
if __name__=="__main__":
    SPY = loader.StockDataLoader(file_path="SPY_daily.csv")
    data = SPY.LoadData()
    print(data)
    