'''
Classes for MACD Strategy
'''

import datetime
import pandas as pd
import pandas_ta as ta

import common as cm
from strategy import Strategy
from datamatrix import DataMatrix, DataMatrixLoader

class MACDStrategy(Strategy):

    ''' Simple Strategy based on MACD
    1. Entry rule: long when MACD crosses above the signal line, short when MACD crosses below the signal line
    2. Exit rule: Close in a week or earn a target gain percentage or sell at a max loss percentage
    3. Capital Allocation: based on a risk allocation percentage parameter.
    '''
    
    def __init__(self, pref, input_datamatrix: DataMatrix, initial_capital: float, price_choice = cm.DataField.close,
                target_gain_percentage = 1.0, max_loss_percentage = -1.0, risk_allocation_percentage = 10):
        super().__init__(pref, 'MACDStrategy', input_datamatrix, initial_capital, price_choice)
        self.target_gain_percentage = target_gain_percentage
        self.max_loss_percentage = max_loss_percentage
        self.risk_allocation_percentage = risk_allocation_percentage

    def validate(self):
        '''
        Validate if the input_dm has everything the strategy needs
        '''
        columns = self.input_dm.columns
        for ticker in self.universe:
            col = f"{ticker}_{self.price_choice}"
            if col not in columns:
                raise Exception(f"Cannot find {col} for {ticker}")

    def _calc_MACD(self):
        '''
        Calculate MACD and signal line
        '''
        for ticker in self.universe:
            price = self.input_dm[f"{ticker}_{cm.DataField.close}"]

            # Check if price data is sufficient
            if price.isnull().all() or len(price) < 26:
                raise Exception(f"Insufficient price data for {ticker} to calculate MACD.")

            macd = ta.macd(price)

            # Print the output for debugging
            # print(f"MACD Output for {ticker}:\n", macd)

            # Check for expected columns
            if 'MACD_12_26_9' in macd.columns and 'MACDs_12_26_9' in macd.columns:
                self.input_dm[f"{ticker}_MACD"] = macd['MACD_12_26_9']
                self.input_dm[f"{ticker}_MACD_Signal"] = macd['MACDs_12_26_9']
            else:
                raise Exception(f"MACD calculation for {ticker} does not have expected columns.")

    def run_model(self, model=None):
        '''
        Return a trade signal and its corresponding shares based on MACD strategy
        '''

        # Calculate MACD and its signal line
        self._calc_MACD()

        MACD = 'MACD'
        MACD_SIGNAL = 'MACD_Signal'

        nrow, ncol = self.pricing_matrix.shape
        taction = self.pricing_matrix.copy()
        tsignal = self.pricing_matrix.copy()
        shares = self.pricing_matrix.copy()
        current_shares_with_sign = self.pricing_matrix.copy()
        dollar_exposure = self.initial_capital * self.risk_allocation_percentage / 100

        taction = taction.map(lambda x: cm.TradeAction.NONE.value)
        tsignal *= 0
        shares *= 0
        current_shares_with_sign *= 0

        # Remember the price and the date index when a trade was put on by ticker
        entry_day_index = {}
        entry_price = {}

        for j in range(ncol):
            ticker = self.pricing_matrix.columns[j]
            macd = self.input_dm[f"{ticker}_MACD"]
            macd_signal = self.input_dm[f"{ticker}_MACD_Signal"]

            for i in range(1, nrow):
                current_price = self.pricing_matrix.iloc[i, j]
                current_shares_with_sign.iloc[i, j] = current_shares_with_sign.iloc[i - 1, j]

                if pd.isna(current_shares_with_sign.iloc[i - 1, j]) or current_price == 0:
                    continue

                # A position exists already, check if one can exit the current position
                if current_shares_with_sign.iloc[i - 1, j] != 0:
                    ret = 100 * (current_price - entry_price[ticker]) / entry_price[ticker]

                    # Reach target gain or exceed max loss, close position
                    if ret >= self.target_gain_percentage or ret < self.max_loss_percentage:
                        # If it was long, sell
                        if current_shares_with_sign.iloc[i - 1, j] > 0:
                            tsignal.iloc[i, j] = -1
                            taction.iloc[i, j] = cm.TradeAction.SELL.value
                            shares.iloc[i, j] = abs(current_shares_with_sign.iloc[i - 1, j])
                            current_shares_with_sign.iloc[i, j] = 0
                        # If it were short, buy back
                        elif current_shares_with_sign.iloc[i - 1, j] < 0:
                            tsignal.iloc[i, j] = 1
                            taction.iloc[i, j] = cm.TradeAction.BUY.value
                            shares.iloc[i, j] = abs(current_shares_with_sign.iloc[i - 1, j])
                            current_shares_with_sign.iloc[i, j] = 0

                # Check for entry signals (crossovers)
                elif macd.iloc[i] > macd_signal.iloc[i] and current_shares_with_sign.iloc[i - 1, j] == 0:
                    tsignal.iloc[i, j] = 1
                    taction.iloc[i, j] = cm.TradeAction.BUY.value
                    shares.iloc[i, j] = int(dollar_exposure / current_price)
                    current_shares_with_sign.iloc[i, j] = shares.iloc[i, j]
                    entry_day_index[ticker] = i
                    entry_price[ticker] = current_price

                elif macd.iloc[i] < macd_signal.iloc[i] and current_shares_with_sign.iloc[i - 1, j] == 0:
                    tsignal.iloc[i, j] = -1
                    taction.iloc[i, j] = cm.TradeAction.SELL.value
                    shares.iloc[i, j] = int(dollar_exposure / current_price)
                    current_shares_with_sign.iloc[i, j] = -1 * shares.iloc[i, j]
                    entry_day_index[ticker] = i
                    entry_price[ticker] = current_price

        return tsignal, taction, shares


def _test1():
    from  .preference import Preference

    pref = Preference()
    # Pick some random name
    # universe = ['AWO', 'BDJ']
    # universe = ["SPY"]
    universe = ["SPY","QQQ","IWM"]
    
    start_date = datetime.date(2013, 1, 1)
    end_date = datetime.date(2023, 1, 1)

    name = 'test'
    loader = DataMatrixLoader(pref, name, universe, start_date, end_date)
    dm = loader.get_daily_datamatrix()

    MACD = MACDStrategy(pref, dm, cm.OneMillion, target_gain_percentage=1.5, max_loss_percentage=-0.5)
    MACD.validate()
    tradesignal, tradeaction, shares = MACD.run_model()

    # Print or save your results
    MACD.run_strategy()
    print(MACD.performance)

    print(f"Saving output to {pref.test_output_dir}")
    MACD.save_to_csv(pref.test_output_dir)


def _test():
    _test1()


if __name__ == "__main__":
    _test()
