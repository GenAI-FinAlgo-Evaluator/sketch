import datetime
import pandas as pd
import pandas_ta as ta

import common as cm
from strategy import Strategy
from datamatrix import DataMatrix, DataMatrixLoader

class ADXStrategy(Strategy):
    '''
    Strategy based on ADX (Average Directional Index)
    1. Entry rule: Buy when ADX > threshold and +DI > -DI (uptrend). Sell when ADX > threshold and -DI > +DI (downtrend).
    2. Exit rule: Close in a week or reach target gain percentage or max loss percentage.
    3. Capital Allocation: based on a risk allocation percentage parameter.
    '''
    def __init__(self, pref, input_datamatrix: DataMatrix, initial_capital: float, price_choice=cm.DataField.close,
                 adx_threshold=25, target_gain_percentage=1.0, max_loss_percentage=-1.0, risk_allocation_percentage=10):
        super().__init__(pref, 'ADXStrategy', input_datamatrix, initial_capital, price_choice)
        self.adx_threshold = adx_threshold
        self.target_gain_percentage = target_gain_percentage
        self.max_loss_percentage = max_loss_percentage
        self.risk_allocation_percentage = risk_allocation_percentage

    def validate(self):
        '''
        validate if the input_dm has everything the strategy needs
        '''
        columns = self.input_dm.columns
        for ticker in self.universe:
            col = f"{ticker}_{self.price_choice}"
            if col not in columns:
                raise Exception(f"Cannot find {col} for {ticker}")

    def _calc_ADX(self):
        '''
        Calculate ADX, +DI, and -DI for each ticker
        '''
        for ticker in self.universe:
            high = self.input_dm[f"{ticker}_{cm.DataField.high}"]
            low = self.input_dm[f"{ticker}_{cm.DataField.low}"]
            close = self.input_dm[f"{ticker}_{cm.DataField.close}"]

            # Calculate ADX, +DI, and -DI
            adx_df = ta.adx(high, low, close, length=14)
            self.input_dm[f"{ticker}_ADX"] = adx_df['ADX_14']
            self.input_dm[f"{ticker}_DIP"] = adx_df['DMP_14']
            self.input_dm[f"{ticker}_DIM"] = adx_df['DMN_14']

    def run_model(self, model=None):
        '''
        Generate trade signals and shares based on ADX strategy
        '''
        self._calc_ADX()

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

        entry_day_index = {}
        entry_price = {}

        for j in range(ncol):
            ticker = self.pricing_matrix.columns[j]
            adx = self.input_dm[f"{ticker}_ADX"]
            dip = self.input_dm[f"{ticker}_DIP"]
            dim = self.input_dm[f"{ticker}_DIM"]

            for i in range(1, nrow):
                current_price = self.pricing_matrix.iloc[i, j]
                current_shares_with_sign.iloc[i, j] = current_shares_with_sign.iloc[i-1, j]

                if pd.isna(current_shares_with_sign.iloc[i-1, j]) or current_price == 0:
                    continue

                if current_shares_with_sign.iloc[i-1, j] != 0:
                    ret = 100 * (current_price - entry_price[ticker]) / entry_price[ticker]
                    if ret >= self.target_gain_percentage or ret < self.max_loss_percentage:
                        if current_shares_with_sign.iloc[i-1, j] > 0:
                            tsignal.iloc[i, j] = -1
                            taction.iloc[i, j] = cm.TradeAction.SELL.value
                            shares.iloc[i, j] = abs(current_shares_with_sign.iloc[i-1, j])
                            current_shares_with_sign.iloc[i, j] = 0
                        elif current_shares_with_sign.iloc[i-1, j] < 0:
                            tsignal.iloc[i, j] = 1
                            taction.iloc[i, j] = cm.TradeAction.BUY.value
                            shares.iloc[i, j] = abs(current_shares_with_sign.iloc[i-1, j])
                            current_shares_with_sign.iloc[i, j] = 0

                elif adx.iloc[i] > self.adx_threshold:
                    if dip.iloc[i] > dim.iloc[i]:
                        tsignal.iloc[i, j] = 1
                        taction.iloc[i, j] = cm.TradeAction.BUY.value
                        shares.iloc[i, j] = int(dollar_exposure / current_price)
                        current_shares_with_sign.iloc[i, j] = shares.iloc[i, j]
                        entry_day_index[ticker] = i
                        entry_price[ticker] = self.pricing_matrix.iloc[i, j]
                    elif dim.iloc[i] > dip.iloc[i] and current_shares_with_sign.iloc[i-1, j] == 0:
                        tsignal.iloc[i, j] = -1
                        taction.iloc[i, j] = cm.TradeAction.SELL.value
                        shares.iloc[i, j] = int(dollar_exposure / current_price)
                        current_shares_with_sign.iloc[i, j] = -shares.iloc[i, j]
                        entry_day_index[ticker] = i
                        entry_price[ticker] = self.pricing_matrix.iloc[i, j]

        return tsignal, taction, shares


def _test_adx():
    from .preference import Preference

    pref = Preference()
    universe = ["SPY", "QQQ", "IWM"]
    start_date = datetime.date(2013, 1, 1)
    end_date = datetime.date(2023, 1, 1)

    loader = DataMatrixLoader(pref, 'test_adx', universe, start_date, end_date)
    dm = loader.get_daily_datamatrix()

    adx_strategy = ADXStrategy(pref, dm, cm.OneMillion, adx_threshold=25, target_gain_percentage=1.5, max_loss_percentage=-0.5)
    adx_strategy.validate()
    tradesignal, tradeaction, shares = adx_strategy.run_model()

    adx_strategy.run_strategy()
    print(adx_strategy.performance)

    print(f"Saving output to {pref.test_output_dir}")
    adx_strategy.save_to_csv(pref.test_output_dir)

if __name__ == "__main__":
    _test_adx()
