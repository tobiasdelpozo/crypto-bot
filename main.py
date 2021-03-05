from load_data import *
import pandas as pd
import numpy as np
from candlestick_id import *
from scipy import stats


# Get difference between open and close price for candlesticks.

def get_difference(df):
    df['difference'] = df['open'] - df['close']
    return df


# Split the data into training and testing data.

def split_train_test(df):
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.set_index(df['timestamp'])
    train = df['2017-08-17 04:00:00':'2020-08-31 23:00:00']
    test = df['2020-08-31 00:00:00': '2020-12-31 23:00:00']
    return train, test


# Testing candlestick strategy - not very good.

def candle_strategy(df):
    df = candle_df(df)
    df['signal'] = np.where(df['candle_cumsum'] > 1, 1, -1)
    df['signal2'] = np.where(df['signal'] == df['signal'].shift(1), 0, df['signal'])
    if int(df[df.signal2 != 0].signal2.sum()) == -1:
        df['signal2'].iat[-1] = 1
    elif int(df[df.signal2 != 0].signal2.sum()) == 1:
        df['signal2'].iat[-1] = -1
    df_returns = df[df['signal2'] != 0].copy()
    df_returns['units'] = 2
    df_returns['units'].iat[0], df_returns['units'].iat[-1] = 1, 1

    print('Total transactions are :', df_returns[df_returns['signal2'] == 1].signal2.sum())
    print('Total returns are :', ((df_returns['signal2'] * df_returns['units'] * df_returns['close']).sum()) * -1)


# Calculate the percentiles of the training data differences.

def calc_percentile(df):
    sz = df['difference'].size - 1
    df['percentile'] = df['difference'].rank(method='max').apply(lambda x: 100.0 * (x - 1) / sz)
    return df


# Buy if the price movement is in the 5th percentile or lower
# Sell if the price movement is in the 95th percentile or higher

def percentile_strategy(train, test):
    train_percentile = np.array(train['difference'])
    test['percentile'] = 0

    for index, row in test.iterrows():
        value = stats.percentileofscore(train_percentile, row['difference'])
        test.at[index, 'percentile'] = value

    test['buy'] = np.where(test['percentile'] <= 5, True, False)
    test['sell'] = np.where(test['percentile'] >= 95, True, False)
    test['hold'] = (test['buy'] == False) & (test['sell'] == False)
    return test


# Test the performance given a starting balance or starting eth holdings.
# Print the results in performance.

def backtesting(test, start_balance, start_eth):
    balance = start_balance
    eth = start_eth
    transcations = 0

    for index, row in test.iterrows():
        if row['sell'] and eth != 0:
            balance += eth * row['close']
            eth = 0
            transcations += 1
        elif row['buy'] and balance != 0:
            eth += balance / row['close']
            balance = 0
            transcations += 1
            
    cashout_price = test.tail(1)['close'].values[0]

    print("Starting Balance: " + str(start_balance))
    print("Ending Balance: " + str(eth * cashout_price))
    print("Transactions Made: " + str(transcations))
    

if __name__ == '__main__':
    # Get hourly data for last 4 years. Uncomment if data not already downloaded.
    # get_all_binance("ETHUSDT", "1h", save=True)

    # Read data, compute difference, split.
    df = read_data("ETHUSDT-1h-data.csv")
    df = get_difference(df)
    train, test = split_train_test(df)

    # Add buy and sell signals, backtest.
    test = percentile_strategy(train, test)
    backtesting(test, 1000, 0)

