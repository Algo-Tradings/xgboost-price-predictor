import ccxt
import pandas as pd
# import numpy as np
# import statsmodels.api as sm
import ta


class BinanceManager:
    def __init__(self, api_key=None, secret_key=None):
        self.binance = ccxt.binance()
        self.binance.apiKey = api_key
        self.binance.secret = secret_key
        self.binance.load_markets()

    def get_historical_data(self, symbol, timeframe, since, limit=None):
        since = self.__str_to_timestamp(since)
        data = []
        limit_remaining = float('inf') if limit is None else limit
        while limit_remaining > 0:
            fetch_limit = min(limit_remaining, 500)  # Maximum fetch limit for Binance
            chunk_data = self.binance.fetch_ohlcv(symbol, timeframe, since=since, limit=fetch_limit)
            data.extend(chunk_data)
            if len(chunk_data) < fetch_limit:
                break  # No more data available
            since = chunk_data[-1][0] + 1  # Update 'since' to fetch next chunk
            limit_remaining -= fetch_limit

        df = pd.DataFrame(data, columns=['datetime', 'open', 'high', 'low', 'close', 'volume'])
        df['datetime'] = pd.to_datetime(df['datetime'], unit='ms')
        df.set_index('datetime', inplace=True)
        return df

    def load_previous_data(self, symbol, timeframe):
        df = pd.read_csv(f'data/{symbol}_{timeframe}.csv')
        df['datetime'] = pd.to_datetime(df['datetime'])
        df.set_index('datetime', inplace=True)
        return df
    def __str_to_timestamp(self, str_date):
        return int(pd.to_datetime(str_date).timestamp() * 1000)

    def get_all_ta_features(self, df):
        df = ta.add_all_ta_features(df, 'open', 'high', 'low', 'close', 'volume', fillna=True)
        return df

    def get_bollinger_bands(self, df, period=20):
        df['bb_bands'] = ta.volatility.bollinger_hband(df['close'], n=period)
        df['bb_bands'] = df['bb_bands'].shift(1)
        return df

    def get_rsi(self, df, period=14):
        df['rsi'] = ta.momentum.rsi(df['close'], n=period)
        return df

    def get_macd(self, df, period_fast=12, period_slow=26, period_signal=9):
        df['macd'] = ta.trend.macd_diff(df['close'], n_fast=period_fast, n_slow=period_slow, n_sign=period_signal)
        return df

    def get_ema(self, df, period=20):
        df['ema'] = ta.trend.ema_indicator(df['close'], n=period)
        return df

    def get_sma(self, df, period=20):
        df['sma'] = ta.trend.sma_indicator(df['close'], n=period)
        return df







