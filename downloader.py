import numpy as np
import pandas as pd
import os
import requests
import time
import collections

class Downloader:
    def __init__(self, time_start: str, time_end: str, period: str, label = ''):
        '''
        - time_start, time_end should be in the form of yyyy-mm-dd
        - period: 'h' or 'd'
        - label: string used to create the directory (hourly data will be stored in data_[label] directory)
        '''
        self.time_start = time_start
        self.time_end = time_end
        self.period = period
        self.directory = 'data' + ('_' if label else '') + label
        
        
    def reset_data(self):
        '''
        - Clear all data in the directory
        '''
        if os.path.exists(self.directory):
            for f in os.listdir(self.directory):
                os.remove(f'{self.directory}/{f}')
        print(f'{self.directory} is cleaned')
                
                
    def download_data(self, symbol: str):
        '''
        - Download ohlcv + marketcap data from CoinmarketCap
        - symbol: Crypto ticker
        
        - Daily data has no limit on the number of rows. So they will be directly downloaded.
        - For hourly data, the process should be divided and all downloaded data will be concatenated later (1 call = 10000 data)
        
        - If we call get_data function with updated tickers, it will download the data only for new tickers
            - To update the data for pre-existing ticker, remove the original file and run again
        
        Note
        - As parquet format is used, pyarrow should be installed before using it
        '''
        
        
        # Parameter Seeting
        CMCurl = "https://web-api.coinmarketcap.com/v1/cryptocurrency/ohlcv/historical"
        file = f'{self.directory}/{symbol}_{self.period}.parquet'        
        query = {
        'time_start' : self.time_start,
        'time_end' : self.time_end,
        'interval' : '1h' if self.period == 'h' else '1d',
        'time_period' : 'hourly' if self.period == 'h' else 'daily',
        'symbol' : symbol
        }
        
        # Create the directory if it does not exist
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)
            
        if self.period == 'd':
            response = requests.get(CMCurl, params = query)
            df = pd.DataFrame({x['time_open'] : x['quote']['USD']for x in response.json()['data']['quotes']}).T.iloc[:,:-1]
            df.to_parquet(file)
            print(f'{file} is downloaded')
            
        if self.period == 'h':
            dates = list(pd.date_range(start = self.time_start, end = self.time_end, freq = '416D'))+[pd.to_datetime(self.time_end)]
            data = []
            for i in range(1,len(dates)):
                query['time_start'] = str(dates[i-1].date())
                query['time_end'] = str(dates[i].date())
                response = requests.get(CMCurl, params = query)
                df = pd.DataFrame({x['time_open'] : x['quote']['USD']for x in response.json()['data']['quotes']}).T.iloc[:,:-1]
                data.append(df)
                print(f'{symbol} from {dates[i-1].date()} to {dates[i].date()} is done')
            pd.concat(data, axis = 0).to_parquet(file)
                
    def get_data(self, symbols: list):
        '''
        - symbols: list of tickers (e.g. ['BTC', 'ETH'])
        
        Note
        - As parquet format is used, pyarrow should be installed before using it
        '''
        data = []
        for sym in symbols:
            file = f'{self.directory}/{sym}_{self.period}.parquet'
            if not os.path.isfile(file):
                self.download_data(sym)
            df = pd.read_parquet(file)
            df.columns = pd.MultiIndex.from_tuples([(c,sym) for c in df.columns])
            data.append(df)
        return pd.concat(data, axis = 1)