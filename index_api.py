import pyarrow.parquet as pq
import polars as pl
import numpy
import torch

import datetime

class data_transform:
    def __init__(self, batch, sequence_month, data_path='kline_5m.parquet'):
        self.status = False
        self.batch = batch
        self.batch_count = 0
        self.sequence_month = sequence_month
        self.data_path = data_path
        self.data = self.read_5m(data_path)
        self.tickers = self.data['jj_code'].unique().sort().to_numpy()
        self.drop_columns = ['open_time', 'jj_code', 'close_time']

        self.results = torch.empty(0, self.sequence_month, 288, 9)

    def get_tickers(self) -> numpy.ndarray:
        return self.tickers

    def read_5m(self, data_path: str) -> pl.DataFrame:
        '''
        Input: data_path
        Output: pl.DataFrame

        Read the 5m kline data from the parquet file
        and sort the data by tickers name and open_time
        '''
        df = pl.read_parquet(data_path)
        df = df.sort(['jj_code', 'open_time'])
        return df
    
    def fetch_date(self, kline:pl.DataFrame) -> datetime.date:
        return kline['open_time'].dt.date()[0]
    
    def get_legal_df(self, legal_tickers:list) -> pl.DataFrame:
        '''
        Input: legal_tickers
        Output: pl.DataFrame

        Get the legal tickers data rows from the self.data
        '''
        return self.data.filter(self.data['jj_code'].is_in(legal_tickers))

    def get_previous_data(self, date:datetime.date, legal_tickers:list) -> torch.tensor:
        '''
        Input: date, legal_tickers
        Output: torch.tensor

        Get the previous data of the legal tickers
        '''
        startdate = date - datetime.timedelta(days=self.sequence_month)  # start date

        df_legal = self.get_legal_df(legal_tickers)
        # print("df_legal shape: ", df_legal.shape)

        df_range = df_legal.filter(
            (df_legal['open_time'].dt.date() >= startdate) &
            (df_legal['open_time'].dt.date() < date)
        )

        assert df_range.shape[0] == self.sequence_month * 288 * len(legal_tickers), 'The data is not enough'

        df_range = df_range.drop(self.drop_columns)
        data = df_range.to_torch().reshape(len(legal_tickers), self.sequence_month, 288, 9)
        print("data shape: ", data.shape)
        return data


    def get_legal_tickers(self, date:datetime.date) -> list:
        '''
        Input: date
        Output: numpy.ndarray

        Get the legal tickers which has date to the sequence_month,
        and all have 288 data in one day
        '''
        start_date = date - datetime.timedelta(days=self.sequence_month)  # start date

        def get_for_each(date):
            df_date = self.data.filter(self.data['open_time'].dt.date() == date)
            tickers_date = []
            for ticker in self.tickers:
                df_temp = df_date.filter(df_date['jj_code'] == ticker)
                if df_temp.shape[0] == 288:  # 288 data in one day
                    tickers_date.append(ticker)
            return tickers_date
        
        legal_tickers = list(set(get_for_each(date)) & set(get_for_each(start_date)))
        legal_tickers.sort()
        return legal_tickers


    def output(self, kline:pl.DataFrame) -> torch.Tensor:
        '''
        Input: kline
        Output: torch.Tensor

        Output the data of the kline in batch size
        '''
        self.batch_count += 1
        print("batch_count: ", self.batch_count)
        if self.batch_count == self.batch - 1:
            self.status = True

        date = self.fetch_date(kline)
        legal_tickers = self.get_legal_tickers(date)
        previous_data = self.get_previous_data(date, legal_tickers)
        
        self.results = torch.cat((self.results, previous_data), 0)
        print("results shape: ", self.results.shape)

        if self.batch_count == self.batch:
            self.status = False
            output = self.results.clone()
            self.results = torch.empty(0, self.sequence_month, 288, 9)
            return output
        else:
            return None


def main():
    data_loader = data_transform(batch=4, sequence_month=20)
    # df = data_loader.data.head(1)

    for i in range(8):
        # print("data_loader.status: ", data_loader.status)
        if data_loader.status:
            output = data_loader.output(data_loader.data.head(i+1))
            print("Data get successfully. Data shape: ", output.shape)
        else:
            data_loader.output(data_loader.data.head(i+1))

if __name__ == '__main__':
    main()
