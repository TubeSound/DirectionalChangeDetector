import polars as pl
import numpy as np
from datetime import datetime
from libs.timeframe import Timeframe
from libs.const import Const
from libs.converter import Converter

def load_data(filepath):
    #df = pl.read_csv(filepath, sep='\t')
    df0 = pl.read_csv(filepath, has_header=True, separator='\t')
    df1 = df0.drop(["<ASK>", "<LAST>", "<VOLUME>"])
    df2 = df1.filter((pl.col("<FLAGS>") == 102) | (pl.col("<FLAGS>") == 98))
    datetime_str = (df2.get_column("<DATE>") + ' ' + df2.get_column("<TIME>")).alias("datetime_str")
    
    n = len(df2)
    time = []
    for i, s in enumerate(datetime_str):
        t = datetime.strptime(s, '%Y.%m.%d %H:%M:%S.%f')
        time.append(t)
    price = df2.get_column("<BID>").to_numpy()
    
    
    dic = {Const.TIME: time, Const.PRICE: price}
    
    return dic


def tick_to_candle(dic: dict):
    time = dic[Const.TIME]
    price = dic[Const.PRICE]
    return Converter.tick_to_candle(dic)    
    
def main():
    path = './data/USDJPY_202306220000_202307270000.csv'
    dic = load_data(path)
    dic2, candles = tick_to_candle(dic)
    print('candles size:', len(candles))
    print(dic2.keys())
    df = pl.DataFrame(dic2)
    df.write_excel('./1min.xlsx')
    
    
if __name__ == '__main__':
    main()
    
