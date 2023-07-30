import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), "../../Libraries/trade"))


import polars as pl
import pandas as pd
import numpy as np
import pickle
from datetime import datetime
from timeframe import Timeframe
from const import Const
from converter import Converter
from candle_chart import CandleChart, BandPlot, makeFig, gridFig, Colors

from dc_detector import DCDetector

def load_tick_data(filepath):
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
    dic = load_tick_data(path)
    tohlcv, candles = tick_to_candle(dic)
    print('candles size:', len(candles))
    print(tohlcv.keys())
    df = pl.DataFrame(tohlcv)
    df.write_excel('./1min.xlsx')
    
    
def plot_events(title, events, time, op, hi, lo, cl, date_format=CandleChart.DATE_FORMAT_YEAR_MONTH):
    fig, ax = makeFig(1, 1, (16, 8))
    chart = CandleChart(fig, ax, title=title, date_format=date_format)
    #chart.drawCandle(time, op, hi, lo, cl)
    chart.drawLine(time, cl, color='blue', xlabel=True)
    for dc_event, os_event in events:
        if dc_event.upward:
            c = 'green'
        else:
            c = 'red'
        chart.drawLine(dc_event.term, dc_event.price, should_set_xlim=False, linewidth=5.0, color=c)
        chart.drawLine(os_event.term, os_event.price, should_set_xlim=False, linewidth=5.0, color=c, linestyle='dotted')
        
def detect():
    with open('./data/asx200.pkl', 'rb') as f:
        df = pickle.load(f)
        
    #print(df.columns, df.index)
    time = df.index.to_pydatetime()
    op = df["Open"].to_numpy()
    hi = df["High"].to_numpy()
    lo = df["Low"].to_numpy()
    cl = df["Close"].to_numpy()
    detector = DCDetector(time, cl)   
    events = detector.detect_events(5)
    plot_events("", events, time, op, hi, lo, cl)
    

    
if __name__ == '__main__':
    detect()
    
