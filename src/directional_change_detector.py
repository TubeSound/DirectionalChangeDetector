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

from dc_detector import DCDetector, indicators, TimeUnit, coastline

from market_data import getCandles, str2time_fx
from time_utils import TimeUtils

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
    
    
def plot_events(events, time, op, hi, lo, cl, date_format=CandleChart.DATE_FORMAT_YEAR_MONTH):
    fig, ax = makeFig(1, 1, (30,12))
    chart = CandleChart(fig, ax, title='', date_format=date_format)
    #chart.drawCandle(time, op, hi, lo, cl)
    chart.drawLine(time, cl, color='blue')
    for i, [dc_event, os_event] in enumerate(events):
        if dc_event is None:
            print('#' +str(i + 1) + '... No DC event and OS event')
        if dc_event.upward:
            c = 'green'
        else:
            c = 'red'
        x = dc_event.term[1]
        y = dc_event.price[1]
        chart.drawMarker(x, y, 'o', c)
        chart.drawLine(dc_event.term, dc_event.price, should_set_xlim=False, linewidth=5.0, color=c)
        if os_event is None:
            print('#' +str(i + 1) + '... No OS event')
            break
        chart.drawLine(os_event.term, os_event.price, should_set_xlim=False, linewidth=5.0, color=c, linestyle='dotted')
        (TMV, T, R) = indicators(dc_event, os_event, TimeUnit.DAY)
        label1 = "#{}  TMV: {:.5f}  ".format(i + 1, TMV)
        label2 = " T: {}  R: {:.5f}".format(T, R)
        chart.drawText(x, y * 1.05, label1 + ' \n' + label2)
        print(label1 + label2)
        
def detect(filepath):
    with open(filepath, 'rb') as f:
        df = pickle.load(f)
        
    #print(df.columns, df.index)
    time = df.index.to_pydatetime()
    op = df["Open"].to_numpy()
    hi = df["High"].to_numpy()
    lo = df["Low"].to_numpy()
    cl = df["Close"].to_numpy()
    detector = DCDetector(time, cl)   
    events = detector.detect_events(5, 3)
    print('DC event num:', len(events), ' Coastline:', coastline(events, TimeUnit.DAY))
    plot_events( events, time, op, hi, lo, cl)


def readFileXM(path, delimiter='\t'):
    def str2time(s: str):
        form = '%Y.%m.%d %H:%M:%S'
        t = datetime.strptime(s, form)
        t = t.astimezone(TimeUtils.TIMEZONE_TOKYO)
        return t    
    f = open(path, encoding='sjis')
    header = f.readline()
    line = f.readline()
    tohlc = []
    while line:
        values = line.split(delimiter)
        s = values[0] + ' ' + values[1]
        t = str2time(s) 
        o = float(values[2])
        h = float(values[3])
        l = float(values[4])
        c = float(values[5])
        tohlc.append([t, o, h, l, c])
        line = f.readline()
    f.close()
    return tohlc

def log_return(prices):
    out = [np.nan]
    for i in range(1, len(prices)):
        if prices[i - 1] == 0.0:
            rt = np.nan
        else:
            rt = (prices[i] - prices[i - 1]) / prices[i - 1]
        out.append(np.log(rt))
    return out

def hmm():
    path = './data/M1/GBPUSD_M1_201603230000_201607222357.csv'
    candles = readFileXM(path)    
    tohlc = Converter.candles2tohlc(candles)

    fig, axes = gridFig([5, 1], (24, 10))
    time = tohlc[0]
    cl = tohlc[3]
    chart1 = CandleChart(fig, axes[0], title='', date_format=CandleChart.DATE_FORMAT_DATE_TIME)
    chart1.drawLine(time, cl, color='blue')

    log_r = log_return(cl)
    chart2 = CandleChart(fig, axes[1], title='', date_format=CandleChart.DATE_FORMAT_DATE_TIME)
    chart2.drawLine(time, log_r, color='red')
    
        
def test():
    path = './data/asx200.pkl'
    #path = './data/aex25.pkl'
    detect(path)

    
if __name__ == '__main__':
    test()
    
