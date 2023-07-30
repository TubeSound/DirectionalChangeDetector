# -*- coding: utf-8 -*-
"""
Created on Sat Jul 29 08:31:27 2023

@author: docs9
"""
import polars as pl
import numpy as np
from numpy import array


class Event:
    def __init__(self, i_begin, i_end, time_begin, time_end, price_begin, price_end, threshold):
        self.index = [i_begin, i_end]
        self.term = [time_begin, time_end]
        self.price = [price_begin, price_end]
        self.delta = (price_end / price_begin - 1.0) * 100.0
        self.upward = (self.delta >= 0)
        self.downward = (self.delta < 0)
        self.threshold = threshold


class DCDetector:
    def __init__(self, time: array, prices: array):
        self.time = time
        self.prices = prices
        
    def detect_os(self, time: array, data: array, begin: int, threshold: float, is_upward):
        ref = data[begin]
        i_peak = begin
        n = len(data)
        for i in range(begin + 1, n):
            delta = (data[i] / ref - 1.0) * 100.0
            event = Event(begin, i_peak, time[begin], time[i_peak], data[begin], data[i_peak], threshold)     
            if is_upward:
                if delta <= -1.0 *  threshold:
                    return event
                if data[i] > ref:
                    ref = data[i]
                    i_peak = i
            else:
                if delta >= threshold:
                    return event
                if data[i] < ref:
                    ref = data[i]
                    i_peak = i
        return None
    
    def detect_dc(self, time: array, data: array, begin: int, threshold: float, upward=None):
        ref = data[begin]
        n = len(data)
        for i in range(begin + 1, n):
            delta = (data[i] / ref - 1.0) * 100.0
            event = Event(begin, i, time[begin], time[i], ref, data[i], threshold)     
            if upward is None:
                # detect DC event
                if abs(delta) >= threshold:
                    return event
            else:
                if upward:
                    if delta >= threshold:
                        return event
                else:
                    if delta <= -1 * threshold:
                        return event
        return None
        
    def search_max_point(self, data: array, begin: int, min_limit: float):
        max_value = -1
        max_i = -1
        for i in range(begin, len(data)):
            if data[i] > max_value:
                max_value = data[i]
                max_i = i
            if data[i] <= min_limit:
                break
        return (max_i, max_value)
        
    def search_min_point(self, data: array, begin: int, max_limit: float):
        min_value = max_limit
        min_i = -1
        for i in range(begin, len(data)):
            if data[i] < min_value:
                min_value = data[i]
                min_i = i
            if data[i] >= max_limit:
                break
        return (min_i, min_value)        
    
    def detect_events(self, threshold_percent):
        time = self.time.copy()
        prices = self.prices.copy()
        events = []
        begin = 0        
        while True:
            dc_event = self.detect_dc(time, prices, begin, threshold_percent)
            if dc_event is None:
                return events
        
            begin = dc_event.index[1]
            direction = dc_event.upward
            os_event = self.detect_os(time, prices, begin, threshold_percent, direction)
            if os_event is None:
                break
            events.append([dc_event, os_event])
            begin = os_event.index[1]
        return events        
        