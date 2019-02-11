import pandas as pd
import numpy as np

def dailytrades(symbol, date_string, venue):
    '''return table of trades'''
        
    s = '''SELECT
            time,
            trade_size,
            price,
            listing_exchange,
            buy_broker,
            sell_broker,
            trade_condition,
            record_type,
        FROM trades 
        WHERE symbol = '%s' 
            AND date_string = '%s' 
            AND venue = '%s'
            AND listing_exchange = '%s'
            AND price > 0
            AND price < 99999
        ORDER BY time ASC'''
    
    sargs = (symbol, date_string, venue)
    return spark.sql(s % sargs)